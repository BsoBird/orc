/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the pluggable {@link StripePlannerFactory} seam. The windowed
 * planner itself is out of scope here; these tests require only that a factory
 * set on the reader options is consulted and that the planner it returns
 * returns the same rows as the default planner.
 */
public class TestStripePlannerFactory {
  private static final TypeDescription SCHEMA =
      TypeDescription.fromString("struct<x:int,y:string>");
  private static final int ROWS = 20000;

  private final Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  private Configuration conf;
  private FileSystem fs;
  private Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestStripePlannerFactory." +
        testInfo.getTestMethod().get().getName().replaceFirst("\\[[0-9]+\\]", "")
        + "." + UUID.randomUUID() + ".orc");
    fs.delete(testFilePath, false);
  }

  /**
   * Write a multi-stripe file whose x column is the row number.
   */
  private void writeFile() throws IOException {
    try (Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf)
        .setSchema(SCHEMA)
        .compress(CompressionKind.NONE)
        .bufferSize(1024)
        .stripeSize(64 * 1024))) {
      VectorizedRowBatch batch = SCHEMA.createRowBatch(1024);
      LongColumnVector x = (LongColumnVector) batch.cols[0];
      BytesColumnVector y = (BytesColumnVector) batch.cols[1];
      for (int row = 0; row < ROWS; row += batch.getMaxSize()) {
        batch.reset();
        int size = Math.min(batch.getMaxSize(), ROWS - row);
        for (int r = 0; r < size; ++r) {
          x.vector[r] = row + r;
          y.setVal(r, ("row" + (row + r)).getBytes(StandardCharsets.UTF_8));
        }
        batch.size = size;
        writer.addRowBatch(batch);
      }
    }
  }

  /**
   * Read the rows selected by a search argument, so that the reader has to
   * plan both the index reads and the data reads, and digest them so that two
   * runs can be compared.
   */
  private ScanResult scan(StripePlannerFactory factory) throws Exception {
    Reader.Options options = new Reader.Options(conf);
    if (factory != null) {
      options.stripePlannerFactory(factory);
    }
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startOr()
        .lessThan("x", PredicateLeaf.Type.LONG, 5L)
        .equals("x", PredicateLeaf.Type.LONG, (long) ROWS - 1)
        .end().build();
    long rows = 0;
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
         RecordReader recordReader = reader.rows(options.searchArgument(sarg, new String[]{}))) {
      VectorizedRowBatch batch = SCHEMA.createRowBatch();
      while (recordReader.nextBatch(batch)) {
        rows += batch.size;
        LongColumnVector x = (LongColumnVector) batch.cols[0];
        BytesColumnVector y = (BytesColumnVector) batch.cols[1];
        for (int r = 0; r < batch.size; ++r) {
          ByteBuffer row = ByteBuffer.allocate(32);
          row.putLong(x.vector[r]);
          row.put(value(y, r).getBytes(StandardCharsets.UTF_8));
          digest.update(row.array());
        }
      }
      return new ScanResult(rows, hex(digest.digest()));
    }
  }

  /**
   * A factory that returns a counting subclass of the base planner, so that we
   * can check that the reader used the planner it was handed.
   */
  private static class CountingFactory implements StripePlannerFactory {
    final AtomicInteger creates = new AtomicInteger();
    volatile boolean returnNull = false;
    volatile CountingPlanner created;

    @Override
    public StripePlanner create(TypeDescription schema,
                                ReaderEncryption encryption,
                                DataReader dataReader,
                                OrcFile.WriterVersion version,
                                boolean ignoreNonUtf8BloomFilter,
                                long maxBufferSize,
                                Set<Integer> filterColIds) {
      creates.incrementAndGet();
      if (returnNull) {
        return null;
      }
      CountingPlanner planner = new CountingPlanner(schema, encryption, dataReader,
          version, ignoreNonUtf8BloomFilter, maxBufferSize, filterColIds);
      created = planner;
      return planner;
    }
  }

  /**
   * A planner that behaves exactly like the base one but counts the plan calls
   * that the reader makes.
   */
  private static class CountingPlanner extends StripePlanner {
    final AtomicInteger parseCalls = new AtomicInteger();
    final AtomicInteger indexPlans = new AtomicInteger();
    final AtomicInteger dataPlans = new AtomicInteger();

    CountingPlanner(TypeDescription schema,
                    ReaderEncryption encryption,
                    DataReader dataReader,
                    OrcFile.WriterVersion version,
                    boolean ignoreNonUtf8BloomFilter,
                    long maxBufferSize,
                    Set<Integer> filterColIds) {
      super(schema, encryption, dataReader, version, ignoreNonUtf8BloomFilter,
          maxBufferSize, filterColIds);
    }

    @Override
    public StripePlanner parseStripe(StripeInformation stripe,
                                     boolean[] columnInclude) throws IOException {
      parseCalls.incrementAndGet();
      return super.parseStripe(stripe, columnInclude);
    }

    @Override
    public StripePlanner parseStripe(StripeInformation stripe,
                                     boolean[] columnInclude,
                                     OrcProto.StripeFooter footer) throws IOException {
      parseCalls.incrementAndGet();
      return super.parseStripe(stripe, columnInclude, footer);
    }

    @Override
    public BufferChunkList planIndexReading(boolean[] bloomFilterColumns) {
      indexPlans.incrementAndGet();
      return super.planIndexReading(bloomFilterColumns);
    }

    @Override
    public BufferChunkList planDataReading(TypeReader.ReadPhase readPhase) {
      dataPlans.incrementAndGet();
      return super.planDataReading(readPhase);
    }

    @Override
    public BufferChunkList planPartialDataReading(OrcIndex index,
                                                 boolean[] includedRowGroups,
                                                 int startGroup,
                                                 TypeReader.ReadPhase readPhase) {
      dataPlans.incrementAndGet();
      return super.planPartialDataReading(index, includedRowGroups, startGroup, readPhase);
    }
  }

  private static class ScanResult {
    final long rows;
    final String digest;

    ScanResult(long rows, String digest) {
      this.rows = rows;
      this.digest = digest;
    }
  }

  private static String value(BytesColumnVector vector, int row) {
    return new String(vector.vector[row], vector.start[row], vector.length[row],
        StandardCharsets.UTF_8);
  }

  private static String hex(byte[] value) {
    StringBuilder builder = new StringBuilder(value.length * 2);
    for (byte b: value) {
      builder.append(String.format("%02x", b));
    }
    return builder.toString();
  }

  @Test
  public void testFactoryIsConsultedAndResultsMatch() throws Exception {
    writeFile();

    // baseline: no factory set, so the reader builds its own planner
    ScanResult baseline = scan(null);
    assertTrue(baseline.rows > 0 && baseline.rows < ROWS,
        "the search argument should select some but not all rows, got " + baseline.rows);

    CountingFactory factory = new CountingFactory();
    ScanResult withFactory = scan(factory);

    // the factory was consulted exactly once, for the one record reader
    assertEquals(1, factory.creates.get());
    assertNotNull(factory.created);

    // and its planner was the one that parsed the stripes and planned the reads
    CountingPlanner planner = factory.created;
    assertTrue(planner.parseCalls.get() > 0, "parseStripe was never called");
    assertTrue(planner.indexPlans.get() > 0, "planIndexReading was never called");
    assertTrue(planner.dataPlans.get() > 0, "no data ranges were planned");

    // identical rows and digest to the default planner
    assertEquals(baseline.rows, withFactory.rows);
    assertEquals(baseline.digest, withFactory.digest);
  }

  @Test
  public void testNoFactoryUsesDefaultPlanner() throws Exception {
    writeFile();
    Reader.Options options = new Reader.Options(conf);
    assertNull(options.getStripePlannerFactory());
    ScanResult result = scan(null);
    assertTrue(result.rows > 0, "no rows were read");
  }

  @Test
  public void testFactoryMustNotReturnNull() throws Exception {
    writeFile();
    CountingFactory factory = new CountingFactory();
    factory.returnNull = true;
    assertThrows(IllegalArgumentException.class, () -> scan(factory));
  }
}
