/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the dictionary block pruning, which is turned on with
 * {@code orc.dictionary.block.prune.enabled}: a stripe whose dictionary can
 * not match the search argument is skipped without reading its data streams.
 */
public class TestDictionaryBlockPrune {

  private static final String PRUNING = "orc.dictionary.block.prune.enabled";
  private static final TypeDescription SCHEMA = TypeDescription.fromString("struct<x:int,y:string>");
  private static final int ROWS = 20000;
  // a handful of distinct values, so the writer dictionary encodes y
  private static final int DISTINCT = 16;

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestDictionaryBlockPrune." +
        testInfo.getTestMethod().get().getName().replaceFirst("\\[[0-9]+\\]", "")
        + "." + UUID.randomUUID() + ".orc");
    fs.delete(testFilePath, false);
  }

  /**
   * Write a file whose y column holds DISTINCT repeating values (plus a null
   * every 101st row when withNulls), so that every stripe is dictionary
   * encoded.
   */
  private void writeDictionaryFile(boolean withNulls) throws IOException {
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
          int global = row + r;
          x.vector[r] = global;
          if (withNulls && global % 101 == 0) {
            y.noNulls = false;
            y.isNull[r] = true;
          } else {
            y.setVal(r, ("val" + String.format("%02d", global % DISTINCT))
                .getBytes(StandardCharsets.UTF_8));
          }
        }
        batch.size = size;
        writer.addRowBatch(batch);
      }
    }
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
    assertTrue(reader.getStripes().size() >= 3,
        "the fixture needs several stripes, got " + reader.getStripes().size());
    reader.close();
  }

  /**
   * A data reader that counts the calls and the bytes that were read, and
   * shares the counters between the copies the record reader makes.
   */
  private static class CountingDataReader implements DataReader {
    private final DataReader delegate;
    long readCalls = 0;
    long bytesRead = 0;

    CountingDataReader(DataReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public void open() throws IOException {
      delegate.open();
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe)
        throws IOException {
      return delegate.readStripeFooter(stripe);
    }

    @Override
    public BufferChunkList readFileData(BufferChunkList list, boolean doForceDirect)
        throws IOException {
      readCalls += 1;
      for (BufferChunk chunk = list.get(); chunk != null;
          chunk = (BufferChunk) chunk.next) {
        if (!chunk.hasData()) {
          bytesRead += chunk.getLength();
        }
      }
      return delegate.readFileData(list, doForceDirect);
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return delegate.isTrackingDiskRanges();
    }

    @Deprecated
    @Override
    public void releaseBuffer(ByteBuffer toRelease) {
      delegate.releaseBuffer(toRelease);
    }

    @Override
    public void releaseAllBuffers() {
      delegate.releaseAllBuffers();
    }

    /**
     * The counters belong to the scan, so every copy shares them.
     */
    @Override
    public DataReader clone() {
      return this;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public InStream.StreamOptions getCompressionOptions() {
      return delegate.getCompressionOptions();
    }
  }

  private static class ScanResult {
    long rows;
    String digest;
    long readCalls;
    long bytesRead;

    ScanResult(long rows, String digest, long readCalls, long bytesRead) {
      this.rows = rows;
      this.digest = digest;
      this.readCalls = readCalls;
      this.bytesRead = bytesRead;
    }
  }

  private ScanResult scan(SearchArgument sarg, Boolean pruning) throws Exception {
    Configuration readConf = new Configuration(conf);
    if (pruning != null) {
      readConf.setBoolean(PRUNING, pruning);
    } else {
      readConf.unset(PRUNING);
    }
    try (Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(readConf))) {
      CountingDataReader counter = newDataReader(reader);
      long rows = 0;
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      try (RecordReader recordReader = reader.rows(reader.options()
          .searchArgument(sarg, new String[]{})
          .dataReader(counter))) {
        VectorizedRowBatch batch = SCHEMA.createRowBatch();
        while (recordReader.nextBatch(batch)) {
          rows += batch.size;
          LongColumnVector x = (LongColumnVector) batch.cols[0];
          BytesColumnVector y = (BytesColumnVector) batch.cols[1];
          for (int r = 0; r < batch.size; ++r) {
            ByteBuffer row = ByteBuffer.allocate(40);
            row.putLong(x.vector[r]);
            row.put(value(y, r).getBytes(StandardCharsets.UTF_8));
            digest.update(row.array());
          }
        }
      }
      return new ScanResult(rows, hex(digest.digest()),
          counter.readCalls, counter.bytesRead);
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

  private static CountingDataReader newDataReader(Reader reader) throws IOException {
    ReaderImpl impl = (ReaderImpl) reader;
    InStream.StreamOptions compression = InStream.options()
        .withCodec(OrcCodecPool.getCodec(impl.getCompressionKind()))
        .withBufferSize(impl.getCompressionSize());
    DataReaderProperties properties = DataReaderProperties.builder()
        .withCompression(compression)
        .withFileSystemSupplier(impl.getFileSystemSupplier())
        .withPath(impl.path)
        .withFile(impl.takeFile())
        .withMaxDiskRangeChunkLimit(OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(impl.conf))
        .withZeroCopy(false)
        .build();
    return new CountingDataReader(RecordReaderUtils.createDefaultDataReader(properties));
  }

  private static SearchArgument equalsSarg(String value) {
    return SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("y", PredicateLeaf.Type.STRING, value)
        .end().build();
  }

  /**
   * A comparison that no dictionary entry can satisfy skips every stripe
   * without reading its data streams: no rows, and a fraction of the bytes
   * that the scan without pruning reads.
   */
  @Test
  public void testMissSkipsDataReads() throws Exception {
    writeDictionaryFile(false);
    SearchArgument sarg = equalsSarg("val0a");
    ScanResult without = scan(sarg, false);
    ScanResult with = scan(sarg, true);
    // without pruning the statistics can not rule the value out (it is inside
    // the min/max range), so the unfiltered superset of the rows comes back
    assertEquals(ROWS, without.rows);
    // with pruning every stripe is skipped from its dictionary
    assertEquals(0, with.rows);
    assertTrue(with.bytesRead * 2 < without.bytesRead,
        "pruned scan should read less than half of the bytes: with=" +
            with.bytesRead + " without=" + without.bytesRead);
  }

  /**
   * A comparison that some dictionary entry satisfies returns the same rows
   * with and without pruning.
   */
  @Test
  public void testHitMatchesBaseline() throws Exception {
    writeDictionaryFile(false);
    SearchArgument sarg = equalsSarg("val07");
    ScanResult without = scan(sarg, false);
    ScanResult with = scan(sarg, true);
    assertTrue(without.rows > 0);
    assertEquals(without.rows, with.rows);
    assertEquals(without.digest, with.digest);
  }

  /**
   * A disjunction is not decided from the dictionaries, so it falls back to
   * the normal path and returns the same rows with and without pruning.
   */
  @Test
  public void testDisjunctionFallsBack() throws Exception {
    writeDictionaryFile(false);
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startOr()
        .equals("y", PredicateLeaf.Type.STRING, "val0a")
        .equals("y", PredicateLeaf.Type.STRING, "val03")
        .end().build();
    ScanResult without = scan(sarg, false);
    ScanResult with = scan(sarg, true);
    assertTrue(without.rows > 0);
    assertEquals(without.rows, with.rows);
    assertEquals(without.digest, with.digest);
  }

  /**
   * A null test is not decided from the dictionaries, so the stripes with
   * nulls are read and the same rows come back with and without pruning.
   */
  @Test
  public void testIsNullFallsBack() throws Exception {
    writeDictionaryFile(true);
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .isNull("y", PredicateLeaf.Type.STRING)
        .end().build();
    ScanResult without = scan(sarg, false);
    ScanResult with = scan(sarg, true);
    assertTrue(without.rows > 0);
    assertEquals(without.rows, with.rows);
    assertEquals(without.digest, with.digest);
  }

  /**
   * Nulls live in the PRESENT stream and never in the dictionary, so a stripe
   * that has nulls may still be skipped by an equality that no dictionary
   * entry satisfies: the null rows can not match the equality either.
   */
  @Test
  public void testNullsDoNotBreakEqualityPruning() throws Exception {
    writeDictionaryFile(true);
    SearchArgument sarg = equalsSarg("val0a");
    ScanResult without = scan(sarg, false);
    ScanResult with = scan(sarg, true);
    assertEquals(ROWS, without.rows);
    assertEquals(0, with.rows);
    assertTrue(with.bytesRead * 2 < without.bytesRead,
        "pruned scan should read less than half of the bytes: with=" +
            with.bytesRead + " without=" + without.bytesRead);
  }
}
