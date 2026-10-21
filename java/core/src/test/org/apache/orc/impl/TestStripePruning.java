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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.InMemoryKeystore;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.RecordReaderImpl.SargApplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the search argument driven stripe pruning, which is turned on with
 * {@code orc.sarg.stripe.pruning.enabled}.
 */
public class TestStripePruning {

  private static final String PRUNING = "orc.sarg.stripe.pruning.enabled";
  private static final TypeDescription SCHEMA = TypeDescription.fromString("struct<x:int,y:string>");
  private static final int ROWS = 20000;

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestStripePruning." +
        testInfo.getTestMethod().get().getName().replaceFirst("\\[[0-9]+\\]", "")
        + "." + UUID.randomUUID() + ".orc");
    fs.delete(testFilePath, false);
  }

  // ------------------------------------------------------------------
  // fixtures
  // ------------------------------------------------------------------

  /**
   * Write a file whose x column is the row number, so that the stripes hold
   * disjoint, increasing ranges of x.
   */
  private void writeSortedFile() throws IOException {
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
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
    assertTrue(reader.getStripes().size() >= 3,
        "the fixture needs several stripes, got " + reader.getStripes().size());
    reader.close();
  }

  /**
   * Write a file whose x column is encrypted, so that the stripe statistics of
   * the unencrypted variant can not be used to filter it.
   */
  private void writeEncryptedFile() throws IOException {
    Random random = new Random(42);
    byte[] piiKey = new byte[16];
    random.nextBytes(piiKey);
    InMemoryKeystore keys = new InMemoryKeystore(random)
        .addKey("pii", EncryptionAlgorithm.AES_CTR_128, piiKey);
    try (Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf)
        .setSchema(SCHEMA)
        .compress(CompressionKind.NONE)
        .bufferSize(1024)
        .stripeSize(64 * 1024)
        .setKeyProvider(keys)
        .encrypt("pii:x"))) {
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

  // ------------------------------------------------------------------
  // scanning helper
  // ------------------------------------------------------------------

  /**
   * The result of a full scan of the test file: enough to prove that pruning
   * returns the same rows without reading the same bytes.
   */
  private static class ScanResult {
    long rows;
    String digest;
    int stripes;
    long readCalls;
    long bytesRead;

    ScanResult(long rows, String digest, int stripes, long readCalls, long bytesRead) {
      this.rows = rows;
      this.digest = digest;
      this.stripes = stripes;
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
      CountingDataReader counter = new CountingDataReader(newDataReader(reader));
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
            ByteBuffer row = ByteBuffer.allocate(32);
            row.putLong(x.vector[r]);
            row.put(value(y, r).getBytes(StandardCharsets.UTF_8));
            digest.update(row.array());
          }
        }
      }
      return new ScanResult(rows, hex(digest.digest()), reader.getStripes().size(),
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

  // ------------------------------------------------------------------
  // pickStripes unit tests
  // ------------------------------------------------------------------

  private static SargApplier newApplier(SearchArgument sarg) throws IOException {
    SchemaEvolution evolution = new SchemaEvolution(SCHEMA, SCHEMA,
        new Reader.Options(new Configuration()));
    return new SargApplier(sarg, 10000, evolution, OrcFile.WriterVersion.ORC_14, false,
        false, false);
  }

  @Test
  public void testPickStripesWithoutStats() throws Exception {
    writeSortedFile();
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("x", PredicateLeaf.Type.LONG, 5L)
        .end().build();
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf))) {
      List<StripeInformation> stripes = reader.getStripes();
      SargApplier applier = newApplier(sarg);
      // no statistics at all, or a list that can not be aligned with the
      // stripes, has to select every stripe
      assertNull(applier.pickStripes(stripes, null));
      assertNull(applier.pickStripes(stripes, Collections.emptyList()));
    }
  }

  @Test
  public void testPickStripesWithUnusableColumnStats() throws Exception {
    writeSortedFile();
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("x", PredicateLeaf.Type.LONG, 5L)
        .end().build();
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf))) {
      // Writers that keep the statistics of an encrypted column in an
      // encrypted variant leave an empty entry in the unencrypted one. Those
      // entries must not be read as "the column has no rows at all".
      List<OrcProto.ColumnStatistics> columns = new ArrayList<>();
      for (int c = 0; c < SCHEMA.getMaximumId() + 1; ++c) {
        columns.add(OrcProto.ColumnStatistics.getDefaultInstance());
      }
      List<StripeStatistics> stats = new ArrayList<>();
      for (int s = 0; s < reader.getStripes().size(); ++s) {
        stats.add(new StripeStatisticsImpl(SCHEMA, new ArrayList<>(columns), false, false));
      }
      assertNull(newApplier(sarg).pickStripes(reader.getStripes(), stats));
    }
  }

  @Test
  public void testPickStripesSelectsDisjointRanges() throws Exception {
    writeSortedFile();
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf))) {
      List<StripeInformation> stripes = reader.getStripes();
      List<StripeStatistics> stats = reader.getVariantStripeStatistics(null);

      // the last row only matches the last stripe
      SearchArgument sarg = SearchArgumentFactory.newBuilder()
          .startAnd()
          .equals("x", PredicateLeaf.Type.LONG, (long) ROWS - 1)
          .end().build();
      boolean[] result = newApplier(sarg).pickStripes(stripes, stats);
      assertNotNull(result);
      assertEquals(stripes.size(), result.length);
      for (int s = 0; s < stripes.size(); ++s) {
        assertEquals(s == stripes.size() - 1, result[s], "stripe " + s);
      }

      // a value that no stripe holds excludes every stripe
      sarg = SearchArgumentFactory.newBuilder()
          .startAnd()
          .equals("x", PredicateLeaf.Type.LONG, (long) ROWS + 1000)
          .end().build();
      result = newApplier(sarg).pickStripes(stripes, stats);
      assertEquals(stripes.size(), result.length);
      for (int s = 0; s < result.length; ++s) {
        assertEquals(false, result[s], "stripe " + s);
      }
    }
  }

  // ------------------------------------------------------------------
  // end to end tests
  // ------------------------------------------------------------------

  @Test
  public void testPruningIsOffByDefault() throws Exception {
    writeSortedFile();
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("x", PredicateLeaf.Type.LONG, (long) ROWS - 1)
        .end().build();

    ScanResult fromDefault = scan(sarg, null);
    ScanResult disabled = scan(sarg, false);
    ScanResult enabled = scan(sarg, true);
    assertTrue(disabled.rows > 0, "the fixture should return rows");
    assertEquals(disabled.rows, fromDefault.rows);
    assertEquals(disabled.digest, fromDefault.digest);
    assertEquals(disabled.readCalls, fromDefault.readCalls);
    assertEquals(disabled.bytesRead, fromDefault.bytesRead);
    // and the enabled reader has to do less work
    assertTrue(enabled.readCalls < disabled.readCalls,
        "pruning did not reduce the number of reads");
    assertTrue(enabled.bytesRead < disabled.bytesRead,
        "pruning did not reduce the number of bytes read");
  }

  @Test
  public void testAllStripesSkipped() throws Exception {
    writeSortedFile();
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("x", PredicateLeaf.Type.LONG, (long) ROWS + 1000)
        .end().build();

    ScanResult disabled = scan(sarg, false);
    assertEquals(0, disabled.rows);
    assertTrue(disabled.readCalls > 0, "the disabled reader should still read the file");

    ScanResult enabled = scan(sarg, true);
    assertEquals(0, enabled.rows);
    assertEquals(0, enabled.readCalls, "a fully pruned scan must not read anything");
    assertEquals(0, enabled.bytesRead, "a fully pruned scan must not read anything");
  }

  @Test
  public void testSameRowsForSeveralSarGs() throws Exception {
    writeSortedFile();
    List<SearchArgument> sarGs = new ArrayList<>();
    // a range that covers the first stripes
    sarGs.add(SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("x", PredicateLeaf.Type.LONG, 500L)
        .end().build());
    // a value in the middle of the file
    sarGs.add(SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("x", PredicateLeaf.Type.LONG, (long) ROWS / 2)
        .end().build());
    // an or of two values in different stripes
    sarGs.add(SearchArgumentFactory.newBuilder()
        .startOr()
        .equals("x", PredicateLeaf.Type.LONG, 10L)
        .equals("x", PredicateLeaf.Type.LONG, (long) ROWS - 10)
        .end().build());
    // a predicate that matches nothing
    sarGs.add(SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("x", PredicateLeaf.Type.LONG, (long) ROWS + 1000)
        .end().build());
    // a predicate on a string column
    sarGs.add(SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("y", PredicateLeaf.Type.STRING, "row19999")
        .end().build());
    // a predicate on a column that is not in the file, which the reader can
    // not use for pruning
    sarGs.add(SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("missing", PredicateLeaf.Type.LONG, 10L)
        .end().build());

    for (SearchArgument sarg: sarGs) {
      ScanResult disabled = scan(sarg, false);
      ScanResult enabled = scan(sarg, true);
      assertEquals(disabled.rows, enabled.rows, "row count for " + sarg);
      assertEquals(disabled.digest, enabled.digest, "rows for " + sarg);
      assertTrue(enabled.readCalls <= disabled.readCalls, "reads for " + sarg);
      assertTrue(enabled.bytesRead <= disabled.bytesRead, "bytes for " + sarg);
    }
  }

  @Test
  public void testEncryptedColumnIsNeverSkipped() throws Exception {
    writeEncryptedFile();
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("x", PredicateLeaf.Type.LONG, (long) ROWS - 1)
        .end().build();

    ScanResult disabled = scan(sarg, false);
    ScanResult enabled = scan(sarg, true);
    // the stripe statistics of an encrypted column can not be used, so both
    // readers have to read the same stripes and return the same rows
    assertEquals(disabled.rows, enabled.rows);
    assertEquals(disabled.digest, enabled.digest);
    assertEquals(disabled.readCalls, enabled.readCalls);
    assertEquals(disabled.bytesRead, enabled.bytesRead);
    assertTrue(enabled.readCalls > 0, "the encrypted file has to be read");
  }

  @Test
  public void testSeekIntoSkippedStripe() throws Exception {
    writeSortedFile();
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals("x", PredicateLeaf.Type.LONG, (long) ROWS - 1)
        .end().build();
    Configuration enabledConf = new Configuration(conf);
    enabledConf.setBoolean(PRUNING, true);
    // with pruning the leading stripes are skipped, so seeking to the first row
    // of the file has to fall back to reading a stripe that was skipped
    assertEquals(seekAndReadFirstRow(new Configuration(conf), sarg),
        seekAndReadFirstRow(enabledConf, sarg));
  }

  /**
   * Seek to the first row of the file and return the value of x in the first
   * batch that the reader returns.
   */
  private long seekAndReadFirstRow(Configuration readConf, SearchArgument sarg)
      throws IOException {
    try (Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(readConf))) {
      try (RecordReader rows = reader.rows(reader.options()
          .searchArgument(sarg, new String[]{}))) {
        rows.seekToRow(0);
        VectorizedRowBatch batch = SCHEMA.createRowBatch();
        assertTrue(rows.nextBatch(batch));
        assertTrue(batch.size > 0);
        return ((LongColumnVector) batch.cols[0]).vector[0];
      }
    }
  }

  /**
   * Test-only decorator that counts the calls that move bytes off disk and
   * then delegates to the real data reader.
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
}
