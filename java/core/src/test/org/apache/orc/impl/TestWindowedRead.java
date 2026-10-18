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
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.reader.StripePlanner;
import org.apache.orc.impl.reader.WindowedDataReader;
import org.apache.orc.impl.reader.WindowedStripePlanner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the windowed read, the seam that reads a run of stripes in two
 * merged requests instead of a footer and an index per stripe.
 *
 * <p>The reader has to answer exactly the rows the default reader answers,
 * and the chunk cache has to be able to satisfy every byte the reader asks
 * for, so the tests compare row digests against the default reader and
 * assert that nothing fell back.</p>
 */
public class TestWindowedRead {

  private static final TypeDescription SCHEMA = TypeDescription.fromString(
      "struct<a:int,b:int,c:int,d:int,e:int,f:int,g:int,h:int,y:string>");
  private static final int STRIPE_BATCH = 32;
  private static final int ROWS = 80_000;

  private static final Path WORK_DIR = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  private static Configuration conf;
  private static FileSystem fs;
  private static Path testFilePath;
  private static int stripes;
  private static long rowsPerStripe;

  @BeforeAll
  public static void writeFile() throws Exception {
    conf = new Configuration();
    // check the stripe memory often, so that the file really does break into
    // hundreds of stripes rather than a handful of huge ones
    conf.setLong(OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(), 1);
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(WORK_DIR, "TestWindowedRead.orc");
    fs.delete(testFilePath, false);
    try (Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf)
        .setSchema(SCHEMA)
        .compress(CompressionKind.NONE)
        .bufferSize(1024)
        .stripeSize(512))) {
      VectorizedRowBatch batch = SCHEMA.createRowBatch(STRIPE_BATCH);
      for (int row = 0; row < ROWS; row += batch.getMaxSize()) {
        batch.reset();
        int size = Math.min(batch.getMaxSize(), ROWS - row);
        for (int r = 0; r < size; ++r) {
          for (int col = 0; col < 8; ++col) {
            ((LongColumnVector) batch.cols[col]).vector[r] = row + r + col;
          }
          ((BytesColumnVector) batch.cols[8]).setVal(r,
              ("row" + (row + r)).getBytes(StandardCharsets.UTF_8));
        }
        batch.size = size;
        writer.addRowBatch(batch);
      }
    }
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf))) {
      stripes = reader.getStripes().size();
      rowsPerStripe = ROWS / stripes;
    }
  }

  // ------------------------------------------------------------------
  // configuration
  // ------------------------------------------------------------------

  /**
   * A configuration with the windowed read switched on, small enough windows
   * to see several of them, and a gate that will admit them.
   */
  private static Configuration windowedConf() {
    Configuration result = new Configuration();
    result.setBoolean(OrcConf.ORC_WINDOWED_READ_ENABLED.getAttribute(), true);
    result.setLong(OrcConf.ORC_WINDOWED_WINDOW_BYTES.getAttribute(), 256 * 1024);
    result.setLong(OrcConf.ORC_WINDOWED_GAP_BYTES.getAttribute(), 1024 * 1024);
    result.setLong(OrcConf.ORC_WINDOWED_GATE_LATENCY_MS.getAttribute(), 50);
    result.setDouble(OrcConf.ORC_WINDOWED_GATE_MBPS.getAttribute(), 100);
    result.setDouble(OrcConf.ORC_WINDOWED_GATE_SAFETY.getAttribute(), 3);
    result.setDouble(OrcConf.ORC_WINDOWED_GATE_MARGIN_MS.getAttribute(), 0);
    result.setDouble(OrcConf.ORC_WINDOWED_GATE_MIN_SAVED.getAttribute(), 1);
    result.setLong(OrcConf.ORC_WINDOWED_MAX_EXTRA_BYTES.getAttribute(), 32L << 20);
    return result;
  }

  /**
   * A predicate on x, expressed the way a table scan would express it.
   */
  private static SearchArgument lessThan(long bound) throws IOException {
    return SearchArgumentFactory.newBuilder()
        .startOr()
        .lessThan("a", PredicateLeaf.Type.LONG, bound)
        .end()
        .build();
  }

  // ------------------------------------------------------------------
  // scanning
  // ------------------------------------------------------------------

  /**
   * Run a scan and digest the rows it produces. The same scan with and
   * without the windowed read must produce the same digest.
   */
  private static Scan scan(Configuration scanConf,
                           SearchArgument sarg,
                           DataReader dataReader) throws Exception {
    Reader.Options options = new Reader.Options(scanConf);
    if (sarg != null) {
      options.searchArgument(sarg, new String[]{});
    }
    if (dataReader != null) {
      options.dataReader(dataReader);
    }
    Counters counters = dataReader instanceof CountingDataReader ?
        ((CountingDataReader) dataReader).counters : null;
    long rows = 0;
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    WindowedDataReader windowed = null;
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(scanConf));
         RecordReader recordReader = reader.rows(options)) {
      StripePlanner planner = ((RecordReaderImpl) recordReader).getStripePlanner();
      if (planner instanceof WindowedStripePlanner) {
        windowed = ((WindowedStripePlanner) planner).getWindowedDataReader();
      }
      VectorizedRowBatch batch = SCHEMA.createRowBatch();
      while (recordReader.nextBatch(batch)) {
        rows += batch.size;
        for (int r = 0; r < batch.size; ++r) {
          ByteBuffer row = ByteBuffer.allocate(96);
          for (int col = 0; col < 8; ++col) {
            row.putLong(((LongColumnVector) batch.cols[col]).vector[r]);
          }
          row.put(value((BytesColumnVector) batch.cols[8], r).getBytes(StandardCharsets.UTF_8));
          digest.update(row.array());
        }
      }
    }
    return new Scan(rows, hex(digest.digest()), windowed, counters);
  }

  private static void assertSameRows(Scan expected, Scan actual) {
    assertEquals(expected.rows, actual.rows);
    assertEquals(expected.digest, actual.digest);
  }

  /**
   * Every windowed scan has to have gone through the window machinery, and
   * none of it may have fallen back to the underlying reader.
   */
  private static void assertApplied(Scan scan) {
    assertNotNull(scan.windowed, "the windowed read should have been installed");
    assertTrue(scan.windowed.getWindowsEngaged() > 0,
        "no window was engaged: " + scan.windowed.getLastGateReason());
    assertTrue(scan.windowed.getRoundOneRequests() > 0,
        "round 1 never ran");
    assertEquals(0, scan.windowed.getMissedChunks(),
        "the round 1 buffers should have covered every chunk");
    assertEquals(0, scan.windowed.getMissedBytes(),
        "the round 1 buffers should have covered every byte");
    assertTrue(scan.windowed.getServedChunks() > 0,
        "nothing was served from the window buffers");
  }

  // ------------------------------------------------------------------
  // tests
  // ------------------------------------------------------------------

  @Test
  public void testTheFixtureHasManyStripes() throws Exception {
    assertTrue(stripes >= 500,
        "the windowed tests need a many-stripe file, got " + stripes);
  }

  @Test
  public void testNoSearchArgument() throws Exception {
    Configuration baseline = new Configuration(conf);
    Configuration windowed = windowedConf();

    Scan expected = scan(baseline, null, newDataReader());
    Scan actual = scan(windowed, null, newDataReader());

    assertSameRows(expected, actual);
    assertEquals(ROWS, actual.rows);
    assertApplied(actual);
    assertTrue(actual.counters.readCalls < expected.counters.readCalls,
        "the windowed read should have merged requests: " +
            actual.counters.readCalls + " vs " + expected.counters.readCalls);
  }

  @Test
  public void testLowSelectivity() throws Exception {
    // nearly every row matches, so the pruning has little to remove
    assertSelectivity(ROWS - 1, ROWS);
  }

  @Test
  public void testMidSelectivity() throws Exception {
    assertSelectivity(ROWS / 2, ROWS * 3 / 4);
  }

  @Test
  public void testHighSelectivity() throws Exception {
    // a fraction of a stripe's worth of rows out of the whole file
    assertSelectivity(STRIPE_BATCH, ROWS / 20);
  }

  /**
   * The windowed read has to return the same rows at any selectivity, and has
   * to serve every byte out of its own round 1 buffers. Stripe boundaries are
   * not aligned with the predicate, so the row count is only bounded to
   * within a stripe.
   */
  private void assertSelectivity(long bound, long atMost) throws Exception {
    Configuration baseline = new Configuration(conf);
    Configuration windowed = windowedConf();
    SearchArgument sarg = lessThan(bound);

    Scan expected = scan(baseline, sarg, newDataReader());
    Scan actual = scan(windowed, sarg, newDataReader());

    assertSameRows(expected, actual);
    assertTrue(actual.rows > 0, "the search argument matched nothing");
    assertTrue(actual.rows <= bound + rowsPerStripe,
        "the search argument returned rows it should have ruled out: " +
            actual.rows + " > " + bound);
    assertTrue(actual.rows <= atMost,
        "the search argument did not prune enough: " + actual.rows + " > " + atMost);
    assertApplied(actual);
  }

  @Test
  public void testTheKillSwitchIsNeutral() throws Exception {
    // a configuration that switches the windowed read off must issue exactly
    // the same requests as one that switches it on but stands the gate down
    Configuration disabled = new Configuration(conf);
    disabled.setBoolean(OrcConf.ORC_WINDOWED_READ_ENABLED.getAttribute(), false);

    Configuration stoodDown = windowedConf();
    stoodDown.setLong(OrcConf.ORC_WINDOWED_GATE_LATENCY_MS.getAttribute(), 0);

    for (SearchArgument sarg : new SearchArgument[] {null, lessThan(ROWS / 4)}) {
      Scan off = scan(disabled, sarg, newDataReader());
      Scan on = scan(stoodDown, sarg, newDataReader());
      assertSameRows(off, on);
      assertEquals(off.counters.readCalls, on.counters.readCalls,
          "standing down changed the number of requests");
      assertEquals(off.counters.bytesRead, on.counters.bytesRead,
          "standing down changed the number of bytes");
      assertEquals(off.counters.footerCalls, on.counters.footerCalls,
          "standing down changed the number of footer reads");
      assertNull(off.windowed, "the disabled run should have no windowed planner");
      assertNotNull(on.windowed, "the stood down run still installs the planner");
      assertEquals(0, on.windowed.getWindowsEngaged());
      assertTrue(on.windowed.getWindowsStoodDown() > 0);
    }
  }

  @Test
  public void testTheGateStandsDownAtZeroLatency() throws Exception {
    Configuration stoodDown = windowedConf();
    stoodDown.setLong(OrcConf.ORC_WINDOWED_GATE_LATENCY_MS.getAttribute(), 0);

    Scan expected = scan(new Configuration(conf), null, newDataReader());
    Scan actual = scan(stoodDown, null, newDataReader());

    assertSameRows(expected, actual);
    assertNotNull(actual.windowed);
    assertEquals(0, actual.windowed.getWindowsEngaged());
    assertTrue(actual.windowed.getWindowsStoodDown() > 0);
    assertEquals("gate-latency-zero", actual.windowed.getLastGateReason());
    assertEquals(0, actual.windowed.getRoundOneRequests());
  }

  @Test
  public void testTheArmIsAbsentWhenDisabled() throws Exception {
    Configuration disabled = new Configuration(conf);
    disabled.setBoolean(OrcConf.ORC_WINDOWED_READ_ENABLED.getAttribute(), false);

    Scan scan = scan(disabled, null, newDataReader());
    assertSameRows(scan(new Configuration(conf), null, newDataReader()), scan);
    assertNull(scan.windowed, "no windowed planner should be installed");
  }

  // ------------------------------------------------------------------
  // helpers
  // ------------------------------------------------------------------

  /**
   * The counters belong to the scan, so every copy of the data reader shares
   * one of them.
   */
  private static class Counters {
    long readCalls = 0;
    long bytesRead = 0;
    long footerCalls = 0;
  }

  private static class Scan {
    final long rows;
    final String digest;
    final WindowedDataReader windowed;
    final Counters counters;

    Scan(long rows, String digest, WindowedDataReader windowed, Counters counters) {
      this.rows = rows;
      this.digest = digest;
      this.windowed = windowed;
      this.counters = counters;
    }
  }

  /**
   * Test-only decorator that counts the calls that move bytes off disk and
   * then delegates to the real data reader.
   */
  private static class CountingDataReader implements DataReader {
    private final DataReader delegate;
    private final Counters counters;

    CountingDataReader(DataReader delegate, Counters counters) {
      this.delegate = delegate;
      this.counters = counters;
    }

    @Override
    public void open() throws IOException {
      delegate.open();
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe)
        throws IOException {
      counters.footerCalls += 1;
      return delegate.readStripeFooter(stripe);
    }

    @Override
    public BufferChunkList readFileData(BufferChunkList list, boolean doForceDirect)
        throws IOException {
      counters.readCalls += 1;
      for (BufferChunk chunk = list.get(); chunk != null;
          chunk = (BufferChunk) chunk.next) {
        if (!chunk.hasData()) {
          counters.bytesRead += chunk.getLength();
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

  /**
   * Build a counting data reader over the default reader for the test file.
   */
  private static DataReader newDataReader() throws IOException {
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf))) {
      ReaderImpl impl = (ReaderImpl) reader;
      InStream.StreamOptions compression = InStream.options()
          .withCodec(OrcCodecPool.getCodec(impl.getCompressionKind()))
          .withBufferSize(impl.getCompressionSize());
      DataReaderProperties properties = DataReaderProperties.builder()
          .withCompression(compression)
          .withFileSystemSupplier(impl.getFileSystemSupplier())
          .withPath(impl.path)
          .withFile(impl.takeFile())
          .withMaxDiskRangeChunkLimit(
              OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(impl.conf))
          .withZeroCopy(false)
          .build();
      return new CountingDataReader(
          RecordReaderUtils.createDefaultDataReader(properties), new Counters());
    }
  }

  private static String value(BytesColumnVector vector, int row) {
    return new String(vector.vector[row], vector.start[row], vector.length[row],
        StandardCharsets.UTF_8);
  }

  private static String hex(byte[] value) {
    StringBuilder builder = new StringBuilder(value.length * 2);
    for (byte b : value) {
      builder.append(String.format("%02x", b));
    }
    return builder.toString();
  }
}
