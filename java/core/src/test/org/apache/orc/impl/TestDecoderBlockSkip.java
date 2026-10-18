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
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the decoder level skipping of contiguous filtered runs.
 * <p>
 * ORC has no page index, so {@link OrcProto.RowIndexEntry} positions exist only
 * per row group. The decoder already uses them:
 * <ul>
 *   <li>{@code RecordReaderImpl.seekToRowEntry} turns a row index entry into a
 *       {@code PositionProvider[]} and every {@code TreeReader.seek} jumps its
 *       streams straight to those offsets,</li>
 *   <li>{@code RecordReaderImpl.advanceToNextRow} uses it to jump over the row
 *       groups that the statistics excluded,</li>
 *   <li>{@code RecordReaderImpl.prepareFollowReaders} uses it to jump the
 *       follower streams over the runs of batches that the filter emptied.</li>
 * </ul>
 * These tests pin that behavior down: every scan must return the same rows as an
 * unfiltered scan, and the recorded IO ranges must never touch the bytes of the
 * row groups that the filter removed.
 */
public class TestDecoderBlockSkip {

  private static final String PRUNING = "orc.sarg.stripe.pruning.enabled";
  private static final TypeDescription SCHEMA =
      TypeDescription.fromString("struct<x:int,payload:string>");
  /** The payload is fat so that its bytes dominate the file. */
  private static final int PAYLOAD_SIZE = 200;
  private static final int ROWS = 40000;
  private static final int STRIDE = 10000;
  private static final int RG_COUNT = ROWS / STRIDE;
  private static final int X_COLUMN = 1;
  private static final int PAYLOAD_COLUMN = 2;

  private final Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  private Configuration conf;
  private Path testFilePath;

  @BeforeEach
  public void setup(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    testFilePath = new Path(workDir, "TestDecoderBlockSkip." +
        testInfo.getTestMethod().get().getName().replaceFirst("\\[[0-9]+\\]", "")
        + "." + UUID.randomUUID() + ".orc");
    FileSystem.getLocal(conf).delete(testFilePath, false);
    writeFile();
  }

  // ------------------------------------------------------------------
  // fixture
  // ------------------------------------------------------------------

  /**
   * A single stripe of {@link #RG_COUNT} row groups, with x = the row number so
   * that the row group statistics are disjoint.
   */
  private void writeFile() throws IOException {
    try (Writer writer = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf)
        .setSchema(SCHEMA)
        .compress(CompressionKind.NONE)
        .rowIndexStride(STRIDE)
        .stripeSize(256 * 1024 * 1024)
        .bufferSize(256 * 1024))) {
      VectorizedRowBatch batch = SCHEMA.createRowBatch(1024);
      LongColumnVector x = (LongColumnVector) batch.cols[0];
      BytesColumnVector payload = (BytesColumnVector) batch.cols[1];
      for (int row = 0; row < ROWS; row += batch.getMaxSize()) {
        batch.reset();
        int size = Math.min(batch.getMaxSize(), ROWS - row);
        for (int r = 0; r < size; ++r) {
          x.vector[r] = row + r;
          payload.setVal(r, payloadOf(row + r));
        }
        batch.size = size;
        writer.addRowBatch(batch);
      }
    }
    try (Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf))) {
      assertEquals(1, reader.getStripes().size(), "the fixture needs one stripe");
      assertEquals(STRIDE, reader.getRowIndexStride());
    }
  }

  /** A per row pseudo random payload, so nothing gets run length encoded. */
  private static byte[] payloadOf(int row) {
    byte[] bytes = new byte[PAYLOAD_SIZE];
    long state = 0x9E3779B97F4A7C15L + (long) row * 0xBF58476D1CE4E5B9L;
    for (int i = 0; i < PAYLOAD_SIZE; ++i) {
      state = state * 6364136223846793005L + 1442695040888963407L;
      bytes[i] = (byte) (state >>> 56);
    }
    return bytes;
  }

  private static long[] slice(long[] values, int from, int to) {
    return Arrays.copyOfRange(values, from, to);
  }

  // ------------------------------------------------------------------
  // IO recording
  // ------------------------------------------------------------------

  /** Every byte range that the reader asked the file system for. */
  private static final class Recorder {
    private final List<long[]> ranges = new ArrayList<>();
    private long bytesRead = 0;
    private int readCalls = 0;

    void record(long offset, int length) {
      readCalls += 1;
      bytesRead += length;
      ranges.add(new long[]{offset, offset + length});
    }
  }

  private static final class RecordingDataReader implements DataReader {
    private final DataReader delegate;
    private final Recorder recorder;

    RecordingDataReader(Recorder recorder, DataReader delegate) {
      this.recorder = recorder;
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
      for (BufferChunk chunk = list.get(); chunk != null;
           chunk = (BufferChunk) chunk.next) {
        if (!chunk.hasData()) {
          recorder.record(chunk.getOffset(), (int) chunk.getLength());
        }
      }
      return delegate.readFileData(list, doForceDirect);
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return delegate.isTrackingDiskRanges();
    }

    @Override
    public void releaseBuffer(ByteBuffer toRelease) {
      delegate.releaseBuffer(toRelease);
    }

    @Override
    public void releaseAllBuffers() {
      delegate.releaseAllBuffers();
    }

    @Override
    public DataReader clone() {
      return new RecordingDataReader(recorder, delegate.clone());
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

  private static DataReader newDataReader(Reader reader) throws IOException {
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
    return RecordReaderUtils.createDefaultDataReader(properties);
  }

  // ------------------------------------------------------------------
  // scanning
  // ------------------------------------------------------------------

  /** The rows of one scan, hash per row, plus the IO it caused. */
  private static final class ScanResult {
    private final long[] rowHashes;
    private final Recorder recorder;

    ScanResult(long[] rowHashes, Recorder recorder) {
      this.rowHashes = rowHashes;
      this.recorder = recorder;
    }

    long rows() {
      return rowHashes.length;
    }
  }

  private ScanResult scan(Consumer<Reader.Options> options) throws Exception {
    Configuration readConf = new Configuration(conf);
    readConf.setBoolean(PRUNING, true);
    Recorder recorder = new Recorder();
    List<Long> hashes = new ArrayList<>(ROWS);
    try (Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(readConf))) {
      Reader.Options rowOptions = reader.options();
      options.accept(rowOptions);
      DataReader counter =
          new RecordingDataReader(recorder, newDataReader(reader));
      try (RecordReader rows = reader.rows(rowOptions.dataReader(counter))) {
        VectorizedRowBatch batch = SCHEMA.createRowBatch();
        LongColumnVector x = (LongColumnVector) batch.cols[0];
        BytesColumnVector payload = (BytesColumnVector) batch.cols[1];
        while (rows.nextBatch(batch)) {
          for (int r = 0; r < batch.size; ++r) {
            int row = batch.selectedInUse ? batch.selected[r] : r;
            byte[] value = Arrays.copyOfRange(payload.vector[row],
                payload.start[row], payload.start[row] + payload.length[row]);
            long hash = 1125899906842597L;
            hash = 31 * hash + x.vector[row];
            for (byte b: value) {
              hash = 31 * hash + b;
            }
            hashes.add(hash);
          }
        }
      }
    }
    long[] result = new long[hashes.size()];
    for (int r = 0; r < result.length; ++r) {
      result[r] = hashes.get(r);
    }
    return new ScanResult(result, recorder);
  }

  /** Reject the rows whose x is in [from, to). */
  private static Consumer<OrcFilterContext> rejectRange(int from, int to) {
    return context -> {
      int size = context.getSelectedSize();
      boolean selectedInUse = context.isSelectedInUse();
      int[] selected = context.getSelected();
      ColumnVector[] branch = context.findColumnVector("x");
      LongColumnVector x = (LongColumnVector) branch[branch.length - 1];
      int keep = 0;
      for (int i = 0; i < size; ++i) {
        int row = selectedInUse ? selected[i] : i;
        if (x.vector[row] < from || x.vector[row] >= to) {
          selected[keep++] = row;
        }
      }
      context.setSelected(selected);
      context.setSelectedSize(keep);
      context.setSelectedInUse(true);
    };
  }

  // ------------------------------------------------------------------
  // row index assertions
  // ------------------------------------------------------------------

  /**
   * The absolute file offsets of the bytes that one row group occupies in the
   * DATA stream of one column.
   * <p>
   * The row index positions are relative to the start of their own stream, which
   * is how {@code InStream.seek} uses them, so the span is the stream offset
   * from the stripe footer plus the row index positions of this row group and of
   * the next one. The fixture is uncompressed and has no nulls, so
   * {@code RecordReaderUtils.getIndexPosition} puts the DATA stream at index 0
   * of the positions for both columns.
   */
  private long[] dataSpan(int columnId, int rowGroup) throws IOException {
    try (Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
         RecordReader rows = reader.rows()) {
      RecordReaderImpl impl = (RecordReaderImpl) rows;
      StripeInformation stripe = reader.getStripes().get(0);
      // The footer lists the streams in file order without their offsets, so
      // accumulate the lengths the way StripePlanner.findStreams does.
      long streamOffset = -1;
      long streamLength = -1;
      long offset = stripe.getOffset();
      for (OrcProto.Stream stream : impl.readStripeFooter(stripe)
          .getStreamsList()) {
        if (stream.getColumn() == columnId &&
            stream.getKind() == OrcProto.Stream.Kind.DATA) {
          streamOffset = offset;
          streamLength = stream.getLength();
        }
        offset += stream.getLength();
      }
      assertTrue(streamOffset >= 0, "column " + columnId + " has no DATA stream");
      OrcProto.RowIndex rowIndex = impl.readRowIndex(0, null, null)
          .getRowGroupIndex()[columnId];
      assertEquals(RG_COUNT, rowIndex.getEntryCount());
      long start = streamOffset + rowIndex.getEntry(rowGroup).getPositions(0);
      long end = rowGroup + 1 == rowIndex.getEntryCount()
          ? streamOffset + streamLength
          : streamOffset + rowIndex.getEntry(rowGroup + 1).getPositions(0);
      assertTrue(end > start,
          "empty DATA span for column " + columnId + " row group " + rowGroup);
      return new long[]{start, end};
    }
  }

  /** No byte of that range may have been fetched from the file. */
  private void assertNotRead(ScanResult scan, long start, long end, String what) {
    for (long[] range : scan.recorder.ranges) {
      assertTrue(range[1] <= start || range[0] >= end,
          what + " [" + start + ", " + end + ") was read as "
              + Arrays.toString(range));
    }
  }

  /** No byte of the payload of those row groups may have been fetched. */
  private void assertRowGroupsNotRead(ScanResult scan, int fromRowGroup,
                                      int toRowGroup) throws IOException {
    for (int rg = fromRowGroup; rg < toRowGroup; ++rg) {
      long[] span = dataSpan(PAYLOAD_COLUMN, rg);
      assertEquals((long) STRIDE * PAYLOAD_SIZE, span[1] - span[0],
          "the payload DATA stream of row group " + rg);
      assertNotRead(scan, span[0], span[1],
          "the payload bytes of row group " + rg);
    }
  }

  // ------------------------------------------------------------------
  // tests
  // ------------------------------------------------------------------

  /**
   * The search argument excludes row groups through the row group statistics.
   * The excluded row groups must be seeked over, not decoded and dropped.
   */
  @Test
  public void testPrunedRowGroupsAreNeverRead() throws Exception {
    ScanResult all = scan(options -> { });
    assertEquals(ROWS, all.rows());

    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .lessThan("x", PredicateLeaf.Type.LONG, 30000L)
        .end().build();
    ScanResult pruned =
        scan(options -> options.searchArgument(sarg, new String[]{"x"}));

    assertEquals(STRIDE, pruned.rows());
    assertArrayEquals(slice(all.rowHashes, 30000, ROWS), pruned.rowHashes);
    assertRowGroupsNotRead(pruned, 0, RG_COUNT - 1);
    assertTrue(pruned.recorder.bytesRead * 2 < all.recorder.bytesRead,
        "expected about a quarter of the bytes, read " +
            pruned.recorder.bytesRead + " of " + all.recorder.bytesRead);
  }

  /**
   * With the search argument applied as a filter the same row groups are
   * dropped, and the LEADERS/FOLLOWERS split must not touch their bytes.
   */
  @Test
  public void testSargAsFilterSeeksOverDroppedRowGroups() throws Exception {
    ScanResult all = scan(options -> { });
    assertEquals(ROWS, all.rows());

    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .lessThan("x", PredicateLeaf.Type.LONG, 30000L)
        .end().build();
    ScanResult filtered = scan(options -> options
        .useSelected(true)
        .searchArgument(sarg, new String[]{"x"})
        .allowSARGToFilter(true));

    assertArrayEquals(slice(all.rowHashes, 30000, ROWS), filtered.rowHashes);
    assertRowGroupsNotRead(filtered, 0, RG_COUNT - 1);
    assertTrue(filtered.recorder.bytesRead * 2 < all.recorder.bytesRead,
        "expected about a quarter of the bytes, read " +
            filtered.recorder.bytesRead + " of " + all.recorder.bytesRead);
  }

  /**
   * A row filter callback has no statistics to prune with, so the reader only
   * learns about the run from the batches it already decoded. The first batch
   * that survives is the first one after the run, so the follower streams must
   * be seeked to the row group that holds it.
   * <p>
   * The rows prove that the seek landed on the row group boundary. The bytes of
   * the skipped run are still fetched, because without a search argument
   * {@code pickRowGroups} returns null and
   * {@code StripePlanner.readFollowData} falls back to planning the whole
   * follower stream. Closing that needs the row index of the follower columns,
   * which the reader only reads for the filter and search argument columns.
   */
  @Test
  public void testRowFilterSeeksToTheFirstSurvivingRowGroup() throws Exception {
    ScanResult all = scan(options -> { });
    assertEquals(ROWS, all.rows());

    ScanResult filtered = scan(options -> options
        .useSelected(true)
        .setRowFilter(new String[]{"x"}, rejectRange(0, 30000)));

    assertEquals(ROWS - 30000, filtered.rows());
    assertArrayEquals(slice(all.rowHashes, 30000, ROWS), filtered.rowHashes);
    assertTrue(filtered.recorder.bytesRead * 2 > all.recorder.bytesRead,
        "expected the whole follower stream, because nothing prunes it, read " +
            filtered.recorder.bytesRead + " of " + all.recorder.bytesRead);
  }

  /**
   * A row filter callback that drops every row must leave the follower streams
   * unplanned, because no batch ever survives. The filter column is still read,
   * because without a search argument the reader has no pruning signal.
   */
  @Test
  public void testFullyFilteredFileReadsNoFollowerStreams() throws Exception {
    ScanResult all = scan(options -> { });
    assertEquals(ROWS, all.rows());

    ScanResult filtered = scan(options -> options
        .useSelected(true)
        .setRowFilter(new String[]{"x"}, rejectRange(0, ROWS)));

    assertEquals(0, filtered.rows());
    assertRowGroupsNotRead(filtered, 0, RG_COUNT);
    // The filter column has no pruning signal at all, so it still has to be
    // decoded row group by row group.
    long filterColumnStart = dataSpan(X_COLUMN, 0)[0];
    boolean readFilterColumn = false;
    for (long[] range : filtered.recorder.ranges) {
      if (range[0] <= filterColumnStart && range[1] > filterColumnStart) {
        readFilterColumn = true;
      }
    }
    assertTrue(readFilterColumn,
        "the unpruned filter column has to be decoded, but was skipped");
    assertTrue(filtered.recorder.bytesRead * 10 < all.recorder.bytesRead,
        "expected only the filter column bytes, read " +
            filtered.recorder.bytesRead + " of " + all.recorder.bytesRead);
  }

  /**
   * A run inside a row group can not be seeked over as a whole, the row group
   * holds rows on both sides of it. The decoder has to skip the run with
   * {@code skipRows} instead, so every row group is still fetched, but the rows
   * have to come out exactly as they do without the filter.
   */
  @Test
  public void testRunInsideOneRowGroupStillMatchesTheBaseline() throws Exception {
    ScanResult all = scan(options -> { });
    assertEquals(ROWS, all.rows());

    int from = 2 * STRIDE + 250;
    int to = 2 * STRIDE + 7500;
    ScanResult filtered = scan(options -> options
        .useSelected(true)
        .setRowFilter(new String[]{"x"}, rejectRange(from, to)));

    long[] expected = new long[all.rowHashes.length - (to - from)];
    System.arraycopy(all.rowHashes, 0, expected, 0, from);
    System.arraycopy(all.rowHashes, to, expected, from,
        all.rowHashes.length - to);
    assertEquals(expected.length, filtered.rows());
    assertArrayEquals(expected, filtered.rowHashes);
    // the row group that holds the run survives on both sides of it
    assertTrue(filtered.recorder.bytesRead > all.recorder.bytesRead / 2,
        "expected the row groups around the run to be read, read " +
            filtered.recorder.bytesRead + " of " + all.recorder.bytesRead);
  }
}
