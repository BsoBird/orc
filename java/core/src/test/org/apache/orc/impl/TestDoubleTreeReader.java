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

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionCodec;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.apache.orc.impl.writer.StreamOptions;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the dense, null and parent null paths of the double reader, including
 * the bulk reads of the data and the present bits.
 */
public class TestDoubleTreeReader {

  // Use a small buffer size, so that the streams have many chunks and the
  // bulk reads have to cross the chunk boundaries.
  private static final int BUFFER_SIZE = 100;
  private static final int ROWS = 1000;
  // The batch sizes add up to the number of rows.
  private static final int[] BATCHES = {8, 1, 7, 8, 500, 476};

  private static double[] values(int rows) {
    double[] result = new double[rows];
    for (int row = 0; row < rows; ++row) {
      result[row] = row + 0.25;
    }
    return result;
  }

  private static InStream toStream(byte[] bytes, CompressionCodec codec) {
    return InStream.create("test", new BufferChunk(ByteBuffer.wrap(bytes), 0),
        0, bytes.length,
        InStream.options().withCodec(codec).withBufferSize(BUFFER_SIZE));
  }

  /**
   * Build a reader over the given rows. The present stream has a bit for each
   * row where the parent isn't null, and the data stream has a value for each
   * of the rows that isn't null in the present stream.
   */
  private static TreeReaderFactory.DoubleTreeReader createReader(
      double[] values, int rows, boolean[] isNull, boolean[] parentIsNull,
      CompressionCodec codec) throws Exception {
    return createReader(values, rows, isNull, parentIsNull, codec, null);
  }

  /**
   * Build a reader over the given rows, recording the data stream in dataOut so
   * that the tests can check how far the reader has read.
   */
  private static TreeReaderFactory.DoubleTreeReader createReader(
      double[] values, int rows, boolean[] isNull, boolean[] parentIsNull,
      CompressionCodec codec, InStream[] dataOut) throws Exception {
    InStream present = null;
    if (isNull != null) {
      StreamOptions options = new StreamOptions(BUFFER_SIZE);
      if (codec != null) {
        options.withCodec(codec, codec.getDefaultOptions());
      }
      TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
      BitFieldWriter out = new BitFieldWriter(
          new OutStream("test", options, collect), 1);
      for (int row = 0; row < rows; ++row) {
        if (parentIsNull == null || !parentIsNull[row]) {
          // the present stream uses a 1 bit for the rows that have a value
          out.write(isNull[row] ? 0 : 1);
        }
      }
      out.flush();
      present = toStream(collect.buffer.get(), codec);
    }
    StreamOptions options = new StreamOptions(BUFFER_SIZE);
    if (codec != null) {
      options.withCodec(codec, codec.getDefaultOptions());
    }
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    OutStream out = new OutStream("test", options, collect);
    SerializationUtils utils = new SerializationUtils();
    for (int row = 0; row < rows; ++row) {
      if ((parentIsNull == null || !parentIsNull[row]) &&
          (isNull == null || !isNull[row])) {
        utils.writeDouble(out, values[row]);
      }
    }
    out.flush();
    InStream data = toStream(collect.buffer.get(), codec);
    if (dataOut != null) {
      dataOut[0] = data;
    }
    return new TreeReaderFactory.DoubleTreeReader(0, present, data,
        new TreeReaderFactory.ReaderContext());
  }

  /**
   * Check a batch of the reader against the expected values and flags.
   *
   * @param parentMask the null flags of the parent for this batch, which are
   *                   indexed from the start of the batch
   * @param expectedNull the expected null flags of all of the rows, which are
   *                     indexed by the row number, or null if no row is null
   */
  private static void checkBatch(TreeReaderFactory.DoubleTreeReader reader,
                                 DoubleColumnVector vec,
                                 VectorizedRowBatch batch,
                                 boolean[] parentMask,
                                 boolean[] expectedNull,
                                 double[] values,
                                 int posn,
                                 int batchSize,
                                 boolean expectedNoNulls,
                                 boolean expectedRepeating) throws Exception {
    reader.nextVector(vec, parentMask, batchSize, batch, TypeReader.ReadPhase.ALL);
    assertEquals(expectedNoNulls, vec.noNulls, "noNulls at " + posn);
    assertEquals(expectedRepeating, vec.isRepeating, "isRepeating at " + posn);
    boolean allNull = true;
    for (int i = 0; i < batchSize; ++i) {
      if (expectedNull == null || !expectedNull[posn + i]) {
        allNull = false;
        break;
      }
    }
    for (int i = 0; i < batchSize; ++i) {
      final int row = posn + i;
      final boolean nullRow = expectedNull != null && expectedNull[row];
      assertEquals(nullRow, vec.isNull[i], "isNull at " + row);
      if (allNull) {
        // the reader only sets the first value of an all null batch and
        // marks the batch as repeating
        assertTrue(Double.isNaN(vec.vector[0]), "NaN at " + posn);
      } else if (nullRow) {
        assertTrue(Double.isNaN(vec.vector[i]), "NaN at " + row);
      } else {
        assertEquals(values[row], vec.vector[i], "value at " + row);
      }
    }
  }

  /**
   * Check that the data stream has been read up to the expected number of
   * bytes. A compressed stream doesn't expose the position of the data that it
   * has returned, so it is only checked for exhaustion at the end.
   */
  private static void assertConsumed(InStream data, long expectedBytes,
                                     CompressionCodec codec,
                                     boolean finished) throws Exception {
    if (codec == null) {
      assertEquals(expectedBytes, data.position, "data stream position");
    } else if (finished) {
      assertEquals(-1, data.read(), "the data stream should be exhausted");
    }
  }

  /**
   * Compute the flags that the reader is expected to set for a batch.
   */
  private static boolean[] batchFlags(boolean[] expectedNull, int posn,
                                       int batchSize, boolean[] noNullsOut) {
    boolean allNull = true;
    boolean anyNull = false;
    for (int i = 0; i < batchSize; ++i) {
      final boolean nullRow = expectedNull != null && expectedNull[posn + i];
      anyNull |= nullRow;
      allNull &= nullRow;
    }
    noNullsOut[0] = !anyNull;
    return new boolean[]{allNull};
  }

  private void checkDense(CompressionCodec codec) throws Exception {
    double[] values = values(ROWS);
    InStream[] dataOut = new InStream[1];
    TreeReaderFactory.DoubleTreeReader reader =
        createReader(values, ROWS, null, null, codec, dataOut);
    DoubleColumnVector vec = new DoubleColumnVector(ROWS);
    vec.reset();
    VectorizedRowBatch batch = new VectorizedRowBatch(ROWS);
    // A zero length batch leaves the vector and the reader alone.
    reader.nextVector(vec, null, 0, batch, TypeReader.ReadPhase.ALL);
    assertTrue(vec.noNulls);
    assertFalse(vec.isRepeating);
    assertConsumed(dataOut[0], 0, codec, false);
    int posn = 0;
    for (int batchSize : BATCHES) {
      // the values are all different, so the vector can't be repeating
      checkBatch(reader, vec, batch, null, null, values, posn, batchSize,
          true, false);
      posn += batchSize;
      assertConsumed(dataOut[0], posn * 8L, codec, posn == ROWS);
    }
    assertEquals(ROWS, posn);
    // the data is exhausted
    assertThrows(EOFException.class,
        () -> reader.nextVector(vec, null, 10, batch, TypeReader.ReadPhase.ALL));
  }

  @Test
  public void testDenseUncompressed() throws Exception {
    checkDense(null);
  }

  @Test
  public void testDenseZlib() throws Exception {
    checkDense(new ZlibCodec());
  }

  private void checkRepeating(CompressionCodec codec) throws Exception {
    final int rows = 100;
    double[] values = new double[rows];
    Arrays.fill(values, 42.0);
    TreeReaderFactory.DoubleTreeReader reader =
        createReader(values, rows, null, null, codec);
    DoubleColumnVector vec = new DoubleColumnVector(rows);
    vec.reset();
    VectorizedRowBatch batch = new VectorizedRowBatch(rows);
    // a batch of more than one value can be repeating
    reader.nextVector(vec, null, 8, batch, TypeReader.ReadPhase.ALL);
    assertTrue(vec.isRepeating);
    assertTrue(vec.noNulls);
    // a zero length batch doesn't clear the flag
    reader.nextVector(vec, null, 0, batch, TypeReader.ReadPhase.ALL);
    assertTrue(vec.isRepeating);
    // a single value can't be repeating
    reader.nextVector(vec, null, 1, batch, TypeReader.ReadPhase.ALL);
    assertFalse(vec.isRepeating);
    // different values in a large batch aren't repeating
    double[] mixed = new double[rows];
    mixed[0] = 42.0;
    mixed[1] = 43.0;
    Arrays.fill(mixed, 2, rows, 42.0);
    TreeReaderFactory.DoubleTreeReader other =
        createReader(mixed, rows, null, null, codec);
    other.nextVector(vec, null, rows, batch, TypeReader.ReadPhase.ALL);
    assertFalse(vec.isRepeating);
    assertEquals(42.0, vec.vector[0]);
    assertEquals(43.0, vec.vector[1]);
  }

  @Test
  public void testRepeatingUncompressed() throws Exception {
    checkRepeating(null);
  }

  @Test
  public void testRepeatingZlib() throws Exception {
    checkRepeating(new ZlibCodec());
  }

  private void checkNulls(CompressionCodec codec) throws Exception {
    double[] values = values(ROWS);
    boolean[] isNull = new boolean[ROWS];
    // nulls at the start, at the end and spread through the middle
    for (int row = 0; row < ROWS; ++row) {
      isNull[row] = row % 11 == 3 || row < 4 || row >= ROWS - 6;
    }
    InStream[] dataOut = new InStream[1];
    TreeReaderFactory.DoubleTreeReader reader =
        createReader(values, ROWS, isNull, null, codec, dataOut);
    DoubleColumnVector vec = new DoubleColumnVector(ROWS);
    vec.reset();
    VectorizedRowBatch batch = new VectorizedRowBatch(ROWS);
    int posn = 0;
    int numValues = 0;
    for (int batchSize : BATCHES) {
      final boolean[] noNulls = new boolean[1];
      final boolean[] allNull = batchFlags(isNull, posn, batchSize, noNulls);
      // the values are all different, so a batch without nulls isn't repeating
      checkBatch(reader, vec, batch, null, isNull, values, posn, batchSize,
          noNulls[0], allNull[0]);
      posn += batchSize;
      for (int i = 0; i < batchSize; ++i) {
        if (!isNull[posn - batchSize + i]) {
          ++numValues;
        }
      }
      // only the rows that aren't null have a value in the data stream
      assertConsumed(dataOut[0], numValues * 8L, codec, posn == ROWS);
    }
    assertEquals(ROWS, posn);
  }

  @Test
  public void testNullsUncompressed() throws Exception {
    checkNulls(null);
  }

  @Test
  public void testNullsZlib() throws Exception {
    checkNulls(new ZlibCodec());
  }

  /**
   * The present stream has to be read for every batch, even when the previous
   * batch had no nulls at all.
   */
  @Test
  public void testPresentAfterNoNullBatch() throws Exception {
    for (CompressionCodec codec : new CompressionCodec[]{null,
        new ZlibCodec()}) {
      double[] values = values(ROWS);
      boolean[] isNull = new boolean[ROWS];
      for (int row = 0; row < ROWS; ++row) {
        isNull[row] = row >= 8;
      }
      TreeReaderFactory.DoubleTreeReader reader =
          createReader(values, ROWS, isNull, null, codec);
      DoubleColumnVector vec = new DoubleColumnVector(ROWS);
      vec.reset();
      VectorizedRowBatch batch = new VectorizedRowBatch(ROWS);
      // the first 8 rows are all present
      reader.nextVector(vec, null, 8, batch, TypeReader.ReadPhase.ALL);
      assertTrue(vec.noNulls);
      assertFalse(vec.isRepeating);
      for (int i = 0; i < 8; ++i) {
        assertEquals(values[i], vec.vector[i], "value at " + i);
      }
      // the rest are all null, so the bits have to be found in the present
      // stream even though the last batch had no nulls
      reader.nextVector(vec, null, 100, batch, TypeReader.ReadPhase.ALL);
      assertFalse(vec.noNulls);
      assertTrue(vec.isRepeating);
      for (int i = 0; i < 100; ++i) {
        assertTrue(vec.isNull[i], "isNull at " + (8 + i));
      }
      assertTrue(Double.isNaN(vec.vector[0]), "NaN at 8");
    }
  }

  /**
   * Check the paths where the parent has nulls, with and without a present
   * stream of the column itself.
   */
  private void checkParentNulls(boolean withPresent, CompressionCodec codec)
      throws Exception {
    double[] values = values(ROWS);
    boolean[] parentIsNull = new boolean[ROWS];
    boolean[] isNull = null;
    for (int row = 0; row < ROWS; ++row) {
      // the parent is null for runs of rows at the start, in the middle
      // and at the end
      parentIsNull[row] = row < 5 || (row / 40) % 3 == 1 ||
          row >= ROWS - 3;
    }
    if (withPresent) {
      isNull = new boolean[ROWS];
      for (int row = 0; row < ROWS; ++row) {
        // the column itself has nulls inside the rows that the parent has
        if (!parentIsNull[row]) {
          isNull[row] = row % 7 == 2;
        }
      }
    }
    // the row is null if the parent is null or the column has a null
    boolean[] expectedNull = new boolean[ROWS];
    for (int row = 0; row < ROWS; ++row) {
      expectedNull[row] = parentIsNull[row] ||
          (isNull != null && isNull[row]);
    }
    TreeReaderFactory.DoubleTreeReader reader =
        createReader(values, ROWS, isNull, parentIsNull, codec);
    DoubleColumnVector vec = new DoubleColumnVector(ROWS);
    vec.reset();
    VectorizedRowBatch batch = new VectorizedRowBatch(ROWS);
    int posn = 0;
    for (int batchSize : BATCHES) {
      final boolean[] noNulls = new boolean[1];
      final boolean[] allNull =
          batchFlags(expectedNull, posn, batchSize, noNulls);
      // the parent's null flags are relative to the start of the batch
      final boolean[] parentMask =
          Arrays.copyOfRange(parentIsNull, posn, posn + batchSize);
      checkBatch(reader, vec, batch, parentMask, expectedNull, values, posn,
          batchSize, noNulls[0], allNull[0]);
      posn += batchSize;
    }
    assertEquals(ROWS, posn);
    assertThrows(EOFException.class,
        () -> reader.nextVector(vec, null, 10, batch, TypeReader.ReadPhase.ALL));
  }

  @Test
  public void testParentNullsUncompressed() throws Exception {
    checkParentNulls(false, null);
  }

  @Test
  public void testParentNullsZlib() throws Exception {
    checkParentNulls(false, new ZlibCodec());
  }

  @Test
  public void testParentNullsWithPresentUncompressed() throws Exception {
    checkParentNulls(true, null);
  }

  @Test
  public void testParentNullsWithPresentZlib() throws Exception {
    checkParentNulls(true, new ZlibCodec());
  }

  /**
   * Skipping rows has to leave the data and the present streams at the right
   * place.
   */
  private void checkSkipRows(boolean withPresent, CompressionCodec codec)
      throws Exception {
    double[] values = values(ROWS);
    boolean[] isNull = null;
    if (withPresent) {
      isNull = new boolean[ROWS];
      for (int row = 0; row < ROWS; ++row) {
        isNull[row] = row % 5 == 1;
      }
    }
    TreeReaderFactory.DoubleTreeReader reader =
        createReader(values, ROWS, isNull, null, codec);
    DoubleColumnVector vec = new DoubleColumnVector(ROWS);
    vec.reset();
    VectorizedRowBatch batch = new VectorizedRowBatch(ROWS);
    // read the first 10 rows
    reader.nextVector(vec, null, 10, batch, TypeReader.ReadPhase.ALL);
    assertEquals(values[0], vec.vector[0]);
    assertEquals(values[9], vec.vector[9]);
    // skip 137 rows and check that the values line up with the row numbers
    reader.skipRows(137, TypeReader.ReadPhase.ALL);
    final boolean[] noNulls = new boolean[1];
    final boolean[] allNull = batchFlags(isNull, 147, 100, noNulls);
    checkBatch(reader, vec, batch, null, isNull, values, 147, 100,
        noNulls[0], allNull[0]);
    // skip another 213 rows, which are counted from the end of the last batch
    reader.skipRows(213, TypeReader.ReadPhase.ALL);
    final boolean[] noNulls2 = new boolean[1];
    final boolean[] allNull2 = batchFlags(isNull, 460, 100, noNulls2);
    checkBatch(reader, vec, batch, null, isNull, values, 460, 100,
        noNulls2[0], allNull2[0]);
    // skip the rest of the rows and check that the data is exhausted
    reader.skipRows(ROWS - 560, TypeReader.ReadPhase.ALL);
    assertThrows(EOFException.class,
        () -> reader.nextVector(vec, null, 10, batch, TypeReader.ReadPhase.ALL));
  }

  @Test
  public void testSkipRowsUncompressed() throws Exception {
    checkSkipRows(false, null);
  }

  @Test
  public void testSkipRowsZlib() throws Exception {
    checkSkipRows(false, new ZlibCodec());
  }

  @Test
  public void testSkipRowsWithPresentUncompressed() throws Exception {
    checkSkipRows(true, null);
  }

  @Test
  public void testSkipRowsWithPresentZlib() throws Exception {
    checkSkipRows(true, new ZlibCodec());
  }

  /**
   * Skipping rows that have a present stream has to work when the skip is
   * longer than the largest batch of bits that the reader reads at a time.
   */
  private void checkSkipRowsLarge(CompressionCodec codec) throws Exception {
    final int LARGE_ROWS = 70000;
    double[] values = values(LARGE_ROWS);
    boolean[] isNull = new boolean[LARGE_ROWS];
    for (int row = 0; row < LARGE_ROWS; ++row) {
      isNull[row] = row % 5 == 1;
    }
    TreeReaderFactory.DoubleTreeReader reader =
        createReader(values, LARGE_ROWS, isNull, null, codec);
    DoubleColumnVector vec = new DoubleColumnVector(ROWS);
    vec.reset();
    VectorizedRowBatch batch = new VectorizedRowBatch(ROWS);
    // skip more rows than the largest batch of present bits
    reader.skipRows(65536, TypeReader.ReadPhase.ALL);
    final boolean[] noNulls = new boolean[1];
    final boolean[] allNull = batchFlags(isNull, 65536, ROWS, noNulls);
    checkBatch(reader, vec, batch, null, isNull, values, 65536, ROWS,
        noNulls[0], allNull[0]);
    // and skip the rest of the rows, which leaves the data exhausted
    reader.skipRows(LARGE_ROWS - 65536 - ROWS, TypeReader.ReadPhase.ALL);
    assertThrows(EOFException.class,
        () -> reader.nextVector(vec, null, 10, batch, TypeReader.ReadPhase.ALL));
  }

  @Test
  public void testSkipRowsLargeUncompressed() throws Exception {
    checkSkipRowsLarge(null);
  }

  @Test
  public void testSkipRowsLargeZlib() throws Exception {
    checkSkipRowsLarge(new ZlibCodec());
  }
}
