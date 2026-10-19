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

import org.apache.orc.CompressionCodec;
import org.apache.orc.impl.writer.StreamOptions;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBitFieldReader {

  public void runSeekTest(CompressionCodec codec) throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    final int COUNT = 16384;
    StreamOptions options = new StreamOptions(500);
    if (codec != null) {
      options.withCodec(codec, codec.getDefaultOptions());
    }
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", options, collect), 1);
    TestInStream.PositionCollector[] positions =
        new TestInStream.PositionCollector[COUNT];
    for(int i=0; i < COUNT; ++i) {
      positions[i] = new TestInStream.PositionCollector();
      out.getPosition(positions[i]);
      // test runs, non-runs
      if (i < COUNT / 2) {
        out.write(i & 1);
      } else {
        out.write((i/3) & 1);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(500)));
    for(int i=0; i < COUNT; ++i) {
      int x = in.next();
      if (i < COUNT / 2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
    }
    for(int i=COUNT-1; i >= 0; --i) {
      in.seek(positions[i]);
      int x = in.next();
      if (i < COUNT / 2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
    }
  }

  @Test
  public void testUncompressedSeek() throws Exception {
    runSeekTest(null);
  }

  @Test
  public void testCompressedSeek() throws Exception {
    runSeekTest(new ZlibCodec());
  }

  @Test
  public void testSkips() throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", new StreamOptions(100), collect), 1);
    final int COUNT = 16384;
    for(int i=0; i < COUNT; ++i) {
      if (i < COUNT/2) {
        out.write(i & 1);
      } else {
        out.write((i/3) & 1);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining()));
    for(int i=0; i < COUNT; i += 5) {
      int x = in.next();
      if (i < COUNT/2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
      if (i < COUNT - 5) {
        in.skip(4);
      }
      in.skip(0);
    }
  }

  @Test
  public void testSeekSkip() throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", new StreamOptions(100), collect), 1);
    final int COUNT = 256;
    TestInStream.PositionCollector posn = new TestInStream.PositionCollector();
    for(int i=0; i < COUNT; ++i) {
      if (i == 200) {
        out.getPosition(posn);
      }
      if (i < COUNT/2) {
        out.write(i & 1);
      } else {
        out.write((i/3) & 1);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining()));
    in.seek(posn);
    in.skip(10);
    for(int r = 210; r < COUNT; ++r) {
      int x = in.next();
      if (r < COUNT/2) {
        assertEquals(r & 1, x);
      } else {
        assertEquals((r/3) & 1, x);
      }
    }
  }

  private static final int BULK_COUNT = 16384;
  private static final int[] BULK_BATCHES =
      {0, 1, 7, 8, 9, 100, 1024, 3, 2000, 1024, 21};

  /**
   * The bit at the given index in the bit field. The first half alternates, so
   * that no two adjacent bits are alike, and the second half has runs of 3.
   */
  private static int bitAt(int index) {
    return index < BULK_COUNT / 2 ? index & 1 : (index / 3) & 1;
  }

  /**
   * Write a bit of the field for each row that isn't null in the parent, using
   * the given codec, and return a reader for the result.
   */
  private static BitFieldReader writeBits(CompressionCodec codec, int rows,
                                          boolean[] parentNullRow)
      throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    StreamOptions options = new StreamOptions(100);
    if (codec != null) {
      options.withCodec(codec, codec.getDefaultOptions());
    }
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", options, collect), 1);
    int index = 0;
    for (int row = 0; row < rows; ++row) {
      if (parentNullRow == null || !parentNullRow[row]) {
        out.write(bitAt(index++));
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    return new BitFieldReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(100)));
  }

  /**
   * Check that nextVector() sets isNull the same way that reading the bits one
   * at a time would, for a set of batch sizes that cross the byte and the
   * buffer boundaries.
   *
   * @param rows the number of rows that the field covers
   * @param parentNullRow the rows that are null in the parent, or null
   */
  private static void checkBulk(BitFieldReader in, int rows,
                                boolean[] parentNullRow) throws Exception {
    // The rows that are null in the parent don't have a bit of their own, so
    // find the bit of each row in this field.
    final int[] bitOfRow = new int[rows];
    for (int row = 0, bit = 0; row < rows; ++row) {
      bitOfRow[row] = (parentNullRow == null || !parentNullRow[row]) ? bit++ : -1;
    }
    int posn = 0;
    int index = 0;
    while (posn < rows) {
      final int size = Math.min(BULK_BATCHES[index++ % BULK_BATCHES.length],
          rows - posn);
      final boolean[] parentIsNull = new boolean[size];
      if (parentNullRow != null) {
        System.arraycopy(parentNullRow, posn, parentIsNull, 0, size);
      }
      final boolean[] isNull = new boolean[size];
      final long nonNulls = in.nextVector(isNull, parentIsNull, 0, size);
      long expected = 0;
      for (int i = 0; i < size; ++i) {
        final int row = posn + i;
        if (bitOfRow[row] < 0) {
          assertTrue(isNull[i], "parent null at " + row);
        } else if (bitAt(bitOfRow[row]) == 1) {
          // the bit is set, so the row has a value
          assertFalse(isNull[i], "not null at " + row);
          expected += 1;
        } else {
          assertTrue(isNull[i], "null at " + row);
        }
      }
      assertEquals(expected, nonNulls, "nonNulls at " + posn);
      posn += size;
    }
    assertEquals(rows, posn);
  }

  private void runBulkTest(CompressionCodec codec) throws Exception {
    BitFieldReader in = writeBits(codec, BULK_COUNT, null);
    checkBulk(in, BULK_COUNT, null);
    // all of the bits have been consumed
    EOFException e = assertThrows(EOFException.class,
        () -> in.nextVector(new boolean[1], null, 0, 1));
    assertTrue(e.getMessage().contains("Read past end of bit field"),
        "message: " + e.getMessage());
  }

  @Test
  public void testBulkUncompressed() throws Exception {
    runBulkTest(null);
  }

  @Test
  public void testBulkCompressed() throws Exception {
    runBulkTest(new ZlibCodec());
  }

  @Test
  public void testBulkParentNulls() throws Exception {
    final int ROWS = 4096;
    // The parent is null for a run of 37 rows out of every 185, which puts
    // runs of parent nulls at the start, in the middle and at the end.
    boolean[] parentNullRow = new boolean[ROWS];
    for (int row = 0; row < ROWS; ++row) {
      parentNullRow[row] = (row / 37) % 5 == 0;
    }
    for (CompressionCodec codec : new CompressionCodec[]{null,
        new ZlibCodec()}) {
      BitFieldReader in = writeBits(codec, ROWS, parentNullRow);
      checkBulk(in, ROWS, parentNullRow);
    }
  }

  @Test
  public void testBulkOffsetSeekAndSkip() throws Exception {
    final int COUNT = 512;
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", new StreamOptions(100), collect), 1);
    TestInStream.PositionCollector[] positions =
        new TestInStream.PositionCollector[COUNT];
    for (int bit = 0; bit < COUNT; ++bit) {
      positions[bit] = new TestInStream.PositionCollector();
      out.getPosition(positions[bit]);
      out.write(bitAt(bit));
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining()));
    // the results can start part way into the isNull array
    boolean[] isNull = new boolean[64];
    assertEquals(10, in.nextVector(isNull, null, 5, 20));
    for (int i = 0; i < 20; ++i) {
      assertEquals(bitAt(i) == 0, isNull[5 + i], "bit at " + i);
    }
    // skipping bits after a bulk read lines up with next()
    in.skip(3);
    assertEquals(bitAt(23), in.next());
    // and so does reading more of the bits in bulk
    assertEquals(10, in.nextVector(isNull, null, 0, 20));
    // seeking back gives the right bits
    in.seek(positions[300]);
    for (int bit = 300; bit < 305; ++bit) {
      assertEquals(bitAt(bit), in.next(), "bit at " + bit);
    }
    in.skip(7);
    assertEquals(bitAt(312), in.next());
    // a bulk read after the seek consumes from the new position
    assertEquals(10, in.nextVector(isNull, null, 0, 20));
    in.seek(positions[100]);
    assertEquals(10, in.nextVector(isNull, null, 0, 20));
  }

  /**
   * Read a large batch of bits in a single call. The field is longer than the
   * buffer of the stream, and the read starts part way into a byte, so the
   * bits left over from the previous byte have to be continued.
   */
  private void runLargeBulkTest(CompressionCodec codec) throws Exception {
    final int ROWS = 1 << 20;
    BitFieldReader in = writeBits(codec, ROWS, null);
    // consume 3 bits, so that the bulk read starts in the middle of a byte
    for (int bit = 0; bit < 3; ++bit) {
      assertEquals(bitAt(bit), in.next(), "bit at " + bit);
    }
    final int start = 3;
    final int size = ROWS - start;
    boolean[] isNull = new boolean[size];
    long expected = 0;
    for (int row = start; row < ROWS; ++row) {
      if (bitAt(row) == 1) {
        expected += 1;
      }
    }
    assertEquals(expected, in.nextVector(isNull, null, 0, size),
        "nonNulls of " + size + " rows");
    for (int row = start; row < ROWS; row += 97) {
      assertEquals(bitAt(row) == 0, isNull[row - start], "row " + row);
    }
    // all of the bits have been consumed
    assertThrows(EOFException.class,
        () -> in.nextVector(new boolean[1], null, 0, 1));
  }

  @Test
  public void testBulkLargeUncompressed() throws Exception {
    runLargeBulkTest(null);
  }

  @Test
  public void testBulkLargeCompressed() throws Exception {
    runLargeBulkTest(new ZlibCodec());
  }

  /**
   * The arguments of nextVector have to be checked before any bit is read.
   */
  @Test
  public void testBulkInvalidArguments() throws Exception {
    BitFieldReader in = writeBits(null, 128, null);
    final boolean[] isNull = new boolean[10];
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(isNull, null, -1, 5));
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(isNull, null, 0, -1));
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(isNull, null, 0, 1L << 33));
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(isNull, null, 5, 6));
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(isNull, isNull, 9, 2));
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(isNull, null, Integer.MAX_VALUE - 1, 100));
    // The batch has to fit in the address space even when there are no arrays
    // to check it against, so that the end of the loop doesn't overflow.
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(null, null, Integer.MAX_VALUE, 1));
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(null, null, Integer.MAX_VALUE - 4, 10));
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(null, null, Integer.MAX_VALUE - 10, 11));
    assertThrows(IllegalArgumentException.class,
        () -> in.nextVector(isNull, isNull, Integer.MAX_VALUE, 1));
    // none of the failed calls consumed a bit of the field
    assertEquals(bitAt(0), in.next(), "bit at 0");
    // reading no rows at all is not an error
    assertEquals(0, in.nextVector(isNull, null, 0, 0));
    assertEquals(0, in.nextVector(null, null, 0, 0));
    assertEquals(bitAt(1), in.next(), "bit at 1");
  }

  /**
   * A batch that ends at the last index still fits in the address space, so
   * it reads the bits without writing them to an array.
   */
  @Test
  public void testBulkLastIndex() throws Exception {
    BitFieldReader in = writeBits(null, 128, null);
    long expected = 0;
    for (int bit = 0; bit < 9; ++bit) {
      if (bitAt(bit) == 1) {
        expected += 1;
      }
    }
    assertEquals(expected,
        in.nextVector(null, null, Integer.MAX_VALUE - 9, 9));
    // the bits of the batch have been consumed
    assertEquals(bitAt(9), in.next(), "bit at 9");
  }

  /**
   * The bytes needed to hold a number of bits are rounded up without
   * overflowing when there are close to Integer.MAX_VALUE of them.
   */
  @Test
  public void testBytesForBits() {
    assertEquals(0, BitFieldReader.bytesForBits(0));
    assertEquals(0, BitFieldReader.bytesForBits(-1));
    assertEquals(0, BitFieldReader.bytesForBits(Integer.MIN_VALUE));
    for (int bits = 1; bits < 1000; ++bits) {
      assertEquals((bits + 7) / 8, BitFieldReader.bytesForBits(bits),
          bits + " bits");
    }
    assertEquals(1, BitFieldReader.bytesForBits(1));
    assertEquals(1, BitFieldReader.bytesForBits(8));
    assertEquals(2, BitFieldReader.bytesForBits(9));
    // (bits + 7) wraps negative at these sizes
    assertEquals(268435456, BitFieldReader.bytesForBits(Integer.MAX_VALUE - 6));
    assertEquals(268435456, BitFieldReader.bytesForBits(Integer.MAX_VALUE));
  }
}
