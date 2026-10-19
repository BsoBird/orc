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

import com.google.common.math.LongMath;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestSerializationUtils {

  private InputStream fromBuffer(ByteArrayOutputStream buffer) {
    return new ByteArrayInputStream(buffer.toByteArray());
  }

  @Test
  public void testDoubles() throws Exception {
    double tolerance = 0.0000000000000001;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils utils = new SerializationUtils();
    utils.writeDouble(buffer, 1343822337.759);
    assertEquals(1343822337.759, utils.readDouble(fromBuffer(buffer)), tolerance);
    buffer = new ByteArrayOutputStream();
    utils.writeDouble(buffer, 0.8);
    double got = utils.readDouble(fromBuffer(buffer));
    assertEquals(0.8, got, tolerance);
  }

  @Test
  public void testReadDoubles() throws Exception {
    final int COUNT = 100;
    SerializationUtils utils = new SerializationUtils();
    double[] expected = new double[COUNT];
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    Random rand = new Random(42);
    for (int i = 0; i < COUNT; ++i) {
      expected[i] = (i % 7 == 0) ? Double.NaN
          : rand.nextDouble() * 2000 - 1000;
      utils.writeDouble(buffer, expected[i]);
    }
    // Read the values in batches with an offset, so that the results have to
    // land in the right places.
    double[] values = new double[COUNT + 2];
    Arrays.fill(values, 42.0);
    InputStream in = fromBuffer(buffer);
    int posn = 0;
    for (int size : new int[]{0, 1, 7, 8, 3, 72, 9}) {
      final int offset = posn % 2;
      utils.readDoubles(in, values, offset, size);
      assertArrayEquals(Arrays.copyOfRange(expected, posn, posn + size),
          Arrays.copyOfRange(values, offset, offset + size),
          "values at " + posn);
      posn += size;
    }
    assertEquals(COUNT, posn);
  }

  /**
   * An input stream that only returns a few bytes per read, so that the bulk
   * read has to handle short reads.
   */
  private static class ShortReadStream extends InputStream {
    private final byte[] bytes;
    private int posn = 0;
    private final int chunk;

    ShortReadStream(byte[] bytes, int chunk) {
      this.bytes = bytes;
      this.chunk = chunk;
    }

    @Override
    public int read() {
      return posn == bytes.length ? -1 : 0xff & bytes[posn++];
    }

    @Override
    public int read(byte[] buffer, int offset, int length) {
      if (posn == bytes.length) {
        return -1;
      }
      final int count = Math.min(Math.min(chunk, length), bytes.length - posn);
      System.arraycopy(bytes, posn, buffer, offset, count);
      posn += count;
      return count;
    }
  }

  @Test
  public void testReadDoublesShortReads() throws Exception {
    final int COUNT = 20;
    SerializationUtils utils = new SerializationUtils();
    double[] expected = new double[COUNT];
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    for (int i = 0; i < COUNT; ++i) {
      expected[i] = -12345.6789 - i;
      utils.writeDouble(buffer, expected[i]);
    }
    for (int chunk : new int[]{1, 2, 3, 7, 8, 1000}) {
      double[] values = new double[COUNT];
      utils.readDoubles(new ShortReadStream(buffer.toByteArray(), chunk),
          values, 0, COUNT);
      assertArrayEquals(expected, values, "chunk " + chunk);
    }
    // reading past the end of the data fails
    double[] values = new double[COUNT];
    InputStream in = fromBuffer(buffer);
    utils.readDoubles(in, values, 0, COUNT - 1);
    EOFException e = assertThrows(EOFException.class,
        () -> utils.readDoubles(in, values, 0, 2));
    assertTrue(e.getMessage().contains("Read past EOF"),
        "message: " + e.getMessage());
  }

  /**
   * A count of doubles that can't fit in an int number of bytes fails, rather
   * than wrapping around to a negative number of bytes.
   */
  @Test
  public void testReadDoublesOverflow() throws Exception {
    SerializationUtils utils = new SerializationUtils();
    double[] values = new double[8];
    InputStream in = fromBuffer(new ByteArrayOutputStream());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> utils.readDoubles(in, values, 0, (Integer.MAX_VALUE - 7) / 8 + 1));
    assertTrue(e.getMessage().contains("overflows an int"),
        "message: " + e.getMessage());
    // the check happens before anything is read
    assertEquals(-1, in.read());
    // reading no values at all is not an error
    utils.readDoubles(in, values, 0, 0);
    utils.readDoubles(in, values, 0, -10);
  }

  /**
   * Check that the size of the scratch buffer that the bulk reads use doesn't
   * grow past the cap, however many values are read at once.
   */
  private static int bulkReadBufferSize(SerializationUtils utils)
      throws Exception {
    Field field = SerializationUtils.class.getDeclaredField("bulkReadBuffer");
    field.setAccessible(true);
    byte[] buffer = (byte[]) field.get(utils);
    return buffer == null ? 0 : buffer.length;
  }

  /**
   * Batches that need more than the 8KB that the scratch buffer can hold are
   * read in chunks, so check that the values still line up with the data.
   */
  @Test
  public void testReadDoublesLargeBatch() throws Exception {
    // More than 8192 bytes of doubles, so that the reads are chunked.
    final int COUNT = 3000;
    SerializationUtils utils = new SerializationUtils();
    double[] expected = new double[COUNT];
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    Random rand = new Random(7);
    for (int i = 0; i < COUNT; ++i) {
      expected[i] = rand.nextDouble() * 1e12 - 5e11;
      utils.writeDouble(buffer, expected[i]);
    }
    byte[] bytes = buffer.toByteArray();
    for (int chunk : new int[]{1, 3, 1023, 1024, 1025, 8192, 1_000_000}) {
      utils = new SerializationUtils();
      double[] values = new double[COUNT];
      utils.readDoubles(new ShortReadStream(bytes, chunk), values, 0, COUNT);
      assertArrayEquals(expected, values, "chunk " + chunk);
      assertTrue(bulkReadBufferSize(utils) <= 8192,
          "the buffer grew to " + bulkReadBufferSize(utils) +
              " bytes for chunk " + chunk);
    }
    // Several batches that each need more than 8192 bytes.
    utils = new SerializationUtils();
    buffer.reset();
    for (int copy = 0; copy < 4; ++copy) {
      for (double value : expected) {
        utils.writeDouble(buffer, value);
      }
    }
    double[] values = new double[COUNT + 3];
    Arrays.fill(values, 42.0);
    InputStream in = fromBuffer(buffer);
    for (int offset : new int[]{3, 2, 1, 0}) {
      utils.readDoubles(in, values, offset, COUNT);
      assertArrayEquals(expected, Arrays.copyOfRange(values, offset,
          offset + COUNT), "offset " + offset);
    }
    assertEquals(-1, in.read());
    assertTrue(bulkReadBufferSize(utils) <= 8192,
        "the buffer grew to " + bulkReadBufferSize(utils) + " bytes");
  }

  /**
   * A chunked batch that runs off the end of the data still reads the values
   * that are there before it fails.
   */
  @Test
  public void testReadDoublesLargeBatchEOF() throws Exception {
    final int COUNT = 1500;
    SerializationUtils utils = new SerializationUtils();
    double[] expected = new double[COUNT];
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    for (int i = 0; i < COUNT; ++i) {
      expected[i] = i * 1.5;
      utils.writeDouble(buffer, expected[i]);
    }
    double[] values = new double[COUNT + 1];
    InputStream in = fromBuffer(buffer);
    assertThrows(EOFException.class,
        () -> utils.readDoubles(in, values, 0, COUNT + 1));
    assertArrayEquals(expected, Arrays.copyOf(values, COUNT));
  }

  /**
   * Values that don't fit in the array fail, rather than writing past the end
   * of it or reading the stream.
   */
  @Test
  public void testReadDoublesBounds() throws Exception {
    SerializationUtils utils = new SerializationUtils();
    double[] written = new double[10];
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    for (int i = 0; i < written.length; ++i) {
      written[i] = i;
      utils.writeDouble(buffer, written[i]);
    }
    byte[] bytes = buffer.toByteArray();
    for (int[] range : new int[][]{
        {-1, 1}, {-1, 10}, {9, 2}, {10, 1}, {11, 1}, {0, 11},
        {Integer.MAX_VALUE, 1}}) {
      InputStream in = new ShortReadStream(bytes, 3);
      double[] values = new double[10];
      final int offset = range[0];
      final int count = range[1];
      IndexOutOfBoundsException e = assertThrows(
          IndexOutOfBoundsException.class,
          () -> utils.readDoubles(in, values, offset, count),
          "offset " + offset + " count " + count);
      assertTrue(e.getMessage().contains("doesn't fit in an array of 10"),
          "message: " + e.getMessage());
      // nothing was read from the stream
      assertEquals(0, in.read());
    }
    // the last value of the array can be filled in
    double[] values = new double[10];
    InputStream last = fromBuffer(buffer);
    utils.readDoubles(last, values, 0, 9);
    utils.readDoubles(last, values, 9, 1);
    assertArrayEquals(written, values);
    // a count of zero or less reads nothing, wherever it starts
    InputStream in = new ShortReadStream(bytes, 3);
    utils.readDoubles(in, values, -1, 0);
    utils.readDoubles(in, values, 100, -10);
    assertEquals(0, in.read());
  }

  @Test
  public void testBigIntegers() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(0));
    assertArrayEquals(new byte[]{0}, buffer.toByteArray());
    assertEquals(0L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(1));
    assertArrayEquals(new byte[]{2}, buffer.toByteArray());
    assertEquals(1L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(-1));
    assertArrayEquals(new byte[]{1}, buffer.toByteArray());
    assertEquals(-1L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(50));
    assertArrayEquals(new byte[]{100}, buffer.toByteArray());
    assertEquals(50L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(-50));
    assertArrayEquals(new byte[]{99}, buffer.toByteArray());
    assertEquals(-50L,
        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
    for(int i=-8192; i < 8192; ++i) {
      buffer.reset();
      SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(i));
      assertEquals(i >= -64 && i < 64 ? 1 : 2, buffer.size(),
          "compare length for " + i);
      assertEquals(i, SerializationUtils.readBigInteger(fromBuffer(buffer)).intValue(),
          "compare result for " + i);
    }
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("123456789abcdef0",16));
    assertEquals(new BigInteger("123456789abcdef0",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("-123456789abcdef0",16));
    assertEquals(new BigInteger("-123456789abcdef0",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    StringBuilder buf = new StringBuilder();
    for(int i=0; i < 256; ++i) {
      String num = Integer.toHexString(i);
      if (num.length() == 1) {
        buf.append('0');
      }
      buf.append(num);
    }
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger(buf.toString(),16));
    assertEquals(new BigInteger(buf.toString(),16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
    buffer.reset();
    SerializationUtils.writeBigInteger(buffer,
        new BigInteger("ff000000000000000000000000000000000000000000ff",16));
    assertEquals(
        new BigInteger("ff000000000000000000000000000000000000000000ff",16),
        SerializationUtils.readBigInteger(fromBuffer(buffer)));
  }

  @Test
  public void testSubtractionOverflow() {
    // cross check results with Guava results below
    SerializationUtils utils = new SerializationUtils();
    assertFalse(utils.isSafeSubtract(22222222222L, Long.MIN_VALUE));
    assertFalse(utils.isSafeSubtract(-22222222222L, Long.MAX_VALUE));
    assertFalse(utils.isSafeSubtract(Long.MIN_VALUE, Long.MAX_VALUE));
    assertTrue(utils.isSafeSubtract(-1553103058346370095L, 6553103058346370095L));
    assertTrue(utils.isSafeSubtract(0, Long.MAX_VALUE));
    assertTrue(utils.isSafeSubtract(Long.MIN_VALUE, 0));
  }

  @Test
  public void testSubtractionOverflowGuava() {
    try {
      LongMath.checkedSubtract(22222222222L, Long.MIN_VALUE);
      fail("expected ArithmeticException for overflow");
    } catch (ArithmeticException ex) {
      assertEquals("overflow: checkedSubtract(22222222222, -9223372036854775808)", ex.getMessage());
    }

    try {
      LongMath.checkedSubtract(-22222222222L, Long.MAX_VALUE);
      fail("expected ArithmeticException for overflow");
    } catch (ArithmeticException ex) {
      assertEquals("overflow: checkedSubtract(-22222222222, 9223372036854775807)", ex.getMessage());
    }

    try {
      LongMath.checkedSubtract(Long.MIN_VALUE, Long.MAX_VALUE);
      fail("expected ArithmeticException for overflow");
    } catch (ArithmeticException ex) {
      assertEquals("overflow: checkedSubtract(-9223372036854775808, 9223372036854775807)", ex.getMessage());
    }

    assertEquals(-8106206116692740190L,
        LongMath.checkedSubtract(-1553103058346370095L, 6553103058346370095L));
    assertEquals(-Long.MAX_VALUE, LongMath.checkedSubtract(0, Long.MAX_VALUE));
    assertEquals(Long.MIN_VALUE, LongMath.checkedSubtract(Long.MIN_VALUE, 0));
  }

  @Test
  public void testRandomFloats() throws Exception {
    float tolerance = 0.0000000000000001f;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils utils = new SerializationUtils();
    Random rand = new Random();
    int n = 100_000;
    float[] expected = new float[n];
    for (int i = 0; i < n; i++) {
      float f = rand.nextFloat();
      expected[i] = f;
      utils.writeFloat(buffer, f);
    }
    InputStream newBuffer = fromBuffer(buffer);
    for (int i = 0; i < n; i++) {
      float got = utils.readFloat(newBuffer);
      assertEquals(expected[i], got, tolerance);
    }
  }

  @Test
  public void testRandomDoubles() throws Exception {
    double tolerance = 0.0000000000000001;
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    SerializationUtils utils = new SerializationUtils();
    Random rand = new Random();
    int n = 100_000;
    double[] expected = new double[n];
    for (int i = 0; i < n; i++) {
      double d = rand.nextDouble();
      expected[i] = d;
      utils.writeDouble(buffer, d);
    }
    InputStream newBuffer = fromBuffer(buffer);
    for (int i = 0; i < n; i++) {
      double got = utils.readDouble(newBuffer);
      assertEquals(expected[i], got, tolerance);
    }
  }
}
