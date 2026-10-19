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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.filter.FilterContext;

import java.io.EOFException;
import java.io.IOException;

public final class BitFieldReader {
  private static final int MAX_BULK_BYTES = 1 << 13;
  private final RunLengthByteReader input;
  private int current;
  private byte currentIdx = 8;
  /**
   * Scratch buffer for the bytes of the current batch. It is reused between
   * the batches and is fully consumed before returning, so that seeking and
   * skipping only ever see the state of the underlying reader.
   */
  private byte[] bulk = null;

  public BitFieldReader(InStream input) {
    this.input = new RunLengthByteReader(input);
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      currentIdx = 0;
    } else {
      throw new EOFException("Read past end of bit field from " + this);
    }
  }

  public int next() throws IOException {
    if (currentIdx > 7) {
      readByte();
    }

    currentIdx++;
    // Highest bit is the first val
    return ((current >>> (8 - currentIdx)) & 1);
  }

  /**
   * Read the next length bits from the bit field, marking the null values.
   *
   * <p>The bits are consumed in bulk, a byte at a time, rather than one
   * {@link #next()} call at a time. The most significant bit of a byte is the
   * first value, exactly as in {@link #next()}, and reading past the end of
   * the field still raises an {@link EOFException}.</p>
   *
   * <p>The bits are the present flags of the rows, so a 1 bit means that the
   * row has a value and a 0 bit means that the row is null.</p>
   *
   * @param isNull the array that the null flags are written into, where
   *               isNull[offset + i] is true if row i is null. If it is null,
   *               the values are only counted.
   * @param parentIsNull the null flags of the enclosing reader, if any. No
   *                     bit is consumed for the rows where it is true.
   * @param offset the first row to fill in
   * @param length the number of rows to read
   * @return the number of rows that are not null
   * @throws IllegalArgumentException if the offset or the length is negative,
   *         if the rows don't fit in the given arrays, or if the rows run past
   *         the end of the address space
   */
  public long nextVector(boolean[] isNull,
                         boolean[] parentIsNull,
                         int offset,
                         long length) throws IOException {
    final int size = checkRange(isNull, parentIsNull, offset, length);
    if (size == 0) {
      return 0;
    }
    if (parentIsNull == null) {
      // Every row has a bit of its own, so read the whole batch in one run.
      return readBits(isNull, offset, size);
    }
    long nonNulls = 0;
    int pos = offset;
    final int end = offset + size;
    while (pos < end) {
      if (parentIsNull[pos]) {
        // The parent is null, so this row doesn't have a bit of its own.
        if (isNull != null) {
          isNull[pos] = true;
        }
        pos += 1;
      } else {
        // Find the end of the run of rows that consume a bit each.
        int runEnd = pos + 1;
        while (runEnd < end && !parentIsNull[runEnd]) {
          runEnd += 1;
        }
        nonNulls += readBits(isNull, pos, runEnd - pos);
        pos = runEnd;
      }
    }
    return nonNulls;
  }

  /**
   * Check that a batch of rows can be read into the given arrays.
   *
   * @param isNull the array that the null flags are written into, if any
   * @param parentIsNull the null flags of the enclosing reader, if any
   * @param offset the first row to fill in
   * @param length the number of rows to read
   * @return the number of rows to read, where offset + length is known not to
   *         overflow an int
   */
  private static int checkRange(boolean[] isNull,
                                boolean[] parentIsNull,
                                int offset,
                                long length) {
    if (offset < 0) {
      throw new IllegalArgumentException("The offset " + offset +
          " of a bit field batch must not be negative");
    }
    if (length < 0) {
      throw new IllegalArgumentException("The length " + length +
          " of a bit field batch must not be negative");
    }
    if (length > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("The length " + length +
          " of a bit field batch is too large to read");
    }
    final int size = (int) length;
    // The rows are found using an int end marker, so the batch has to fit in
    // the address space even when there are no arrays to check it against.
    if (offset + (long) size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Reading " + size + " bit field rows" +
          " at " + offset + " runs past the end of the address space");
    }
    if (isNull != null && offset + (long) size > isNull.length) {
      throw new IllegalArgumentException("Reading " + size + " bit field rows" +
          " at " + offset + " doesn't fit in an array of " + isNull.length);
    }
    if (parentIsNull != null && offset + (long) size > parentIsNull.length) {
      throw new IllegalArgumentException("Checking " + size + " bit field rows" +
          " at " + offset + " doesn't fit in an array of " +
          parentIsNull.length);
    }
    return size;
  }

  /**
   * The number of bytes that hold the given number of bits.
   *
   * @param bits the number of bits
   * @return the number of bytes that they need, rounded up, or 0 if there are
   *         no bits to hold
   */
  static int bytesForBits(int bits) {
    if (bits <= 0) {
      return 0;
    }
    // ((bits - 1) / 8) + 1 rounds up without overflowing bits + 7, which
    // wraps negative when there are Integer.MAX_VALUE bits to hold.
    return ((bits - 1) / 8) + 1;
  }

  /**
   * Read a run of bits, setting the null flags of the rows.
   *
   * @param isNull the array that the null flags are written into, or null to
   *               only count the values
   * @param offset the first row to fill in
   * @param length the number of rows to read
   * @return the number of rows that are not null
   */
  private int readBits(boolean[] isNull, int offset, int length)
      throws IOException {
    int nonNulls = 0;
    int pos = offset;
    int remaining = length;
    int available = 8 - currentIdx;
    int bulkIdx = 0;
    int bulkSize = 0;
    while (remaining > 0) {
      if (available == 0) {
        if (bulkIdx >= bulkSize) {
          // Read the bytes that hold the remaining bits all at once, so that
          // the runs of the underlying reader are handled in bulk. The number
          // of bytes is exact, so the end of the field is found at the same
          // place as it is by next().
          bulkSize = Math.min(bytesForBits(remaining), MAX_BULK_BYTES);
          if (bulk == null || bulk.length < bulkSize) {
            bulk = new byte[bulkSize];
          }
          fillBytes(bulk, bulkSize);
          bulkIdx = 0;
        }
        current = 0xff & bulk[bulkIdx++];
        currentIdx = 0;
        available = 8;
      }
      final int consume = Math.min(available, remaining);
      final int bits =
          (current >>> (8 - currentIdx - consume)) & ((1 << consume) - 1);
      // The bits are the present flags, where a 1 bit means that the row has
      // a value and a 0 bit means that the row is null.
      nonNulls += Integer.bitCount(bits);
      if (isNull != null) {
        // The most significant of the bits is the first row.
        for (int i = 0; i < consume; ++i) {
          isNull[pos + i] = ((bits >>> (consume - 1 - i)) & 1) == 0;
        }
      }
      currentIdx += consume;
      pos += consume;
      remaining -= consume;
      available = 8 - currentIdx;
    }
    return nonNulls;
  }

  /**
   * Fill the buffer with the next bytes of the underlying reader.
   *
   * @param bytes the buffer to fill in
   * @param count the number of bytes to read
   */
  private void fillBytes(byte[] bytes, int count) throws IOException {
    try {
      input.nextBytes(bytes, 0, count);
    } catch (EOFException e) {
      final EOFException failure =
          new EOFException("Read past end of bit field from " + this);
      failure.initCause(e);
      throw failure;
    }
  }

  public void nextVector(LongColumnVector previous,
                         FilterContext filterContext,
                         long previousLen) throws IOException {
    previous.isRepeating = false;
    int previousIdx = 0;
    if (previous.noNulls) {
      for (int i = 0; i != filterContext.getSelectedSize(); i++) {
        int idx = filterContext.getSelected()[i];
        if (idx - previousIdx > 0) {
          skip(idx - previousIdx);
        }
        previous.vector[idx] = next();
        previousIdx = idx + 1;
      }
      skip(previousLen - previousIdx);
    } else {
      for (int i = 0; i != filterContext.getSelectedSize(); i++) {
        int idx = filterContext.getSelected()[i];
        if (idx - previousIdx > 0) {
          skip(TreeReaderFactory.TreeReader.countNonNullRowsInRange(
              previous.isNull, previousIdx, idx));
        }
        if (!previous.isNull[idx]) {
          previous.vector[idx] = next();
        } else {
          previous.vector[idx] = 1;
        }
        previousIdx = idx + 1;
      }
      skip(TreeReaderFactory.TreeReader.countNonNullRowsInRange(
          previous.isNull, previousIdx, (int)previousLen));
    }
  }

  public void nextVector(LongColumnVector previous,
                         long previousLen) throws IOException {
    previous.isRepeating = true;
    for (int i = 0; i < previousLen; i++) {
      if (previous.noNulls || !previous.isNull[i]) {
        previous.vector[i] = next();
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        previous.vector[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating && i > 0 && ((previous.vector[0] != previous.vector[i]) ||
          (previous.isNull[0] != previous.isNull[i]))) {
        previous.isRepeating = false;
      }
    }
  }

  public void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed > 8) {
      throw new IllegalArgumentException("Seek past end of byte at " +
          consumed + " in " + input);
    } else if (consumed != 0) {
      readByte();
      currentIdx = (byte) consumed;
    } else {
      currentIdx = 8;
    }
  }

  public void skip(long totalBits) throws IOException {
    final int availableBits = 8 - currentIdx;
    if (totalBits <= availableBits) {
      currentIdx += totalBits;
    } else {
      final long bitsToSkip = (totalBits - availableBits);
      input.skip(bitsToSkip / 8);
      // Edge case: when skipping the last bits of a bitField there is nothing more to read!
      if (input.hasNext()) {
        current = input.next();
        currentIdx = (byte) (bitsToSkip % 8);
      }
    }
  }

  @Override
  public String toString() {
    return "bit reader current: " + current
        + " current bit index: " + currentIdx
        + " from " + input;
  }
}
