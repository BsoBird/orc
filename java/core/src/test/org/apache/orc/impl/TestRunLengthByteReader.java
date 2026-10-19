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
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestRunLengthByteReader {

  @Test
  public void testUncompressedSeek() throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test",
        new StreamOptions(100), collect));
    TestInStream.PositionCollector[] positions =
        new TestInStream.PositionCollector[2048];
    for(int i=0; i < 2048; ++i) {
      positions[i] = new TestInStream.PositionCollector();
      out.getPosition(positions[i]);
      if (i < 1024) {
        out.write((byte) (i/4));
      } else {
        out.write((byte) i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    RunLengthByteReader in = new RunLengthByteReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining()));
    for(int i=0; i < 2048; ++i) {
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/4) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
    }
    for(int i=2047; i >= 0; --i) {
      in.seek(positions[i]);
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/4) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
    }
  }

  @Test
  public void testCompressedSeek() throws Exception {
    CompressionCodec codec = new SnappyCodec();
    StreamOptions options = new StreamOptions(500)
                                .withCodec(codec, codec.getDefaultOptions());
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    RunLengthByteWriter out = new RunLengthByteWriter(
        new OutStream("test", options, collect));
    TestInStream.PositionCollector[] positions =
        new TestInStream.PositionCollector[2048];
    for(int i=0; i < 2048; ++i) {
      positions[i] = new TestInStream.PositionCollector();
      out.getPosition(positions[i]);
      if (i < 1024) {
        out.write((byte) (i/4));
      } else {
        out.write((byte) i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    RunLengthByteReader in = new RunLengthByteReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(500)));
    for(int i=0; i < 2048; ++i) {
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/4) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
    }
    for(int i=2047; i >= 0; --i) {
      in.seek(positions[i]);
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/4) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
    }
  }

  @Test
  public void testSkips() throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test",
        new StreamOptions(100), collect));
    for(int i=0; i < 2048; ++i) {
      if (i < 1024) {
        out.write((byte) (i/16));
      } else {
        out.write((byte) i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    RunLengthByteReader in = new RunLengthByteReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining()));
    for(int i=0; i < 2048; i += 10) {
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/16) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
      if (i < 2038) {
        in.skip(9);
      }
      in.skip(0);
    }
  }

  /**
   * Write the COUNT values used by the nextBytes tests and check that reading
   * them in bulk gives the same values as reading them one at a time.
   */
  private static byte[] nextBytesExpected(int count) {
    byte[] expected = new byte[count];
    for (int i = 0; i < count; ++i) {
      expected[i] = (byte) (i < count / 2 ? (i / 4) : i);
    }
    return expected;
  }

  private static void checkNextBytes(RunLengthByteReader in, byte[] expected)
      throws Exception {
    // Read the values with batches that span the runs, the repeat to
    // literal boundary and the buffer flushes.
    byte[] buffer = new byte[expected.length + 8];
    int posn = 0;
    for (int size : new int[]{0, 1, 7, 8, 3, 1000, 9,
        expected.length - 1028}) {
      // alternate the offset so that the results are checked in place
      final int offset = (posn % 3) * 2;
      assertEquals(size, in.nextBytes(buffer, offset, size),
          "nextBytes(" + size + ") at " + posn);
      assertArrayEquals(Arrays.copyOfRange(expected, posn, posn + size),
          Arrays.copyOfRange(buffer, offset, offset + size),
          "values at " + posn);
      posn += size;
    }
    assertEquals(expected.length, posn);
    // nothing is left, so a zero length read succeeds and a real read fails
    assertEquals(0, in.nextBytes(buffer, 0, 0));
    assertThrows(EOFException.class, () -> in.nextBytes(buffer, 0, 1));
  }

  @Test
  public void testNextBytesUncompressed() throws Exception {
    final int COUNT = 2048;
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test",
        new StreamOptions(100), collect));
    for (int i = 0; i < COUNT; ++i) {
      if (i < COUNT / 2) {
        out.write((byte) (i / 4));
      } else {
        out.write((byte) i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    checkNextBytes(new RunLengthByteReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining())),
        nextBytesExpected(COUNT));
  }

  @Test
  public void testNextBytesCompressed() throws Exception {
    final int COUNT = 2048;
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(100)
        .withCodec(codec, codec.getDefaultOptions());
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    RunLengthByteWriter out = new RunLengthByteWriter(
        new OutStream("test", options, collect));
    for (int i = 0; i < COUNT; ++i) {
      if (i < COUNT / 2) {
        out.write((byte) (i / 4));
      } else {
        out.write((byte) i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    checkNextBytes(new RunLengthByteReader(InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(100))),
        nextBytesExpected(COUNT));
  }
}
