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

package org.apache.orc.impl.reader;

import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * Test that a cloned {@link WindowedDataReader} is independent of the reader
 * it was cloned from: its own delegate, its own plan, its own counters.
 */
public class TestWindowedDataReaderClone {

  @Test
  public void testTheCloneStartsFresh() throws Exception {
    WindowedDataReader reader = new WindowedDataReader(new StubDataReader(),
        config(), context());
    StripeInformation stripe = new StubStripe(0);

    // Give the original some state: the gate refuses the window, so the
    // reader records a window that stood down.
    reader.readStripeFooter(stripe);
    assertEquals(1, reader.getWindowsStoodDown());
    assertEquals("min-saved", reader.getLastGateReason());

    WindowedDataReader copy = (WindowedDataReader) reader.clone();
    assertNotSame(reader, copy);
    // The two never share a file handle.
    assertNotSame(reader.getDelegate(), copy.getDelegate());
    // The copy starts with no plan and no counters of its own.
    assertEquals(0, copy.getWindowsEngaged());
    assertEquals(0, copy.getWindowsStoodDown());
    assertEquals(0, copy.getRoundOneRequests());
    assertEquals(0, copy.getRoundTwoRequests());
    assertEquals(0, copy.getRoundOneRanges());
    assertEquals(0, copy.getRoundTwoRanges());
    assertEquals(0, copy.getServedChunks());
    assertEquals(0, copy.getServedBytes());
    assertEquals(0, copy.getMissedChunks());
    assertEquals(0, copy.getMissedBytes());
    assertEquals(0, copy.getSpeculatedBytes());
    assertEquals(0d, copy.getSavedRoundTrips());
    assertEquals("not-planned", copy.getLastGateReason());

    // The copy prices its own window: the original's counters stay put.
    copy.readStripeFooter(stripe);
    assertEquals(1, copy.getWindowsStoodDown());
    assertEquals("min-saved", copy.getLastGateReason());
    assertEquals(1, reader.getWindowsStoodDown());
  }

  /**
   * A gate that refuses every window, so that a reader never reads a byte
   * and the test stays about the clone.
   */
  private static WindowedReadConfig config() {
    return new WindowedReadConfig(true,
        1L << 20,
        1L << 16,
        50,
        100d,
        3d,
        0d,
        1e9d,
        32L << 20);
  }

  private static WindowedScanContext context() {
    List<StripeInformation> stripes = Arrays.asList(new StubStripe(0),
        new StubStripe(1));
    return new WindowedScanContext(stripes,
        new boolean[]{true, true},
        null,
        new boolean[]{true},
        null,
        TypeReader.ReadPhase.ALL,
        false,
        1);
  }

  /** A reader that does nothing: the windowed reader only needs it there. */
  private static class StubDataReader implements DataReader {
    @Override
    public void open() throws IOException {
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe)
        throws IOException {
      return null;
    }

    @Override
    public BufferChunkList readFileData(BufferChunkList range,
        boolean doForceDirect) throws IOException {
      return range;
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return false;
    }

    @Deprecated
    @Override
    public void releaseBuffer(ByteBuffer buffer) {
    }

    @Override
    public void releaseAllBuffers() {
    }

    @Override
    public DataReader clone() {
      return new StubDataReader();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public InStream.StreamOptions getCompressionOptions() {
      return new InStream.StreamOptions();
    }
  }

  /** A stripe of no bytes: the gate prices it, the reader never reads it. */
  private static class StubStripe implements StripeInformation {
    private final long stripeId;

    StubStripe(long stripeId) {
      this.stripeId = stripeId;
    }

    @Override
    public long getOffset() {
      return stripeId * 1024;
    }

    @Override
    public long getLength() {
      return 1024;
    }

    @Override
    public long getIndexLength() {
      return 0;
    }

    @Override
    public long getDataLength() {
      return 0;
    }

    @Override
    public long getFooterLength() {
      return 0;
    }

    @Override
    public long getNumberOfRows() {
      return 1;
    }

    @Override
    public long getStripeId() {
      return stripeId;
    }

    @Override
    public boolean hasEncryptionStripeId() {
      return false;
    }

    @Override
    public long getEncryptionStripeId() {
      return 0;
    }

    @Override
    public byte[][] getEncryptedLocalKeys() {
      return null;
    }
  }
}
