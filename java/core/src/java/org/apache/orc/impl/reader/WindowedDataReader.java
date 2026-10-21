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
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.reader.tree.TypeReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link DataReader} decorator that reads a whole window of stripes in two
 * merged requests instead of one footer, one index and one data request per
 * stripe.
 *
 * <h2>Layout, with zero probe I/O</h2>
 * <p>A {@link StripeInformation} already describes the physical shape of a
 * stripe, so nothing has to be read to place a window:</p>
 * <pre>
 *   [ indexLength ][          dataLength           ][ footerLength ]
 *   ^offset                                                   ^offset+length
 * </pre>
 * <p>The footer therefore sits at {@code offset + indexLength + dataLength}
 * and the index at {@code [offset, offset + indexLength)} — the same
 * arithmetic {@link org.apache.orc.impl.RecordReaderUtils} already uses.</p>
 *
 * <h2>Two rounds per window</h2>
 * <ul>
 *   <li><b>Round 1</b> is a single merged request carrying the footer of
 *       <em>every</em> stripe in the window plus, when the scan carries a
 *       SARG, every row index. The footers of the stripes that stripe
 *       statistics already pruned are included on purpose: folding them into
 *       the one request is free, whereas asking for them later would cost a
 *       round trip apiece. Those stripes' <em>data</em> is never requested,
 *       and round 1 will not bridge across it — the pruned data areas become
 *       barriers that the coalescer must not cross.</li>
 *   <li><b>Round 2</b> is a single merged request carrying the selected data
 *       ranges of the window, minus whatever round 1 already brought in.</li>
 * </ul>
 *
 * <h2>Serving without a new interface</h2>
 * <p>{@link #readFileData} fills the chunks it can from the resident window
 * buffers and hands the rest to the delegate. The stock reader paths already
 * skip chunks that carry data, so pre-filling a chunk is enough to satisfy it
 * for free. Anything genuinely missing is counted as a miss and forwarded, so
 * a miss costs bytes but never correctness. Tests assert the miss count is
 * zero.</p>
 *
 * <h2>Admission</h2>
 * <p>A window is only engaged when the gate in {@link WindowedReadConfig}
 * agrees that the round trips saved outweigh the bytes speculated; see
 * {@link #gate}. Every other window — and every scan at all when
 * {@code orc.windowed.read.enabled} is false — passes straight through, with
 * no extra request and no extra buffer.</p>
 */
public class WindowedDataReader implements DataReader {

  /**
   * The reader that does the actual IO. It is cloned together with this
   * reader, so that the two never share a file handle.
   */
  private DataReader delegate;
  private final WindowedReadConfig config;
  private final WindowedScanContext context;

  /**
   * The planner that mints the scratch planners used for in-memory SARG
   * evaluation. Set by {@link WindowedStripePlanner} once the factory has
   * built it.
   */
  private WindowedStripePlanner templatePlanner;

  // ---- lazily built plan (pure arithmetic over StripeInformation) ----
  private boolean planned;
  private List<Window> windows = new ArrayList<>();
  /** Stripe id to window. The id is file-wide; the stripe list may be filtered. */
  private Map<Long, Integer> windowOfStripe = new HashMap<>();

  /** The window whose bytes are currently resident, or {@code null}. */
  private Window resident;

  // ---- counters, for tests and diagnostics ----
  private long windowsEngaged;
  private long windowsStoodDown;
  private long roundOneRequests;
  private long roundTwoRequests;
  private long roundOneRanges;
  private long roundTwoRanges;
  private long servedChunks;
  private long servedBytes;
  private long missedChunks;
  private long missedBytes;
  private long speculatedBytes;
  private double savedRoundTrips;
  private String lastGateReason = "not-planned";

  public WindowedDataReader(DataReader delegate,
                            WindowedReadConfig config,
                            WindowedScanContext context) {
    this.delegate = delegate;
    this.config = config;
    this.context = context;
  }

  /** Called by {@link WindowedStripePlanner} once it exists. */
  void setTemplatePlanner(WindowedStripePlanner planner) {
    this.templatePlanner = planner;
  }

  public DataReader getDelegate() {
    return delegate;
  }

  public WindowedReadConfig getConfig() {
    return config;
  }

  // ------------------------------------------------------------------
  // counters
  // ------------------------------------------------------------------

  /** The number of windows the gate let through. */
  public long getWindowsEngaged() {
    return windowsEngaged;
  }

  /** The number of windows the gate refused. */
  public long getWindowsStoodDown() {
    return windowsStoodDown;
  }

  /** Merged round-1 requests issued, one per engaged window. */
  public long getRoundOneRequests() {
    return roundOneRequests;
  }

  /** Merged round-2 requests issued, at most one per engaged window. */
  public long getRoundTwoRequests() {
    return roundTwoRequests;
  }

  /** Requests this decorator added on top of the delegate's own. */
  public long getWindowedRequests() {
    return roundOneRequests + roundTwoRequests;
  }

  /** How many coalesced ranges round 1 was built from. */
  public long getRoundOneRanges() {
    return roundOneRanges;
  }

  /** How many coalesced ranges round 2 was built from. */
  public long getRoundTwoRanges() {
    return roundTwoRanges;
  }

  /** Chunks served from resident window buffers instead of the disk. */
  public long getServedChunks() {
    return servedChunks;
  }

  public long getServedBytes() {
    return servedBytes;
  }

  /**
   * Chunks the scan asked for inside an engaged window that round 1 and
   * round 2 had not covered. Each one was forwarded to the delegate, so the
   * answer stayed correct — but a non-zero count means the window arm
   * promised bytes it did not deliver. Must be zero.
   */
  public long getMissedChunks() {
    return missedChunks;
  }

  public long getMissedBytes() {
    return missedBytes;
  }

  /**
   * The bytes the engaged windows speculated on top of what the scan asked
   * for, summed over those windows.
   */
  public long getSpeculatedBytes() {
    return speculatedBytes;
  }

  /** The round trips the engaged windows saved, summed over those windows. */
  public double getSavedRoundTrips() {
    return savedRoundTrips;
  }

  /** Why the gate last said no, for diagnostics. */
  public String getLastGateReason() {
    return lastGateReason;
  }

  /** Put the counters back to zero, the way a fresh reader has them. */
  private void resetCounters() {
    windowsEngaged = 0;
    windowsStoodDown = 0;
    roundOneRequests = 0;
    roundTwoRequests = 0;
    roundOneRanges = 0;
    roundTwoRanges = 0;
    servedChunks = 0;
    servedBytes = 0;
    missedChunks = 0;
    missedBytes = 0;
    speculatedBytes = 0;
    savedRoundTrips = 0;
    lastGateReason = "not-planned";
  }

  // ------------------------------------------------------------------
  // DataReader
  // ------------------------------------------------------------------

  /**
   * Get the stripe footer, materialising the stripe's window on the way.
   *
   * <p>This is the arm's trigger: it runs exactly once per stripe the scan
   * actually enters, and it runs before ORC has asked the delegate for any
   * part of the stripe.</p>
   */
  @Override
  public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
    if (!config.isEnabled()) {
      return delegate.readStripeFooter(stripe);
    }
    ensureWindow(stripe.getStripeId());
    if (resident == null) {
      return delegate.readStripeFooter(stripe);
    }
    ByteBuffer footer = residentSlice(footerOffset(stripe), stripe.getFooterLength());
    if (footer == null) {
      return delegate.readStripeFooter(stripe);
    }
    return RecordReaderUtils.parseStripeFooter(footer, delegate.getCompressionOptions());
  }

  /**
   * Fill the chunks the resident window already covers and forward the rest.
   *
   * <p>The chunks that are served are left in the caller's list, filled, and
   * the chunks that are not are handed to the delegate as a fresh list of
   * fresh {@link BufferChunk}s whose contents are copied back afterwards —
   * re-linking the caller's chunks into a second list would corrupt it.</p>
   */
  @Override
  public BufferChunkList readFileData(BufferChunkList range, boolean doForceDirect)
      throws IOException {
    if (resident == null) {
      return delegate.readFileData(range, doForceDirect);
    }
    List<BufferChunk> pendingTargets = new ArrayList<>();
    BufferChunkList pending = null;
    for (BufferChunk chunk = range == null ? null : range.get();
         chunk != null; ) {
      BufferChunk next = (BufferChunk) chunk.next;
      if (!chunk.hasData()) {
        ByteBuffer served = residentSlice(chunk.getOffset(), chunk.getLength());
        if (served != null) {
          chunk.setChunk(served);
          servedChunks++;
          servedBytes += chunk.getLength();
        } else {
          missedChunks++;
          missedBytes += chunk.getLength();
          if (pending == null) {
            pending = new BufferChunkList();
          }
          pendingTargets.add(chunk);
          pending.add(new BufferChunk(chunk.getOffset(), chunk.getLength()));
        }
      }
      chunk = next;
    }
    if (pending != null) {
      BufferChunkList filled = delegate.readFileData(pending, doForceDirect);
      BufferChunk source = filled == null ? null : filled.get();
      for (int i = 0; i < pendingTargets.size(); i++) {
        if (source != null && source.hasData()) {
          pendingTargets.get(i).setChunk(source.getData());
        }
        source = source == null ? null : (BufferChunk) source.next;
      }
    }
    return range;
  }

  @Override
  public boolean isTrackingDiskRanges() {
    return delegate.isTrackingDiskRanges();
  }

  @Override
  @Deprecated
  public void releaseBuffer(ByteBuffer buffer) {
    delegate.releaseBuffer(buffer);
  }

  @Override
  public void releaseAllBuffers() {
    delegate.releaseAllBuffers();
  }

  /**
   * Clone the reader.
   *
   * <p>The copy owns a clone of the delegate, so that the two never share a
   * file handle, and it starts with no plan of its own: the plan is pure
   * arithmetic over the stripes, so the copy builds its own the first time it
   * is asked for a window. It deliberately does not inherit the template
   * planner, whose scratch planners read their index bytes through the reader
   * that owns them. The counters start at zero, because they measure the
   * copy's own work.</p>
   */
  @Override
  public DataReader clone() {
    try {
      WindowedDataReader copy = (WindowedDataReader) super.clone();
      copy.delegate = delegate.clone();
      copy.templatePlanner = null;
      copy.windows = new ArrayList<>();
      copy.windowOfStripe = new HashMap<>();
      copy.planned = false;
      copy.resident = null;
      copy.resetCounters();
      return copy;
    } catch (CloneNotSupportedException e) {
      throw new UnsupportedOperationException("uncloneable", e);
    }
  }

  @Override
  public InStream.StreamOptions getCompressionOptions() {
    return delegate.getCompressionOptions();
  }

  @Override
  public void open() throws IOException {
    delegate.open();
  }

  @Override
  public void close() throws IOException {
    resident = null;
    delegate.close();
  }

  // ------------------------------------------------------------------
  // planning
  // ------------------------------------------------------------------

  static long indexOffset(StripeInformation stripe) {
    return stripe.getOffset();
  }

  static long dataOffset(StripeInformation stripe) {
    return stripe.getOffset() + stripe.getIndexLength();
  }

  static long footerOffset(StripeInformation stripe) {
    return stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
  }

  /**
   * Make the window holding {@code stripeId} the resident one, reading it if
   * the gate allows. A window the gate refused, or a stripe outside the
   * scan's range, simply leaves nothing resident.
   */
  void ensureWindow(long stripeId) throws IOException {
    if (!config.isEnabled()) {
      return;
    }
    if (!planned) {
      buildPlan();
    }
    Integer index = windowOfStripe.get(stripeId);
    Window window = index == null ? null : windows.get(index);
    if (window == null) {
      resident = null;
      return;
    }
    if (window == resident) {
      return;
    }
    resident = null;
    if (!window.engaged) {
      windowsStoodDown++;
      lastGateReason = window.gateReason;
      return;
    }
    materialise(window);
  }

  /**
   * Group the stripes into windows and price every one of them. Pure
   * arithmetic over {@link StripeInformation}: this reads nothing.
   */
  private void buildPlan() {
    List<? extends StripeInformation> stripes = context.stripes();
    long windowBytes = config.getWindowBytes();
    Window current = null;
    for (int i = 0; i < stripes.size(); i++) {
      StripeInformation stripe = stripes.get(i);
      long start = stripe.getOffset();
      long end = start + stripe.getLength();
      if (current == null || end - current.start > windowBytes) {
        current = new Window();
        windows.add(current);
        current.start = start;
      }
      windowOfStripe.putIfAbsent(stripe.getStripeId(), windows.size() - 1);
      current.end = Math.max(current.end, end);
      current.stripes.add(stripe);
      current.included.add(context.isStripeIncluded(i));
    }
    for (Window window : windows) {
      gate(window);
    }
    planned = true;
  }

  /**
   * Price a window and decide whether to engage.
   *
   * <p>The arm engages only when both halves of the gate agree:</p>
   * <ol>
   *   <li>the round trips saved, discounted by the safety factor, outweigh
   *       the bytes speculated — the classic break-even test, at the
   *       pessimistic end of the latency and bandwidth envelope; and</li>
   *   <li>the speculated bytes fit inside the credit those saved round
   *       trips earn, itself capped at twice the honest baseline and at the
   *       configured ceiling.</li>
   * </ol>
   */
  private void gate(Window window) {
    window.engaged = false;
    long latencyNanos = config.getGateLatencyNanos();
    if (latencyNanos <= 0) {
      // No round trip to save: every byte the arm bridges over is pure cost.
      window.gateReason = "gate-latency-zero";
      return;
    }
    boolean hasSarg = context.hasSarg();
    int n = window.stripes.size();
    if (n < 2) {
      window.gateReason = "window-too-small";
      return;
    }

    long[][] barriers = barriers(window);
    long[][] meta = metaRanges(window);
    long wantedMeta = 0;
    for (long[] range : meta) {
      wantedMeta += range[1] - range[0];
    }
    long[][] metaPlan = coalesce(meta, config.getGapBytes(), barriers);

    long[][] data = dataRanges(window);
    long wantedData = 0;
    for (long[] range : data) {
      wantedData += range[1] - range[0];
    }
    long[][] dataPlan = coalesce(data, config.getGapBytes(), barriers);

    long extraBytes = (bytes(metaPlan) + bytes(dataPlan)) - (wantedMeta + wantedData);
    // Without a SARG the window reads its data either way — only earlier — so
    // those bytes are not speculation. The time saving is still credited.
    long speculative = hasSarg ? extraBytes : 0L;

    // The bytes a pass-through scan reads out of this window's range.
    long honestBaselineBytes = window.honestBaselineBytes();

    // The requests a pass-through scan makes for this window. With a SARG a
    // stripe ruled out by the statistics is dropped before its bytes are
    // asked for, so charging for those requests would credit the arm with
    // work the scan never does. Only the metadata phase is countable, and
    // under-crediting is the safe direction for an admission test.
    double metaBaseline = n * (hasSarg ? 2 : 1);
    double dataBaseline = hasSarg ? 0 : n;
    double saved = metaBaseline + dataBaseline - metaPlan.length - dataPlan.length;
    if (saved < config.getGateMinSaved()) {
      window.gateReason = "min-saved";
      return;
    }
    if (config.netScaledNanos(saved, speculative) <= 0d) {
      window.gateReason = "break-even";
      return;
    }
    long credit = WindowedReadConfig.boundedCredit(config, saved, honestBaselineBytes)[2];
    if (speculative > credit) {
      window.gateReason = speculative > config.getGateMaxExtraBytes() ?
          "max-extra-bytes" : "credit";
      return;
    }

    // Keep the window resident inside the envelope the credit describes: the
    // bytes the arm holds must stay under what a full scan of the window
    // would have streamed past.
    long residentBytes = bytes(metaPlan) + bytes(dataPlan);
    if (residentBytes > 2 * honestBaselineBytes) {
      window.gateReason = "resident-bytes";
      return;
    }

    window.engaged = true;
    window.gateReason = "engage";
    window.plannedMeta = metaPlan;
    window.plannedData = dataPlan;
    speculatedBytes += extraBytes;
    savedRoundTrips += saved;
    windowsEngaged++;
  }

  /** Every footer in the window, plus every index when there is a SARG. */
  private long[][] metaRanges(Window window) {
    boolean hasSarg = context.hasSarg();
    List<long[]> meta = new ArrayList<>();
    for (StripeInformation stripe : window.stripes) {
      if (hasSarg && stripe.getIndexLength() > 0) {
        meta.add(new long[]{indexOffset(stripe),
            indexOffset(stripe) + stripe.getIndexLength()});
      }
      if (stripe.getFooterLength() > 0) {
        meta.add(new long[]{footerOffset(stripe), footerOffset(stripe)
            + stripe.getFooterLength()});
      }
    }
    meta.sort(Comparator.comparingLong(a -> a[0]));
    return meta.toArray(new long[0][]);
  }

  /** The data area of every stripe the scan will actually read. */
  private long[][] dataRanges(Window window) {
    List<long[]> data = new ArrayList<>();
    for (int i = 0; i < window.stripes.size(); i++) {
      if (!window.included.get(i)) {
        continue;
      }
      StripeInformation stripe = window.stripes.get(i);
      if (stripe.getDataLength() > 0) {
        data.add(new long[]{dataOffset(stripe), dataOffset(stripe)
            + stripe.getDataLength()});
      }
    }
    data.sort(Comparator.comparingLong(a -> a[0]));
    return data.toArray(new long[0][]);
  }

  /**
   * The data areas the coalescer may not bridge across: those of the stripes
   * that stripe-statistics pruning already removed. Bridging one would spend
   * the very bytes the pruning just saved.
   */
  private long[][] barriers(Window window) {
    if (!context.hasSarg()) {
      return null;
    }
    List<long[]> barriers = new ArrayList<>();
    for (int i = 0; i < window.stripes.size(); i++) {
      if (window.included.get(i)) {
        continue;
      }
      StripeInformation stripe = window.stripes.get(i);
      if (stripe.getDataLength() > 0) {
        barriers.add(new long[]{dataOffset(stripe), dataOffset(stripe)
            + stripe.getDataLength()});
      }
    }
    if (barriers.isEmpty()) {
      return null;
    }
    barriers.sort(Comparator.comparingLong(a -> a[0]));
    return barriers.toArray(new long[0][]);
  }

  /**
   * Merge sorted ranges, bridging gaps of at most {@code gapBytes} that
   * contain no barrier. Returns the merged ranges plus their total length in
   * the last element.
   */
  static long[][] coalesce(long[][] ranges, long gapBytes, long[][] barriers) {
    List<long[]> merged = new ArrayList<>();
    for (long[] range : ranges) {
      if (merged.isEmpty()) {
        merged.add(range);
        continue;
      }
      long[] last = merged.get(merged.size() - 1);
      long bridgeStart = last[1];
      long bridgeEnd = range[0];
      if (bridgeEnd - bridgeStart <= gapBytes && !isBarred(barriers, bridgeStart, bridgeEnd)) {
        last[1] = range[1];
      } else {
        merged.add(range);
      }
    }
    return merged.toArray(new long[0][]);
  }

  private static boolean isBarred(long[][] barriers, long start, long end) {
    if (barriers == null) {
      return false;
    }
    for (long[] barrier : barriers) {
      if (barrier[1] > start && barrier[0] < end) {
        return true;
      }
    }
    return false;
  }

  private static long bytes(long[][] ranges) {
    long total = 0;
    for (long[] range : ranges) {
      total += range[1] - range[0];
    }
    return total;
  }

  // ------------------------------------------------------------------
  // materialisation
  // ------------------------------------------------------------------

  /**
   * Run the two rounds for a window and leave its bytes resident.
   *
   * <p>Round 1 must land before the SARG can be evaluated, because the SARG
   * reads the row indexes that round 1 brought in. The per-stripe planning
   * in between therefore costs no extra request, and round 2 is issued once,
   * merged, over whatever survived.</p>
   */
  private void materialise(Window window) throws IOException {
    resident = window;
    List<Span> spans = new ArrayList<>();
    long[][] barriers = barriers(window);

    // ---- round 1: footers and indexes, one merged request ----
    long[][] metaPlan = window.plannedMeta;
    BufferChunkList roundOne = toChunks(metaPlan);
    roundOneRanges += metaPlan.length;
    if (roundOne.get() != null) {
      roundOneRequests++;
      collect(delegate.readFileData(roundOne, false), spans);
    }
    // The scratch planners read the row indexes out of these buffers, so the
    // window has to look resident before the SARG runs.
    window.spans = spans;

    // ---- in-memory SARG: what data does the window still need? ----
    long[][] selected;
    if (context.hasSarg()) {
      selected = sargRanges(window, spans);
    } else {
      selected = window.plannedData;
    }

    // ---- round 2: the selected data, minus what round 1 already read ----
    long[][] wanted = subtract(selected, spans);
    long[][] dataPlan = coalesce(wanted, config.getGapBytes(), barriers);
    BufferChunkList roundTwo = toChunks(dataPlan);
    roundTwoRanges += dataPlan.length;
    if (roundTwo.get() != null) {
      roundTwoRequests++;
      collect(delegate.readFileData(roundTwo, false), spans);
    }

    // The two rounds cover disjoint byte intervals, but round two lands after
    // round one in the list. Everything that walks the spans -- subtracting
    // them, and slicing out of them -- needs them in file order.
    spans.sort(Comparator.comparingLong(span -> span.start));
    window.spans = spans;
  }

  /**
   * Evaluate the SARG for every stripe the scan will read, entirely out of
   * the resident round-1 buffers, and return the data ranges it selected.
   *
   * <p>A scratch planner is used per stripe: it is a plain
   * {@link StripePlanner} holding exactly the base state, so its byte plan
   * is the one the real reader would produce for that stripe.</p>
   */
  private long[][] sargRanges(Window window, List<Span> spans) throws IOException {
    List<long[]> selected = new ArrayList<>();
    StripePlanner scratch = templatePlanner == null ? null : templatePlanner.newScratchPlanner();
    if (scratch == null) {
      return window.plannedData;
    }
    List<StripeInformation> stripes = window.stripes;
    boolean[] rowIndexColsToRead = context.rowIndexColsToRead();
    for (int i = 0; i < stripes.size(); i++) {
      if (!window.included.get(i)) {
        continue;
      }
      StripeInformation stripe = stripes.get(i);
      long footerStart = footerOffset(stripe);
      ByteBuffer footerBuf = residentSlice(spans, footerStart, stripe.getFooterLength());
      if (footerBuf == null) {
        // Cannot evaluate in memory; ask for the whole stripe's data later.
        selected.add(new long[]{dataOffset(stripe),
            dataOffset(stripe) + stripe.getDataLength()});
        continue;
      }
      OrcProto.StripeFooter footer =
          RecordReaderUtils.parseStripeFooter(footerBuf, delegate.getCompressionOptions());
      scratch.parseStripe(stripe, context.fileIncluded(), footer);
      int columns = context.columns();
      OrcIndex index = new OrcIndex(new OrcProto.RowIndex[columns],
          new OrcProto.Stream.Kind[columns],
          new OrcProto.BloomFilterIndex[columns]);
      if (rowIndexColsToRead != null) {
        scratch.readRowIndex(rowIndexColsToRead, index);
      }
      RecordReaderImpl.SargApplier applier = context.sargApplier();
      boolean[] rowGroups = null;
      if (applier != null) {
        rowGroups = applier.pickRowGroups(stripe,
            index.getRowGroupIndex(),
            context.skipBloomFilters() ? null : index.getBloomFilterKinds(),
            footer.getColumnsList(),
            context.skipBloomFilters() ? null : index.getBloomFilterIndex(),
            false);
      }
      BufferChunkList chunks = rowGroups == null ?
          scratch.planDataReading(TypeReader.ReadPhase.ALL) :
          scratch.planPartialDataReading(index, rowGroups, TypeReader.ReadPhase.ALL);
      for (BufferChunk chunk = chunks.get();
           chunk != null; chunk = (BufferChunk) chunk.next) {
        selected.add(new long[]{chunk.getOffset(),
            chunk.getOffset() + chunk.getLength()});
      }
    }
    selected.sort(Comparator.comparingLong(a -> a[0]));
    return selected.toArray(new long[0][]);
  }

  private static final int MAX_CHUNK = 1 << 30;

  private static BufferChunkList toChunks(long[][] ranges) {
    BufferChunkList list = new BufferChunkList();
    for (long[] range : ranges) {
      long start = range[0];
      while (start < range[1]) {
        // A BufferChunk holds an int length, so a window wider than that is
        // asked for in pieces.
        int length = (int) Math.min((long) MAX_CHUNK, range[1] - start);
        list.add(new BufferChunk(start, length));
        start += length;
      }
    }
    return list;
  }

  /** Remember every chunk the delegate actually read. */
  private static void collect(BufferChunkList list, List<Span> spans) {
    for (BufferChunk chunk = list == null ? null : list.get();
         chunk != null; chunk = (BufferChunk) chunk.next) {
      if (chunk.hasData() && chunk.getLength() > 0) {
        spans.add(new Span(chunk.getOffset(),
            chunk.getOffset() + chunk.getLength(), chunk.getData()));
      }
    }
  }

  /** Drop the parts of {@code wanted} that the resident spans already hold. */
  private static long[][] subtract(long[][] wanted, List<Span> spans) {
    List<long[]> out = new ArrayList<>();
    for (long[] range : wanted) {
      long start = range[0];
      long end = range[1];
      for (Span span : spans) {
        if (span.start >= end) {
          break;
        }
        if (span.end <= start) {
          continue;
        }
        if (span.start > start) {
          out.add(new long[]{start, Math.min(span.start, end)});
        }
        start = Math.max(start, span.end);
        if (start >= end) {
          break;
        }
      }
      if (start < end) {
        out.add(new long[]{start, end});
      }
    }
    return out.toArray(new long[0][]);
  }

  private ByteBuffer residentSlice(long offset, long length) {
    return residentSlice(resident == null ? null : resident.spans, offset, length);
  }

  /**
   * Find {@code [offset, offset+length)} in the resident spans and hand back
   * a view of exactly those bytes. This is the chunk-identity trick: the
   * returned buffer is what {@link #readFileData} stores in the chunk, and
   * the stock reader paths then ignore that chunk.
   */
  private static ByteBuffer residentSlice(List<Span> spans, long offset, long length) {
    if (spans == null || length <= 0) {
      return null;
    }
    long end = offset + length;
    for (Span span : spans) {
      if (span.start >= end) {
        break;
      }
      if (span.end <= offset) {
        continue;
      }
      if (span.start <= offset && span.end >= end) {
        ByteBuffer view = span.data.duplicate();
        view.position((int) (offset - span.start));
        view.limit((int) (offset - span.start) + (int) length);
        return view.slice();
      }
    }
    return null;
  }

  // ------------------------------------------------------------------
  // value types
  // ------------------------------------------------------------------

  /** A resident byte interval. */
  private static final class Span {
    final long start;
    final long end;
    final ByteBuffer data;

    Span(long start, long end, ByteBuffer data) {
      this.start = start;
      this.end = end;
      this.data = data;
    }
  }

  /** One window of stripes, plus the gate's verdict on it. */
  private static final class Window {
    final List<StripeInformation> stripes = new ArrayList<>();
    final List<Boolean> included = new ArrayList<>();
    long start;
    long end;
    boolean engaged;
    String gateReason = "not-priced";
    long[][] plannedMeta;
    long[][] plannedData;
    List<Span> spans;

    /**
     * What a pass-through scan reads out of this window: the whole of every
     * stripe it still reads, plus the footer of every stripe the statistics
     * pruned, since round 1 fetches those too.
     */
    long honestBaselineBytes() {
      long total = 0;
      for (int i = 0; i < stripes.size(); i++) {
        StripeInformation stripe = stripes.get(i);
        if (included.get(i)) {
          total += stripe.getIndexLength() + stripe.getDataLength() + stripe.getFooterLength();
        } else {
          total += stripe.getFooterLength();
        }
      }
      return total;
    }
  }
}
