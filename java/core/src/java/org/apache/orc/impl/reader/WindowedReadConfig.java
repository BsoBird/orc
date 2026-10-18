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

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcConf;

import java.util.Properties;

/**
 * The parsed form of the {@code orc.windowed.*} configuration.
 * <p>
 * Every field is read once, here, from {@link OrcConf}, so that the windowed
 * read has a single place its numbers come from and the code that uses them
 * never has to know which key spelled them. The two derivations that the gate
 * makes - the pessimistic link model and the bounded credit - are methods on
 * this class rather than arithmetic sprinkled through the gate, for the same
 * reason.
 * <p>
 * The class is immutable and is safe to share between readers.
 *
 * @since 1.9.0
 */
public class WindowedReadConfig {

  /** The gate's own band: speculation is charged at {@code 1 + GATE_BAND}. */
  public static final double GATE_BAND = 0.1d;

  private final boolean enabled;
  private final long windowBytes;
  private final long gapBytes;
  private final long gateLatencyNanos;
  private final double gateBytesPerNano;
  private final double gateSafety;
  private final double gateMarginNanos;
  private final double gateMinSaved;
  private final long gateMaxExtraBytes;

  /**
   * Read the configuration out of a Hadoop {@link Configuration}.
   * @param conf the configuration, which may be {@code null} for the defaults
   */
  public WindowedReadConfig(Configuration conf) {
    this(OrcConf.ORC_WINDOWED_READ_ENABLED.getBoolean(conf),
        OrcConf.ORC_WINDOWED_WINDOW_BYTES.getLong(conf),
        OrcConf.ORC_WINDOWED_GAP_BYTES.getLong(conf),
        OrcConf.ORC_WINDOWED_GATE_LATENCY_MS.getLong(conf),
        OrcConf.ORC_WINDOWED_GATE_MBPS.getDouble(conf),
        OrcConf.ORC_WINDOWED_GATE_SAFETY.getDouble(conf),
        OrcConf.ORC_WINDOWED_GATE_MARGIN_MS.getDouble(conf),
        OrcConf.ORC_WINDOWED_GATE_MIN_SAVED.getDouble(conf),
        OrcConf.ORC_WINDOWED_MAX_EXTRA_BYTES.getLong(conf));
  }

  /**
   * Read the configuration out of a Hive style {@link Properties} table, which
   * is how {@link OrcFile} carries a per file configuration.
   * @param properties the table, which may be {@code null} for the defaults
   * @param conf the fallback {@link Configuration}, which may be {@code null}
   */
  public WindowedReadConfig(Properties properties, Configuration conf) {
    this(OrcConf.ORC_WINDOWED_READ_ENABLED.getBoolean(properties, conf),
        OrcConf.ORC_WINDOWED_WINDOW_BYTES.getLong(properties, conf),
        OrcConf.ORC_WINDOWED_GAP_BYTES.getLong(properties, conf),
        OrcConf.ORC_WINDOWED_GATE_LATENCY_MS.getLong(properties, conf),
        OrcConf.ORC_WINDOWED_GATE_MBPS.getDouble(properties, conf),
        OrcConf.ORC_WINDOWED_GATE_SAFETY.getDouble(properties, conf),
        OrcConf.ORC_WINDOWED_GATE_MARGIN_MS.getDouble(properties, conf),
        OrcConf.ORC_WINDOWED_GATE_MIN_SAVED.getDouble(properties, conf),
        OrcConf.ORC_WINDOWED_MAX_EXTRA_BYTES.getLong(properties, conf));
  }

  /**
   * Build a configuration directly. This is the one constructor the others
   * funnel into, so a test can pin the gate on values that no configuration
   * would produce.
   */
  public WindowedReadConfig(boolean enabled,
                            long windowBytes,
                            long gapBytes,
                            long gateLatencyMs,
                            double gateMbps,
                            double gateSafety,
                            double gateMarginMs,
                            double gateMinSaved,
                            long gateMaxExtraBytes) {
    this.enabled = enabled;
    this.windowBytes = windowBytes;
    this.gapBytes = gapBytes;
    this.gateLatencyNanos = gateLatencyMs * 1_000_000L;
    this.gateBytesPerNano = gateMbps * 1048576d / 1e9d;
    this.gateSafety = gateSafety;
    this.gateMarginNanos = gateMarginMs * 1_000_000d;
    this.gateMinSaved = gateMinSaved;
    this.gateMaxExtraBytes = gateMaxExtraBytes;
  }

  /**
   * Is the cross-stripe windowed read enabled at all? When this is false the
   * reader must behave exactly as it did before the feature existed, which is
   * what makes the kill switch a kill switch rather than a default.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * The largest file span one window may cover, in bytes.
   */
  public long getWindowBytes() {
    return windowBytes;
  }

  /**
   * The largest gap the windowed read may coalesce across, in bytes.
   */
  public long getGapBytes() {
    return gapBytes;
  }

  /**
   * The assumed round trip latency of the link, in nanoseconds.
   */
  public long getGateLatencyNanos() {
    return gateLatencyNanos;
  }

  /**
   * The assumed bandwidth of the link, in bytes per nanosecond. This is the
   * form the gate divides by, so it is the nominal bandwidth already divided by
   * nothing; {@link #pessimisticBytesPerNano()} is the form the break-even test
   * uses.
   */
  public double getGateBytesPerNano() {
    return gateBytesPerNano;
  }

  /**
   * The pessimism factor of the link model.
   */
  public double getGateSafety() {
    return gateSafety;
  }

  /**
   * The break-even margin, in nanoseconds, that a window must beat.
   */
  public double getGateMarginNanos() {
    return gateMarginNanos;
  }

  /**
   * The smallest number of round trips a window may save and still engage.
   */
  public double getGateMinSaved() {
    return gateMinSaved;
  }

  /**
   * The absolute ceiling on a window's speculative over-read, in bytes.
   */
  public long getGateMaxExtraBytes() {
    return gateMaxExtraBytes;
  }

  /**
   * The bandwidth the break-even test charges extra bytes at: the nominal
   * bandwidth divided by the safety factor.
   * <p>
   * Dividing the credited latency and the charged bandwidth by the SAME factor
   * is the point of the safety factor. It is the pessimistic corner of a link
   * that is {@code safety} times faster and {@code safety} times narrower than
   * the model, and it is the corner that costs: moving a round trip and the
   * bytes it buys in opposite directions at the same time would make the gate
   * optimistic in both directions at once.
   */
  public double pessimisticBytesPerNano() {
    return gateBytesPerNano / gateSafety;
  }

  /**
   * The latency the break-even test credits a saved round trip at: the nominal
   * latency divided by the safety factor. See {@link #pessimisticBytesPerNano()}.
   */
  public double pessimisticLatencyNanos() {
    return gateLatencyNanos / gateSafety;
  }

  /**
   * THE ONE CREDIT FORMULA. Every place that decides whether a window may spend
   * speculative bytes calls this, and so does any audit of a decision it made:
   *
   * <pre>
   *   raw     = savedRoundTrips * L * B * (1 + band)   (the time model)
   *   honesty = 2 * honestBaselineBytes                (what is at stake)
   *   ceiling = maxExtraBytes                          (the absolute rail)
   *   credit  = min(raw, min(honesty, ceiling))
   * </pre>
   *
   * <p>The {@code raw} term is {@code L * B}, not {@code L / B}. A round trip
   * of latency {@code L} buys {@code L * B} bytes, so that is what a saved
   * round trip is worth in bytes, and the derivative has the right sign:
   * a faster link makes a saved round trip worth MORE bytes, not fewer.
   *
   * <p>{@code honesty} is what is at stake, not what is being gambled: the
   * windowed read may not fetch more ahead of the scan than twice the material
   * the scan has to traverse anyway. {@code ceiling} is the absolute rail from
   * {@code orc.windowed.max.extra.bytes}, present so that no combination of
   * the other terms can raise the over-read without limit.
   *
   * @param savedRoundTrips the round trips the window removes; may be fractional
   *                        because the baseline is a two-phase estimate
   * @param honestBaselineBytes the bytes the scan must traverse anyway
   * @return {@code {raw, honesty, credit}}; {@code credit} is the bound
   */
  public static long[] boundedCredit(WindowedReadConfig config,
                                     double savedRoundTrips,
                                     long honestBaselineBytes) {
    long raw = creditRawBytes(config, savedRoundTrips);
    long honesty = honestBaselineBytes >= Long.MAX_VALUE / 2
        ? Long.MAX_VALUE : 2 * honestBaselineBytes;
    long ceiling = Math.max(0L, config.getGateMaxExtraBytes());
    long cap = Math.min(honesty, ceiling);
    return new long[] {raw, honesty, Math.min(raw, cap)};
  }

  /**
   * The time model's own number: what {@code savedRoundTrips} is worth in bytes
   * before any cap is applied. Static in its arithmetic so that a test can pin
   * the value at a named (saved, L, B, band) triple instead of only ever
   * exercising it through a scan.
   */
  public static long creditRawBytes(WindowedReadConfig config,
                                    double savedRoundTrips) {
    return creditRawBytesFor(savedRoundTrips, config.getGateLatencyNanos(),
        config.getGateBytesPerNano(), GATE_BAND);
  }

  /**
   * {@link #creditRawBytes(WindowedReadConfig, double)} with its inputs spelled
   * out, so that the formula itself can be pinned by a test rather than only
   * ever reached through a configuration.
   *
   * @param bytesPerNano the bandwidth in bytes per nanosecond, which the
   *                     {@link #WindowedReadConfig(Configuration)} constructor
   *                     derives as {@code mbps * 1048576 / 1e9}
   */
  public static long creditRawBytesFor(double savedRoundTrips,
                                       long latencyNanos,
                                       double bytesPerNano,
                                       double band) {
    if (bytesPerNano <= 0d) {
      return Long.MAX_VALUE;
    }
    double raw = savedRoundTrips * latencyNanos * bytesPerNano * (1d + band);
    return raw >= (double) Long.MAX_VALUE ? Long.MAX_VALUE : (long) raw;
  }

  /**
   * THE BREAK-EVEN TEST, in the pessimistic envelope, in nanoseconds.
   *
   * <pre>
   *   saved * (L / safety) &gt; extraBytes / (B / safety) * (1 + band) + margin
   * </pre>
   *
   * <p>A window engages only when this is strictly positive. A window that
   * merely ties is refused, which is what makes the margin a floor rather than
   * a decoration.
   *
   * @param savedRoundTrips the round trips the window removes
   * @param bridgeCostBytes the bytes the window fetches ahead of the scan
   * @return the net time the window wins, in nanoseconds
   */
  public double netScaledNanos(double savedRoundTrips, long bridgeCostBytes) {
    double bytesPerNano = pessimisticBytesPerNano();
    double byteCost = bytesPerNano > 0d ? bridgeCostBytes / bytesPerNano : 0d;
    return savedRoundTrips * pessimisticLatencyNanos()
        - byteCost * (1d + GATE_BAND)
        - gateMarginNanos;
  }

  @Override
  public String toString() {
    return "WindowedReadConfig[enabled=" + enabled +
        ", windowBytes=" + windowBytes +
        ", gapBytes=" + gapBytes +
        ", gateLatencyNanos=" + gateLatencyNanos +
        ", gateBytesPerNano=" + gateBytesPerNano +
        ", gateSafety=" + gateSafety +
        ", gateMarginNanos=" + gateMarginNanos +
        ", gateMinSaved=" + gateMinSaved +
        ", gateMaxExtraBytes=" + gateMaxExtraBytes + "]";
  }
}
