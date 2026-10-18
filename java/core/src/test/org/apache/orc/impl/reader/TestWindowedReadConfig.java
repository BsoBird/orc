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
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that {@link WindowedReadConfig} reads the {@code orc.windowed.*} keys.
 *
 * @see WindowedReadConfig
 */
public class TestWindowedReadConfig {

  private static void set(Configuration conf, OrcConf key, Object value) {
    conf.set(key.getAttribute(), value.toString());
  }

  @Test
  public void testDefaults() {
    WindowedReadConfig config = new WindowedReadConfig(new Configuration());
    // The kill switch is off by default. If this ever flips, the reader's
    // request pattern changes for every user who never asked for it.
    assertFalse(config.isEnabled());
    assertEquals(8L << 20, config.getWindowBytes());
    assertEquals(1L << 20, config.getGapBytes());
    assertEquals(50_000_000L, config.getGateLatencyNanos());
    // 100 MB/s expressed in bytes per nanosecond: the form the gate divides by.
    assertEquals(100.0d * 1048576d / 1e9d, config.getGateBytesPerNano(), 1e-18d);
    assertEquals(3.0d, config.getGateSafety(), 1e-12d);
    assertEquals(0.0d, config.getGateMarginNanos(), 1e-12d);
    assertEquals(1.0d, config.getGateMinSaved(), 1e-12d);
    assertEquals(32L << 20, config.getGateMaxExtraBytes());
  }

  @Test
  public void testParseRoundTrip() {
    Configuration conf = new Configuration();
    set(conf, OrcConf.ORC_WINDOWED_READ_ENABLED, true);
    set(conf, OrcConf.ORC_WINDOWED_WINDOW_BYTES, 4096L);
    set(conf, OrcConf.ORC_WINDOWED_GAP_BYTES, 512L);
    set(conf, OrcConf.ORC_WINDOWED_GATE_LATENCY_MS, 25L);
    set(conf, OrcConf.ORC_WINDOWED_GATE_MBPS, 12.5d);
    set(conf, OrcConf.ORC_WINDOWED_GATE_SAFETY, 5.0d);
    set(conf, OrcConf.ORC_WINDOWED_GATE_MARGIN_MS, 7.5d);
    set(conf, OrcConf.ORC_WINDOWED_GATE_MIN_SAVED, 2.0d);
    set(conf, OrcConf.ORC_WINDOWED_MAX_EXTRA_BYTES, 1024L);

    WindowedReadConfig config = new WindowedReadConfig(conf);
    assertTrue(config.isEnabled());
    assertEquals(4096L, config.getWindowBytes());
    assertEquals(512L, config.getGapBytes());
    assertEquals(25_000_000L, config.getGateLatencyNanos());
    assertEquals(12.5d * 1048576d / 1e9d, config.getGateBytesPerNano(), 1e-18d);
    assertEquals(5.0d, config.getGateSafety(), 1e-12d);
    assertEquals(7_500_000d, config.getGateMarginNanos(), 1e-9d);
    assertEquals(2.0d, config.getGateMinSaved(), 1e-12d);
    assertEquals(1024L, config.getGateMaxExtraBytes());
  }

  @Test
  public void testPropertiesFallback() {
    // A value in the table wins; a value only in the Configuration is the
    // fallback; a value in neither is the OrcConf default. All three routes have
    // to land on the same parsed config, which is the round trip the Hive style
    // per file Properties table relies on.
    Configuration conf = new Configuration();
    set(conf, OrcConf.ORC_WINDOWED_WINDOW_BYTES, 2048L);
    Properties table = new Properties();
    table.setProperty(OrcConf.ORC_WINDOWED_GAP_BYTES.getAttribute(), "256");

    WindowedReadConfig config = new WindowedReadConfig(table, conf);
    assertEquals(2048L, config.getWindowBytes());
    assertEquals(256L, config.getGapBytes());
    assertFalse(config.isEnabled());

    // An empty table and a null table are both the Configuration's values.
    assertEquals(2048L, new WindowedReadConfig(new Properties(), conf).getWindowBytes());
    assertEquals(2048L, new WindowedReadConfig((Properties) null, conf).getWindowBytes());
    assertFalse(new WindowedReadConfig((Properties) null, null).isEnabled());
  }

  @Test
  public void testPessimisticEnvelope() {
    WindowedReadConfig config = new WindowedReadConfig(new Configuration());
    // BOTH sides are divided by the same safety factor. That is what makes the
    // factor the conservative corner of a link that is `safety` times faster and
    // `safety` times narrower, rather than two independent fudge factors.
    assertEquals(config.getGateBytesPerNano() / 3.0d,
        config.pessimisticBytesPerNano(), 1e-18d);
    assertEquals(config.getGateLatencyNanos() / 3.0d,
        config.pessimisticLatencyNanos(), 1e-9d);
  }

  @Test
  public void testCreditRawBytesUsesLatencyTimesBandwidth() {
    // ROUND 15 F15-1: the raw credit is saved * L * B, NOT saved * L / B. A
    // round trip of latency L buys L*B bytes, so that is what it is worth in
    // bytes, and the derivative has the right sign - a faster link makes a saved
    // round trip worth MORE bytes, not fewer. The inverted form has the sign
    // backwards, so a slower link appeared to make round trips more valuable.
    // 1000 saved round trips of 1ms each at 100MB/s. 100MB/s is 0.1048576
    // bytes per ns, so the 1000 round trips are worth 1000 * 1e6 * 0.1048576 =
    // 104857600 bytes of time model, and the 10% band makes that 115343360.
    assertEquals(115_343_360L,
        WindowedReadConfig.creditRawBytesFor(1000d, 1_000_000L,
            1048576d * 100.0d / 1e9d, WindowedReadConfig.GATE_BAND));
    // Doubling the bandwidth must double the credit.
    assertEquals(2 * 115_343_360L,
        WindowedReadConfig.creditRawBytesFor(1000d, 1_000_000L,
            1048576d * 200.0d / 1e9d, WindowedReadConfig.GATE_BAND));
    // A link that is infinitely fast cannot price bytes, so it cannot cap them:
    // the time model's raw value is left uncapped and only the honesty term and
    // the absolute rail can bind.
    assertEquals(Long.MAX_VALUE,
        WindowedReadConfig.creditRawBytesFor(1d, 1_000_000L, 0d,
            WindowedReadConfig.GATE_BAND));
  }

  @Test
  public void testBoundedCredit() {
    // A generous config so the honesty term and the raw term can be seen apart.
    // Latency 1ms, bandwidth 1e6 MB/s, so 1048.576 bytes per ns: one saved
    // round trip is worth 1e6 * 1048.576 = 1048576000 bytes, and the band
    // makes that 1153433600.
    WindowedReadConfig config = new WindowedReadConfig(true, 1L << 20, 1L << 16,
        1L, 1_000_000.0d, 1.0d, 0.0d, 0.0d, 32L << 20);
    long[] credit = WindowedReadConfig.boundedCredit(config, 1.0d, 1L << 20);
    assertEquals(1_153_433_600L, credit[0]);
    // Honesty bounds it at twice the baseline.
    assertEquals(2L << 20, credit[1]);
    assertEquals(2L << 20, credit[2]);

    // A small baseline bounds the credit at twice that baseline, which is the
    // term that says the read may not run more than twice the material the scan
    // has to traverse anyway.
    credit = WindowedReadConfig.boundedCredit(config, 0.000_1d, 1024L);
    assertEquals(115_343L, credit[0]);
    assertEquals(2048L, credit[1]);
    assertEquals(2048L, credit[2]);

    // The absolute rail binds when it is the smallest of the three.
    WindowedReadConfig railed = new WindowedReadConfig(true, 1L << 20, 1L << 16,
        1L, 1_000_000.0d, 1.0d, 0.0d, 0.0d, 4096L);
    credit = WindowedReadConfig.boundedCredit(railed, 1.0d, 1L << 20);
    assertEquals(4096L, credit[2]);
  }

  @Test
  public void testNetScaledNanosIsTheBreakEvenTest() {
    // Safety 1 so the numbers are the nominal ones.
    // Latency 10ms, bandwidth 1MB/s.
    WindowedReadConfig config = new WindowedReadConfig(true, 1L << 20, 1L << 16,
        10L, 1.0d, 1.0d, 0.0d, 0.0d, 1L << 20);
    // 10ms at 1MB/s is 0.001048576 bytes per ns, so one saved round trip is
    // worth 1e7 * 0.001048576 = 10485.76 bytes. With the 10% band the
    // break-even bridge is 10485.76 / 1.1 = 9523 bytes.
    assertTrue(config.netScaledNanos(1d, 9_000L) > 0d);
    assertTrue(config.netScaledNanos(1d, 10_000L) < 0d);
    // A window that merely TIES is refused: the margin is what makes it a floor.
    // A margin of exactly one latency makes one saved round trip bridging
    // nothing tie, and a tie is refused.
    WindowedReadConfig withMargin = new WindowedReadConfig(true, 1L << 20, 1L << 16,
        10L, 1.0d, 1.0d, 10.0d, 0.0d, 1L << 20);
    assertEquals(0d, withMargin.netScaledNanos(1d, 0L), 0d);
    assertTrue(withMargin.netScaledNanos(1d, 0L) <= 0d);
    assertTrue(withMargin.netScaledNanos(1.5d, 0L) > 0d);
  }
}
