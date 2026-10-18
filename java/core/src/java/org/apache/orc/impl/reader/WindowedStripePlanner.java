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
import org.apache.orc.OrcFile;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.Set;

/**
 * The planner half of the windowed read arm.
 *
 * <p>It does not change a single byte of what the scan reads: the row plan
 * is the base {@link StripePlanner} plan, served out of whatever the
 * {@link WindowedDataReader} already fetched. All it adds is a hook that lets
 * the windowed reader get its turn before ORC asks the delegate for a
 * stripe, and the ability to mint the scratch planners the windowed reader
 * uses to evaluate the SARG in memory.</p>
 *
 * <p>Because {@code RecordReaderImpl.readRowIndex} copies its planner with
 * {@code new StripePlanner(planner)}, no state of this class survives that
 * copy — which is why the arm keeps its state in the
 * {@link WindowedDataReader} instead.</p>
 */
public class WindowedStripePlanner extends StripePlanner {

  private final WindowedDataReader windowed;

  public WindowedStripePlanner(TypeDescription schema,
                               ReaderEncryption encryption,
                               DataReader dataReader,
                               OrcFile.WriterVersion version,
                               boolean ignoreNonUtf8BloomFilter,
                               long maxBufferSize,
                               Set<Integer> filterColIds) {
    super(schema, encryption, dataReader, version, ignoreNonUtf8BloomFilter, maxBufferSize,
        filterColIds);
    this.windowed = dataReader instanceof WindowedDataReader ?
        (WindowedDataReader) dataReader : null;
    if (windowed != null) {
      windowed.setTemplatePlanner(this);
    }
  }

  /** The windowed reader this planner was built around, if any. */
  public WindowedDataReader getWindowedDataReader() {
    return windowed;
  }

  /**
   * A plain planner carrying this planner's base state. The windowed reader
   * uses one per stripe to produce the same byte plan the real reader would,
   * which is what makes the in-memory SARG selection exact.
   */
  public StripePlanner newScratchPlanner() {
    return new StripePlanner(this);
  }

  /**
   * Give the windowed reader its turn before the stripe is touched. The
   * reader normally drives this through {@link #readStripeFooter}; doing it
   * here as well keeps the planner correct when used on its own.
   */
  @Override
  public StripePlanner parseStripe(StripeInformation stripe,
                                   boolean[] columnInclude) throws IOException {
    if (windowed != null) {
      windowed.ensureWindow(stripe.getStripeId());
    }
    return super.parseStripe(stripe, columnInclude);
  }
}
