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

import org.apache.orc.StripeInformation;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.reader.tree.TypeReader;

import java.util.List;

/**
 * The read-side scan state that the windowed read arm needs but that
 * {@link StripePlannerFactory#create} does not carry.
 *
 * <p>The factory seam only hands the planner a schema, the encryption
 * variant, the {@link org.apache.orc.DataReader}, the writer version and a
 * handful of flags. Windowing additionally needs to know <em>which</em>
 * stripes the scan will walk, which of them survived the stripe-statistics
 * pruning, and how to re-run the SARG entirely in memory. {@code
 * RecordReaderImpl} owns all of that, so it hands it over here.</p>
 *
 * <p>This is a pure value holder: it performs no I/O and the arrays are the
 * very same ones the reader uses.</p>
 */
public final class WindowedScanContext {

  private final List<? extends StripeInformation> stripes;
  private final boolean[] includedStripes;
  private final RecordReaderImpl.SargApplier sargApplier;
  private final boolean[] fileIncluded;
  private final boolean[] rowIndexColsToRead;
  private final TypeReader.ReadPhase startReadPhase;
  private final boolean skipBloomFilters;
  private final int columns;

  public WindowedScanContext(List<? extends StripeInformation> stripes,
                             boolean[] includedStripes,
                             RecordReaderImpl.SargApplier sargApplier,
                             boolean[] fileIncluded,
                             boolean[] rowIndexColsToRead,
                             TypeReader.ReadPhase startReadPhase,
                             boolean skipBloomFilters,
                             int columns) {
    this.stripes = stripes;
    this.includedStripes = includedStripes;
    this.sargApplier = sargApplier;
    this.fileIncluded = fileIncluded;
    this.rowIndexColsToRead = rowIndexColsToRead;
    this.startReadPhase = startReadPhase;
    this.skipBloomFilters = skipBloomFilters;
    this.columns = columns;
  }

  /** The number of columns in the file, i.e. {@code schema.getMaximumId() + 1}. */
  public int columns() {
    return columns;
  }

  public List<? extends StripeInformation> stripes() {
    return stripes;
  }

  /**
   * Was this stripe kept by the stripe-statistics pruning? A {@code null}
   * array, an out-of-range id, or a {@code false} entry all mean "pruned":
   * round 1 still fetches the stripe's footer, but no code path ever reads
   * its data.
   */
  public boolean isStripeIncluded(int stripeId) {
    if (includedStripes == null || stripeId < 0 || stripeId >= includedStripes.length) {
      return true;
    }
    return includedStripes[stripeId];
  }

  public RecordReaderImpl.SargApplier sargApplier() {
    return sargApplier;
  }

  /**
   * Does the scan carry a SARG? The row indexes are only worth prefetching
   * when there is something to evaluate against them.
   */
  public boolean hasSarg() {
    return sargApplier != null;
  }

  public boolean[] fileIncluded() {
    return fileIncluded;
  }

  public boolean[] rowIndexColsToRead() {
    return rowIndexColsToRead;
  }

  public TypeReader.ReadPhase startReadPhase() {
    return startReadPhase;
  }

  public boolean skipBloomFilters() {
    return skipBloomFilters;
  }
}
