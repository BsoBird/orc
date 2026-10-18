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
import org.apache.orc.TypeDescription;

import java.util.Set;

/**
 * Hands out a {@link WindowedStripePlanner} — and nothing else.
 *
 * <p>{@code RecordReaderImpl} installs this only when
 * {@code orc.windowed.read.enabled} is true, and only when the caller has not
 * supplied a factory of their own. With the flag off the reader takes its
 * ordinary {@link StripePlanner} path and nothing here is constructed.</p>
 *
 * <p>Per the {@link StripePlannerFactory} contract this method performs no
 * I/O: the window is planned lazily, on the first stripe the scan enters.</p>
 */
public class WindowedStripePlannerFactory implements StripePlannerFactory {

  @Override
  public StripePlanner create(TypeDescription schema,
                              ReaderEncryption encryption,
                              DataReader dataReader,
                              OrcFile.WriterVersion version,
                              boolean ignoreNonUtf8BloomFilter,
                              long maxBufferSize,
                              Set<Integer> filterColIds) {
    if (dataReader instanceof WindowedDataReader) {
      return new WindowedStripePlanner(schema, encryption, dataReader, version,
          ignoreNonUtf8BloomFilter, maxBufferSize, filterColIds);
    }
    // The decorator is not in place, so there is nothing to window. Fall back
    // to the base planner rather than fail: the factory must not return null.
    return new StripePlanner(schema, encryption, dataReader, version,
        ignoreNonUtf8BloomFilter, maxBufferSize, filterColIds);
  }
}
