/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
 * A seam for replacing the {@link StripePlanner} that a
 * {@code org.apache.orc.impl.RecordReaderImpl} uses to parse stripes and to plan
 * the index and data reads.
 * <p>
 * The factory is set with
 * {@link org.apache.orc.Reader.Options#stripePlannerFactory(StripePlannerFactory)}
 * and is consulted once per {@code RecordReaderImpl}. When no factory is set,
 * the reader creates a plain {@code StripePlanner} exactly as it did before
 * this seam existed, so behavior is unchanged for existing callers.
 * <p>
 * <b>Subclassing contract</b> for implementations that return a
 * {@code StripePlanner} subclass:
 * <ul>
 *   <li>{@link #create} must not return {@code null}; it must either return a
 *       planner or throw.</li>
 *   <li>Implementing {@code create} as
 *       {@code return new MyPlanner(schema, encryption, dataReader, version,
 *       ignoreNonUtf8BloomFilter, maxBufferSize, filterColIds);} is the expected
 *       shape, since that is the constructor of {@link StripePlanner} itself.</li>
 *   <li>Implementations must perform no I/O in {@code create}: the reader calls
 *       it while it is still constructing itself. Deferring all I/O to
 *       {@link StripePlanner#parseStripe(org.apache.orc.StripeInformation, boolean[], org.apache.orc.OrcProto.StripeFooter)}
 *       keeps the stripe walk unchanged. That three argument overload is the one
 *       the reader uses, because it has already read the stripe footer.</li>
 *   <li>The returned planner is owned by a single reader and is used
 *       single-threaded.</li>
 *   <li>Any behavior that a planner override changes must not change the rows
 *       that the reader returns, only which bytes are read. Planners are
 *       expected to plan a subset of the bytes that the base planner plans and
 *       to serve exactly the chunks the base planner would have served.</li>
 *   <li>{@code RecordReaderImpl.readRowIndex(int, boolean[], boolean[])} reads
 *       an off-current-stripe row index with {@code new StripePlanner(planner)},
 *       which invokes the base copy constructor and therefore drops subclass
 *       state. Subclasses must therefore treat the base fields
 *       (schema, encryption, dataReader, version, ignoreNonUtf8BloomFilter,
 *       maxBufferSize and filterColIds) as the complete state they can rely on,
 *       because that copy constructor is not overridable. Subclass state that
 *       must survive there has to be reachable from those fields or recomputed
 *       in the subclass constructor.</li>
 * </ul>
 * @since 1.9.0
 */
public interface StripePlannerFactory {

  /**
   * Create the stripe planner for a record reader.
   * @param schema the file schema
   * @param encryption the encryption information for the file variant
   * @param dataReader the underlying data reader, already cloned and configured
   * @param version the file writer version
   * @param ignoreNonUtf8BloomFilter ignore old non-utf8 bloom filters
   * @param maxBufferSize the largest single buffer to use
   * @param filterColIds the ids of the columns whose streams are always read
   * @return a planner for the reader, never {@code null}
   * @since 1.9.0
   */
  StripePlanner create(TypeDescription schema,
                       ReaderEncryption encryption,
                       DataReader dataReader,
                       OrcFile.WriterVersion version,
                       boolean ignoreNonUtf8BloomFilter,
                       long maxBufferSize,
                       Set<Integer> filterColIds);
}
