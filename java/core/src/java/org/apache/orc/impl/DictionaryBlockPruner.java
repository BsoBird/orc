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

import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StripePlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Skips the stripes whose dictionary can not contain a single value that
 * matches the search argument.
 * <p>
 * A string column that is dictionary encoded for a whole stripe can only hold
 * the values of its dictionary, and a dictionary never holds a null (the nulls
 * live in the PRESENT stream). So when none of the dictionary entries satisfies
 * one of the comparisons of the search argument, no row of the stripe can
 * satisfy the search argument and the data streams of the stripe never have to
 * be read. This is the same idea as the dictionary block filter of the Parquet
 * reader, where a row group whose dictionary can not match is dropped.
 * <p>
 * Only conjunctions of plain comparisons of a string column are decided this
 * way. A search argument with a disjunction, a negation, a null test or a
 * comparison of another type falls back to the normal read path, as do
 * encrypted columns and the char and varchar categories, whose values are
 * trimmed after they are read and so differ from the dictionary bytes.
 */
class DictionaryBlockPruner {

  private static final Logger LOG = LoggerFactory.getLogger(DictionaryBlockPruner.class);

  /**
   * A single comparison of the search argument, evaluated against one
   * dictionary entry.
   */
  private interface ColumnPredicate {
    boolean matches(byte[] bytes, int offset, int length);
  }

  /**
   * The dictionary values of one column of one stripe.
   */
  private static class Dictionary {
    private final byte[] bytes;
    private final int[] offsets;

    Dictionary(byte[] bytes, int[] offsets) {
      this.bytes = bytes;
      this.offsets = offsets;
    }
  }

  // the comparisons of the search argument, by the file column they apply to
  private final Map<Integer, List<ColumnPredicate>> predicates;
  private final TreeReaderFactory.Context context;

  private DictionaryBlockPruner(Map<Integer, List<ColumnPredicate>> predicates,
                                TreeReaderFactory.Context context) {
    this.predicates = predicates;
    this.context = context;
  }

  /**
   * Build the pruner for a search argument.
   * @param sarg the search argument of the reader, null for no search argument
   * @param evolution the mapping from the reader to the file schema
   * @param encryption the encryption of the file, null for unencrypted files
   * @param context the context to read the lengths of the dictionary with
   * @return the pruner or null if the search argument can not be decided by
   *         the dictionaries of the file
   */
  static DictionaryBlockPruner create(SearchArgument sarg,
                                      SchemaEvolution evolution,
                                      ReaderEncryption encryption,
                                      TreeReaderFactory.Context context) {
    if (sarg == null || sarg.getExpression() == null) {
      return null;
    }
    Map<Integer, List<ColumnPredicate>> predicates = new TreeMap<>();
    if (!collect(sarg.getExpression(), evolution, encryption, predicates)) {
      return null;
    }
    if (predicates.isEmpty()) {
      return null;
    }
    return new DictionaryBlockPruner(predicates, context);
  }

  /**
   * Collect the comparisons of a conjunction of the search argument.
   * @return false if any part of the expression is not a comparison of a
   *         string column that the dictionaries can decide
   */
  private static boolean collect(ExpressionTree tree,
                                 SchemaEvolution evolution,
                                 ReaderEncryption encryption,
                                 Map<Integer, List<ColumnPredicate>> predicates) {
    switch (tree.getOperator()) {
      case AND:
        for(ExpressionTree child: tree.getChildren()) {
          if (!collect(child, evolution, encryption, predicates)) {
            return false;
          }
        }
        return true;
      case LEAF:
        return addLeaf(tree.getPredicateLeaf(), evolution, encryption, predicates);
      default:
        // a disjunction, a negation or a constant is not decided by any single
        // dictionary entry
        return false;
    }
  }

  /**
   * Add one comparison of the search argument to the columns it applies to.
   * @return false if the comparison can not be decided by the dictionary of
   *         its column
   */
  private static boolean addLeaf(PredicateLeaf leaf,
                                 SchemaEvolution evolution,
                                 ReaderEncryption encryption,
                                 Map<Integer, List<ColumnPredicate>> predicates) {
    if (leaf.getType() != PredicateLeaf.Type.STRING) {
      return false;
    }
    int readerColumn;
    try {
      readerColumn = RecordReaderImpl.findColumns(evolution, leaf.getColumnName());
    } catch (IllegalArgumentException e) {
      LOG.debug("The dictionary of {} can not be used: {}", leaf.getColumnName(),
          e.getMessage());
      return false;
    }
    if (readerColumn == -1) {
      return false;
    }
    TypeDescription readerType = evolution.getReaderSchema().findSubtype(readerColumn);
    TypeDescription fileType = evolution.getFileType(readerColumn);
    if (readerType == null || fileType == null ||
        readerType.getCategory() != TypeDescription.Category.STRING ||
        fileType.getCategory() != TypeDescription.Category.STRING) {
      // the values of char and varchar columns are trimmed after they are
      // read, so the dictionary bytes are not the values that the comparison
      // of the search argument sees
      return false;
    }
    int column = fileType.getId();
    if (!evolution.getFileIncluded()[column]) {
      return false;
    }
    if (encryption != null && encryption.getVariant(column) != null) {
      return false;
    }
    ColumnPredicate predicate = create(leaf);
    if (predicate == null) {
      return false;
    }
    predicates.computeIfAbsent(column, key -> new ArrayList<>()).add(predicate);
    return true;
  }

  /**
   * Convert one leaf of the search argument into a comparison of a dictionary
   * entry. The comparison uses the same functions as the row filters of
   * {@link org.apache.orc.impl.filter.leaf.StringFilters}, so that it accepts
   * exactly the values that those filters accept.
   * @return null if the operator is not one that the dictionary can decide
   */
  private static ColumnPredicate create(PredicateLeaf leaf) {
    switch (leaf.getOperator()) {
      case EQUALS: {
        byte[] literal = literal(leaf.getLiteral());
        return literal == null ? null
            : (bytes, offset, length) ->
                StringExpr.equal(literal, 0, literal.length, bytes, offset, length);
      }
      case LESS_THAN: {
        byte[] literal = literal(leaf.getLiteral());
        return literal == null ? null
            : (bytes, offset, length) ->
                StringExpr.compare(bytes, offset, length, literal, 0, literal.length) < 0;
      }
      case LESS_THAN_EQUALS: {
        byte[] literal = literal(leaf.getLiteral());
        return literal == null ? null
            : (bytes, offset, length) ->
                StringExpr.compare(bytes, offset, length, literal, 0, literal.length) <= 0;
      }
      case BETWEEN: {
        List<Object> literals = leaf.getLiteralList();
        if (literals == null || literals.size() != 2) {
          return null;
        }
        byte[] low = literal(literals.get(0));
        byte[] high = literal(literals.get(1));
        return low == null || high == null ? null
            : (bytes, offset, length) ->
                StringExpr.compare(bytes, offset, length, low, 0, low.length) >= 0 &&
                StringExpr.compare(bytes, offset, length, high, 0, high.length) <= 0;
      }
      case IN: {
        List<Object> literals = leaf.getLiteralList();
        if (literals == null) {
          return null;
        }
        List<byte[]> values = new ArrayList<>(literals.size());
        for(Object value: literals) {
          byte[] bytes = literal(value);
          if (bytes == null) {
            return null;
          }
          values.add(bytes);
        }
        return (bytes, offset, length) -> {
          for(byte[] value: values) {
            if (StringExpr.equal(value, 0, value.length, bytes, offset, length)) {
              return true;
            }
          }
          return false;
        };
      }
      default:
        // null tests, null safe equals and anything the row filters do not
        // support fall back to the normal read path
        return null;
    }
  }

  /**
   * Convert a literal of the search argument the same way that the row filters
   * convert it.
   * @return null if the literal is not a string
   */
  private static byte[] literal(Object value) {
    return value instanceof String
        ? ((String) value).getBytes(StandardCharsets.UTF_8) : null;
  }

  /**
   * @return the number of columns that the pruner checks
   */
  int getColumnCount() {
    return predicates.size();
  }

  /**
   * Check if the current stripe can be skipped because none of the dictionary
   * entries satisfies one of the comparisons of the search argument. The
   * dictionaries of the candidate columns are read to answer the question, but
   * their data streams are not.
   * @param planner the planner of the stripe that has been parsed
   * @return true if no row of the stripe can match the search argument
   */
  boolean canSkipStripe(StripePlanner planner) throws IOException {
    // only the columns that the stripe encodes as a whole dictionary can be
    // decided without their data streams
    Map<Integer, List<ColumnPredicate>> candidates = new TreeMap<>();
    for(Map.Entry<Integer, List<ColumnPredicate>> entry: predicates.entrySet()) {
      OrcProto.ColumnEncoding encoding = planner.getEncoding(entry.getKey());
      if (encoding == null || encoding.getDictionarySize() == 0) {
        continue;
      }
      if (encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
          encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
        candidates.put(entry.getKey(), entry.getValue());
      }
    }
    if (candidates.isEmpty()) {
      return false;
    }
    planner.readDictionaries(candidates.keySet());
    for(Map.Entry<Integer, List<ColumnPredicate>> entry: candidates.entrySet()) {
      int column = entry.getKey();
      Dictionary dictionary = readDictionary(planner, column);
      if (dictionary == null) {
        continue;
      }
      // every row of the stripe holds one of the dictionary entries, so a
      // comparison that none of them satisfies can not match any row. A null
      // row does not match either, because the literals are never null.
      for(ColumnPredicate predicate: entry.getValue()) {
        boolean possible = false;
        for(int i = 0; i + 1 < dictionary.offsets.length && !possible; ++i) {
          int offset = dictionary.offsets[i];
          possible = predicate.matches(dictionary.bytes, offset,
              dictionary.offsets[i + 1] - offset);
        }
        if (!possible) {
          LOG.debug("Column {} has no dictionary entry that matches the search argument, " +
              "so the stripe is skipped", column);
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Read the dictionary of a column of the current stripe.
   * @return the dictionary or null if it can not be read
   */
  private Dictionary readDictionary(StripePlanner planner, int column) throws IOException {
    OrcProto.ColumnEncoding encoding = planner.getEncoding(column);
    int dictionarySize = encoding.getDictionarySize();
    // read the length of each entry
    int[] offsets = new int[dictionarySize + 1];
    InStream in = planner.getStream(new StreamName(column, OrcProto.Stream.Kind.LENGTH));
    if (in == null) {
      return null;
    }
    IntegerReader lengthReader = TreeReaderFactory.TreeReader.createIntegerReader(
        encoding.getKind(), in, false, context);
    int offset = 0;
    for(int i = 0; i < dictionarySize; ++i) {
      offsets[i] = offset;
      offset += (int) lengthReader.next();
    }
    offsets[dictionarySize] = offset;
    in.close();
    // read the bytes of all of the entries
    byte[] bytes = new byte[offset];
    in = planner.getStream(new StreamName(column, OrcProto.Stream.Kind.DICTIONARY_DATA));
    if (in == null) {
      return offset == 0 ? new Dictionary(bytes, offsets) : null;
    }
    int position = 0;
    while(position < offset) {
      int read = in.read(bytes, position, offset - position);
      if (read <= 0) {
        return null;
      }
      position += read;
    }
    in.close();
    return new Dictionary(bytes, offsets);
  }
}
