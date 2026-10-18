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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.RecordReaderImpl.SargApplier;
import org.apache.orc.impl.RecordReaderImpl.SargApplier.RowGroupPruning;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Check that binary searching the row group index of an ordered column picks
 * the same row groups as comparing the statistics of every row group, and that
 * the columns whose statistics are not ordered keep the linear evaluation.
 */
public class TestSargApplierBinarySearch {
  private static final long ROW_INDEX_STRIDE = 100;
  private static final OrcFile.WriterVersion WRITER_VERSION = OrcFile.WriterVersion.ORC_135;
  private static final int COLUMN = 1;
  private static final TypeDescription SCHEMA = TypeDescription.fromString("struct<x:int>");

  private static OrcProto.ColumnStatistics createIntStats(long min, long max, boolean hasNull) {
    return OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1)
        .setHasNull(hasNull)
        .setIntStatistics(OrcProto.IntegerStatistics.newBuilder()
            .setMinimum(min)
            .setMaximum(max)
            .build())
        .build();
  }

  private static OrcProto.RowIndex createIndex(long[][] bounds, boolean[] hasNulls) {
    OrcProto.RowIndex.Builder index = OrcProto.RowIndex.newBuilder();
    for (int group = 0; group < bounds.length; ++group) {
      index.addEntry(OrcProto.RowIndexEntry.newBuilder()
          .setStatistics(createIntStats(bounds[group][0], bounds[group][1], hasNulls[group]))
          .build());
    }
    return index.build();
  }

  private static List<OrcProto.ColumnEncoding> createEncodings() {
    List<OrcProto.ColumnEncoding> encodings = new ArrayList<>();
    for (int col = 0; col <= COLUMN; ++col) {
      encodings.add(OrcProto.ColumnEncoding.newBuilder()
          .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    }
    return encodings;
  }

  private static SargApplier newApplier(SearchArgument sarg) {
    Configuration conf = new Configuration();
    SchemaEvolution evolution = new SchemaEvolution(SCHEMA, SCHEMA, new Reader.Options(conf));
    return new SargApplier(sarg, ROW_INDEX_STRIDE, evolution, WRITER_VERSION,
        false, false, false);
  }

  private static OrcProto.RowIndex[] newIndexes(OrcProto.RowIndex index) {
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[COLUMN + 1];
    indexes[COLUMN] = index;
    return indexes;
  }

  private static ReaderImpl.StripeInformationImpl newStripe(int groups) {
    OrcProto.StripeInformation stripe = OrcProto.StripeInformation.newBuilder()
        .setNumberOfRows(groups * ROW_INDEX_STRIDE).build();
    return new ReaderImpl.StripeInformationImpl(stripe, 1, -1, null);
  }

  /**
   * Evaluate the predicate against the statistics of every row group, which is
   * the way the row group index used to be searched and the reference of the
   * binary search.
   */
  private static TruthValue[] linearTruthValues(SearchArgument sarg,
                                                OrcProto.RowIndex index,
                                                int groups) throws Exception {
    PredicateLeaf predicate = sarg.getLeaves().get(0);
    OrcProto.ColumnEncoding encoding = createEncodings().get(COLUMN);
    TruthValue[] values = new TruthValue[groups];
    for (int group = 0; group < groups; ++group) {
      OrcProto.RowIndexEntry entry = index.getEntry(group);
      OrcProto.ColumnStatistics stats = entry.hasStatistics() ?
          entry.getStatistics() : RecordReaderImpl.EMPTY_COLUMN_STATISTICS;
      values[group] = RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
          encoding, null, WRITER_VERSION, SCHEMA.findSubtype(COLUMN), false, false);
    }
    return values;
  }

  /**
   * Get the truth values that the binary search found, and check that they are
   * the ones that comparing every row group returns.
   *
   * @return the truth value of every row group
   */
  private static TruthValue[] checkPruning(SearchArgument sarg,
                                           long[][] bounds,
                                           boolean[] hasNulls,
                                           boolean expectPruning) throws Exception {
    int groups = bounds.length;
    OrcProto.RowIndex index = createIndex(bounds, hasNulls);
    OrcProto.RowIndex[] indexes = newIndexes(index);
    TruthValue[] expected = linearTruthValues(sarg, index, groups);

    SargApplier applier = newApplier(sarg);
    RowGroupPruning[] prunings = applier.buildRowGroupPrunings(indexes, groups);
    TruthValue[] actual = new TruthValue[groups];
    TruthValue[][] leaves = new TruthValue[groups][1];
    boolean[] rows = applier.pickRowGroupTruthValues(newStripe(groups), indexes, null,
        createEncodings(), null, leaves);

    // the pruned row groups are exactly the ones whose statistics rule the
    // predicate out, and they are the only ones that are worth pruning
    int skipped = 0;
    for (int group = 0; group < groups; ++group) {
      TruthValue linear = expected[group];
      boolean ruledOut = linear == TruthValue.NO || linear == TruthValue.NO_NULL;
      actual[group] = leaves[group][0];
      String message = "row group " + group + " of " +
          Arrays.toString(bounds[group]) + " with " + sarg.getLeaves().get(0);
      if (expectPruning) {
        assertEquals(ruledOut, prunings[0].isSkipped(group), message);
      } else {
        assertFalse(prunings[0].isSkipped(group), message);
      }
      if (prunings[0].isSkipped(group)) {
        assertEquals(linear, prunings[0].getSkippedValue(group), message);
        skipped += 1;
      }
      assertEquals(linear, actual[group], message);
      assertEquals(ruledOut, !isNeeded(rows, group), message);
    }
    assertEquals(expectPruning, skipped > 0, "the row groups of " +
        Arrays.deepToString(bounds) + " are pruned");
    return actual;
  }

  /**
   * Did the row group survive? pickRowGroups returns null for every row group
   * that is kept and an empty array when it keeps none of them.
   */
  private static boolean isNeeded(boolean[] rows, int group) {
    return rows == null || (group < rows.length && rows[group]);
  }

  private static boolean[] noNulls(int groups) {
    boolean[] hasNulls = new boolean[groups];
    Arrays.fill(hasNulls, false);
    return hasNulls;
  }

  @Test
  public void testAscendingStatistics() throws Exception {
    long[][] bounds = {{0, 10}, {20, 30}, {40, 50}, {60, 70}, {80, 90}};
    SearchArgument equals = SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 65L).end().build();
    TruthValue[] values = checkPruning(equals, bounds, noNulls(bounds.length), true);
    assertEquals(TruthValue.NO, values[0]);
    assertEquals(TruthValue.NO, values[1]);
    assertEquals(TruthValue.NO, values[2]);
    assertEquals(TruthValue.YES_NO, values[3]);
    assertEquals(TruthValue.NO, values[4]);

    // the value is in the first and the last row group
    checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 5L).end().build(),
        bounds, noNulls(bounds.length), true);
    checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 85L).end().build(),
        bounds, noNulls(bounds.length), true);
  }

  @Test
  public void testDescendingStatistics() throws Exception {
    long[][] bounds = {{80, 90}, {60, 70}, {40, 50}, {20, 30}, {0, 10}};
    SearchArgument lessThan = SearchArgumentFactory.newBuilder()
        .startAnd().lessThan("x", PredicateLeaf.Type.LONG, 65L).end().build();
    TruthValue[] values = checkPruning(lessThan, bounds, noNulls(bounds.length), true);
    assertEquals(TruthValue.NO, values[0]);
    assertEquals(TruthValue.YES_NO, values[1]);
    assertEquals(TruthValue.YES, values[2]);
    assertEquals(TruthValue.YES, values[3]);
    assertEquals(TruthValue.YES, values[4]);

    checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().lessThan("x", PredicateLeaf.Type.LONG, 5L).end().build(),
        bounds, noNulls(bounds.length), true);
  }

  @Test
  public void testLessThanAndBetween() throws Exception {
    long[][] bounds = {{0, 10}, {20, 30}, {40, 50}, {60, 70}, {80, 90}};
    TruthValue[] values = checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().lessThan("x", PredicateLeaf.Type.LONG, 50L).end().build(),
        bounds, noNulls(bounds.length), true);
    assertEquals(TruthValue.YES, values[0]);
    assertEquals(TruthValue.YES, values[1]);
    assertEquals(TruthValue.YES_NO, values[2]);
    assertEquals(TruthValue.NO, values[3]);
    assertEquals(TruthValue.NO, values[4]);

    values = checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().lessThanEquals("x", PredicateLeaf.Type.LONG, 40L).end().build(),
        bounds, noNulls(bounds.length), true);
    assertEquals(TruthValue.YES, values[0]);
    assertEquals(TruthValue.YES, values[1]);
    assertEquals(TruthValue.YES_NO, values[2]);
    assertEquals(TruthValue.NO, values[3]);
    assertEquals(TruthValue.NO, values[4]);

    // the range covers the middle three row groups
    values = checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().between("x", PredicateLeaf.Type.LONG, 25L, 65L).end().build(),
        bounds, noNulls(bounds.length), true);
    assertEquals(TruthValue.NO, values[0]);
    assertEquals(TruthValue.YES_NO, values[1]);
    assertEquals(TruthValue.YES, values[2]);
    assertEquals(TruthValue.YES_NO, values[3]);
    assertEquals(TruthValue.NO, values[4]);

    // the range matches nothing
    values = checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().between("x", PredicateLeaf.Type.LONG, 11L, 19L).end().build(),
        bounds, noNulls(bounds.length), true);
    for (TruthValue value : values) {
      assertEquals(TruthValue.NO, value);
    }
  }

  @Test
  public void testNullStatistics() throws Exception {
    long[][] bounds = {{0, 10}, {20, 30}, {40, 50}, {60, 70}, {80, 90}};
    boolean[] hasNulls = {true, false, true, false, true};
    // the pruned row groups answer with NO_NULL when their statistics hold nulls
    TruthValue[] values = checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 65L).end().build(),
        bounds, hasNulls, true);
    assertEquals(TruthValue.NO_NULL, values[0]);
    assertEquals(TruthValue.NO, values[1]);
    assertEquals(TruthValue.NO_NULL, values[2]);
    assertEquals(TruthValue.YES_NO, values[3]);
    assertEquals(TruthValue.NO_NULL, values[4]);

    // the less than operator answers with YES_NULL when it matches every value,
    // so there is nothing to prune
    values = checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().lessThan("x", PredicateLeaf.Type.LONG, 100L).end().build(),
        bounds, hasNulls, false);
    assertEquals(TruthValue.YES_NULL, values[0]);
    assertEquals(TruthValue.YES, values[1]);
    assertEquals(TruthValue.YES_NULL, values[2]);
    assertEquals(TruthValue.YES, values[3]);
    assertEquals(TruthValue.YES_NULL, values[4]);

    // the row groups with nulls are never ruled out by the is null predicate,
    // which keeps the linear evaluation
    checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().isNull("x", PredicateLeaf.Type.LONG).end().build(),
        bounds, hasNulls, false);
  }

  @Test
  public void testUnorderedStatistics() throws Exception {
    // the statistics go up and down, so they cannot be binary searched
    long[][] bounds = {{0, 10}, {60, 70}, {20, 30}, {80, 90}, {40, 50}};
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 65L).end().build();
    TruthValue[] values = checkPruning(sarg, bounds, noNulls(bounds.length), false);
    assertEquals(TruthValue.NO, values[0]);
    assertEquals(TruthValue.YES_NO, values[1]);
    assertEquals(TruthValue.NO, values[2]);
    assertEquals(TruthValue.NO, values[3]);
    assertEquals(TruthValue.NO, values[4]);

    // the minimum is ordered but the maximum is not
    bounds = new long[][]{{0, 90}, {20, 30}, {40, 70}};
    checkPruning(sarg, bounds, noNulls(bounds.length), false);

    // the maximum is ordered but the minimum is not
    bounds = new long[][]{{0, 10}, {40, 30}, {20, 50}};
    checkPruning(sarg, bounds, noNulls(bounds.length), false);
  }

  @Test
  public void testEdgeCases() throws Exception {
    SearchArgument equals = SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 5L).end().build();

    // a single row group that matches nothing
    TruthValue[] values = checkPruning(equals, new long[][]{{10, 20}},
        noNulls(1), true);
    assertEquals(TruthValue.NO, values[0]);

    // a single row group that matches everything
    values = checkPruning(equals, new long[][]{{0, 10}}, noNulls(1), false);
    assertEquals(TruthValue.YES_NO, values[0]);

    // every row group is ruled out
    values = checkPruning(equals, new long[][]{{10, 20}, {30, 40}, {50, 60}},
        noNulls(3), true);
    for (TruthValue value : values) {
      assertEquals(TruthValue.NO, value);
    }

    // no row group is ruled out
    values = checkPruning(equals, new long[][]{{0, 10}, {0, 20}, {0, 30}},
        noNulls(3), false);
    for (TruthValue value : values) {
      assertEquals(TruthValue.YES_NO, value);
    }

    // the value is on the first and the last boundary
    values = checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 0L).end().build(),
        new long[][]{{0, 10}, {0, 10}, {0, 10}}, noNulls(3), false);
    for (TruthValue value : values) {
      assertEquals(TruthValue.YES_NO, value);
    }
    values = checkPruning(SearchArgumentFactory.newBuilder()
        .startAnd().lessThanEquals("x", PredicateLeaf.Type.LONG, 10L).end().build(),
        new long[][]{{0, 10}, {0, 10}, {0, 10}}, noNulls(3), false);
    for (TruthValue value : values) {
      assertEquals(TruthValue.YES, value);
    }

    // the statistics do not change at all, which is ordered either way
    checkPruning(equals, new long[][]{{5, 5}, {5, 5}, {5, 5}, {5, 5}},
        noNulls(4), false);
  }

  @Test
  public void testMissingStatistics() throws Exception {
    SearchArgument equals = SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 65L).end().build();
    // a row group without statistics cannot be ruled out, so the whole stripe
    // falls back to the linear evaluation
    OrcProto.RowIndex index = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .setStatistics(createIntStats(0, 10, false)).build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder().build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .setStatistics(createIntStats(80, 90, false)).build())
        .build();
    OrcProto.RowIndex[] indexes = newIndexes(index);
    SargApplier applier = newApplier(equals);
    assertFalse(applier.buildRowGroupPrunings(indexes, 3)[0].isSkipped(0));
    TruthValue[][] values = new TruthValue[3][1];
    applier.pickRowGroupTruthValues(newStripe(3), indexes, null,
        createEncodings(), null, values);
    assertEquals(TruthValue.NO, values[0][0]);
    assertEquals(TruthValue.YES_NO_NULL, values[1][0]);
    assertEquals(TruthValue.NO, values[2][0]);
  }

  @Test
  public void testUncastablePredicate() throws Exception {
    // the statistics cannot be converted to the type of the predicate, so the
    // linear evaluation has to report the failure of every row group
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.DATE, Date.valueOf("2017-01-02"))
        .end().build();
    long[][] bounds = {{0, 10}, {20, 30}, {40, 50}};
    OrcProto.RowIndex[] indexes = newIndexes(createIndex(bounds, noNulls(bounds.length)));
    SargApplier applier = newApplier(sarg);
    assertFalse(applier.buildRowGroupPrunings(indexes, bounds.length)[0].isSkipped(0));
    TruthValue[][] values = new TruthValue[bounds.length][1];
    boolean[] rows = applier.pickRowGroupTruthValues(newStripe(bounds.length), indexes,
        null, createEncodings(), null, values);
    assertTrue(applier.getExceptionCount()[0] > 0);
    for (int group = 0; group < bounds.length; ++group) {
      assertTrue(isNeeded(rows, group));
      assertEquals(TruthValue.YES_NO, values[group][0]);
    }
  }

  @Test
  public void testRandomStatistics() throws Exception {
    Random rand = new Random(20240701);
    int[] operators = {0, 1, 2, 3};
    for (int run = 0; run < 400; ++run) {
      int groups = 1 + rand.nextInt(12);
      long[][] bounds = new long[groups][2];
      long min = 0;
      long max = 0;
      int shape = rand.nextInt(4);
      // the first three shapes keep the statistics ordered
      boolean ordered = shape != 3;
      for (int group = 0; group < groups; ++group) {
        switch (shape) {
          case 0: // ascending and disjoint
            min = max + rand.nextInt(3);
            max = min + rand.nextInt(3);
            break;
          case 1: // descending and disjoint
            max = min - rand.nextInt(3);
            min = max - rand.nextInt(3);
            break;
          case 2: // ascending, sharing the boundaries
            min = max;
            if (rand.nextBoolean()) {
              max = min + 1;
            }
            break;
          default: // unordered
            min = rand.nextInt(21) - 10;
            max = min + rand.nextInt(3);
            break;
        }
        bounds[group][0] = min;
        bounds[group][1] = max;
      }
      boolean[] hasNulls = new boolean[groups];
      for (int group = 0; group < groups; ++group) {
        hasNulls[group] = rand.nextBoolean();
      }
      long literal = rand.nextInt(21) - 10;
      long other = literal + rand.nextInt(11) - 5;
      SearchArgument.Builder builder = SearchArgumentFactory.newBuilder().startAnd();
      switch (operators[rand.nextInt(operators.length)]) {
        case 0:
          builder.equals("x", PredicateLeaf.Type.LONG, literal);
          break;
        case 1:
          builder.lessThan("x", PredicateLeaf.Type.LONG, literal);
          break;
        case 2:
          builder.lessThanEquals("x", PredicateLeaf.Type.LONG, literal);
          break;
        default:
          builder.between("x", PredicateLeaf.Type.LONG,
              Math.min(literal, other), Math.max(literal, other));
          break;
      }
      SearchArgument sarg = builder.end().build();
      OrcProto.RowIndex index = createIndex(bounds, hasNulls);
      OrcProto.RowIndex[] indexes = newIndexes(index);
      SargApplier applier = newApplier(sarg);
      RowGroupPruning pruning = applier.buildRowGroupPrunings(indexes, groups)[0];
      TruthValue[] expected = linearTruthValues(sarg, index, groups);
      TruthValue[][] actual = new TruthValue[groups][1];
      boolean[] rows = applier.pickRowGroupTruthValues(newStripe(groups), indexes, null,
          createEncodings(), null, actual);
      for (int group = 0; group < groups; ++group) {
        TruthValue linear = expected[group];
        boolean ruledOut = linear == TruthValue.NO || linear == TruthValue.NO_NULL;
        String message = "run " + run + " " + sarg.getLeaves().get(0) + " " +
            Arrays.deepToString(bounds) + " row group " + group;
        // the ordered statistics prune exactly the row groups that cannot
        // match, the unordered ones never prune a row group that may match
        if (ordered) {
          assertEquals(ruledOut, pruning.isSkipped(group), message);
        } else {
          assertTrue(!pruning.isSkipped(group) || ruledOut, message);
        }
        if (pruning.isSkipped(group)) {
          assertEquals(linear, pruning.getSkippedValue(group), message);
        }
        assertEquals(linear, actual[group][0], message);
        assertEquals(ruledOut, !isNeeded(rows, group), message);
      }
    }
  }

  @Test
  public void testPicksTheSameRowGroups() throws Exception {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd().equals("x", PredicateLeaf.Type.LONG, 100L).end().build();
    List<OrcProto.ColumnEncoding> encodings = createEncodings();

    // the statistics of the last row group break the order, so they keep being
    // compared one by one, and the row groups that are picked are unchanged
    long[][] bounds = {{0, 10}, {100, 200}, {300, 500}, {100, 100}};
    OrcProto.RowIndex[] indexes = newIndexes(createIndex(bounds, noNulls(bounds.length)));
    boolean[] rows = newApplier(sarg).pickRowGroups(newStripe(bounds.length), indexes,
        null, encodings, null, false);
    assertNotEquals(null, rows);
    assertEquals(4, rows.length);
    assertFalse(rows[0]);
    assertTrue(rows[1]);
    assertFalse(rows[2]);
    assertTrue(rows[3]);

    // the same statistics without the last row group are ordered, and the row
    // groups that are picked are the same
    bounds = new long[][]{{0, 10}, {100, 200}, {300, 500}, {600, 700}};
    indexes = newIndexes(createIndex(bounds, noNulls(bounds.length)));
    rows = newApplier(sarg).pickRowGroups(newStripe(bounds.length), indexes,
        null, encodings, null, false);
    assertNotEquals(null, rows);
    assertEquals(4, rows.length);
    assertFalse(rows[0]);
    assertTrue(rows[1]);
    assertFalse(rows[2]);
    assertFalse(rows[3]);
  }
}
