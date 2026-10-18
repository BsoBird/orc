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

package org.apache.orc.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the optional parallel footer read of {@link OrcInputFormat}.
 */
public class TestParallelFooterInputFormat {

  private static final TypeDescription SCHEMA =
      TypeDescription.fromString("struct<i:int>");

  private static final Path WORK_DIR = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"),
      TestParallelFooterInputFormat.class.getSimpleName());

  private Configuration conf;
  private LocalFileSystem fs;
  /** Each test works in a directory of its own, so no footers are left over. */
  private Path workDir;

  /**
   * An input format that counts the batches of footers it read up front.
   */
  private static class CountingInputFormat extends OrcInputFormat<OrcStruct> {
    private final List<Integer> prefetched = new ArrayList<>();

    @Override
    protected void prefetchFooters(Configuration conf, List<InputSplit> splits) {
      prefetched.add(splits.size());
      super.prefetchFooters(conf, splits);
    }
  }

  @BeforeEach
  public void setup(TestInfo info) throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    workDir = new Path(WORK_DIR, info.getTestMethod().get().getName());
    fs.delete(workDir, true);
    fs.mkdirs(workDir);
  }

  /**
   * Write a file of the given size into the work directory.
   * @param file the number of this file
   * @param rows the number of rows to write
   * @return the path of the file
   * @throws IOException if the file can't be written
   */
  private Path writeFile(int file, int rows) throws IOException {
    Path path = new Path(workDir, "part-" + file + ".orc");
    try (org.apache.orc.Writer writer = OrcFile.createWriter(path,
        OrcFile.writerOptions(conf).setSchema(SCHEMA))) {
      VectorizedRowBatch batch = SCHEMA.createRowBatch(rows);
      LongColumnVector ints = (LongColumnVector) batch.cols[0];
      ints.noNulls = true;
      for (int r = 0; r < rows; ++r) {
        ints.vector[r] = r + file * 1000;
      }
      batch.size = rows;
      writer.addRowBatch(batch);
    }
    return path;
  }

  /**
   * Put rubbish over the ORC magic at both ends of a file, leaving its rows
   * intact. Nothing can tell that this is an ORC file any more.
   * @param path the file to spoil
   * @throws IOException if the file can't be written
   */
  private void spoilFooter(Path path) throws IOException {
    byte[] content;
    try (FSDataInputStream in = fs.open(path)) {
      content = new byte[(int) fs.getFileStatus(path).getLen()];
      in.readFully(content);
    }
    Arrays.fill(content, 0, 3, (byte) 'x');
    Arrays.fill(content, content.length - 4, content.length, (byte) 'x');
    try (FSDataOutputStream out = fs.create(path, true)) {
      out.write(content);
    }
  }

  /**
   * Make the files of the work directory the input of a job.
   * @return the context of that job
   */
  private JobContext getJobContext() {
    conf.set("mapreduce.input.fileinputformat.inputdir", workDir.toString());
    return new JobContextImpl(conf, new JobID("jt", 0));
  }

  private TaskAttemptContext getAttemptContext() {
    return new TaskAttemptContextImpl(conf,
        new TaskAttemptID("jt", 0, TaskType.MAP, 0, 0));
  }

  @Test
  public void testParallelFooterReadIsOffByDefault() throws Exception {
    for (int file = 1; file <= 3; ++file) {
      writeFile(file, 1000 * file);
    }
    assertFalse(OrcConf.PARALLEL_FOOTER_READ.getBoolean(conf),
        "the parallel footer read should be off until it is asked for");

    CountingInputFormat format = new CountingInputFormat();
    List<InputSplit> splits = format.getSplits(getJobContext());
    assertEquals(3, splits.size());
    assertEquals(0, format.prefetched.size(),
        "no footers should be read up front when the switch is off");
  }

  @Test
  public void testParallelFooterReadReadsEveryFileUpFront() throws Exception {
    for (int file = 1; file <= 3; ++file) {
      writeFile(file, 1000 * file);
    }
    conf.setBoolean(OrcConf.PARALLEL_FOOTER_READ.getAttribute(), true);

    CountingInputFormat format = new CountingInputFormat();
    List<InputSplit> splits = format.getSplits(getJobContext());
    assertEquals(3, splits.size());
    assertEquals(1, format.prefetched.size(),
        "the footers should be read once, up front");
    assertEquals(3, format.prefetched.get(0).intValue());

    // The footers that were read up front have to leave every task able to
    // read its file from start to finish.
    TaskAttemptContext attemptContext = getAttemptContext();
    for (InputSplit split : splits) {
      Path path = ((FileSplit) split).getPath();
      int file = Integer.parseInt(path.getName().substring("part-".length(),
          path.getName().length() - ".orc".length()));
      try (RecordReader<NullWritable, OrcStruct> reader =
               format.createRecordReader(split, attemptContext)) {
        for (int r = 0; r < 1000 * file; ++r) {
          assertTrue(reader.nextKeyValue(), "row " + r + " of " + path);
          OrcStruct row = reader.getCurrentValue();
          assertEquals(r + file * 1000,
              ((IntWritable) row.getFieldValue(0)).get(), "row " + r + " of " + path);
        }
        assertFalse(reader.nextKeyValue(), "past the end of " + path);
      }
    }
  }

  @Test
  public void testParallelFooterReadIgnoresUnreadableFiles() throws Exception {
    Path good = writeFile(1, 1000);
    // A file that a task would fail on, but that is not the prefetch's fault.
    Path bad = new Path(workDir, "part-9.orc");
    try (FSDataOutputStream out = fs.create(bad)) {
      out.write("not an ORC file".getBytes(StandardCharsets.UTF_8));
    }
    conf.setBoolean(OrcConf.PARALLEL_FOOTER_READ.getAttribute(), true);

    CountingInputFormat format = new CountingInputFormat();
    List<InputSplit> splits = format.getSplits(getJobContext());
    // The unreadable file is left to the task that needs it, where it fails the
    // job just as it did before.
    assertEquals(2, splits.size());
    assertEquals(1, format.prefetched.size());
    assertEquals(2, format.prefetched.get(0).intValue());

    // The file whose footer could be read is still readable from end to end.
    for (InputSplit split : splits) {
      if (((FileSplit) split).getPath().equals(good)) {
        int rows = 0;
        try (RecordReader<NullWritable, OrcStruct> reader =
                 format.createRecordReader(split, getAttemptContext())) {
          while (reader.nextKeyValue()) {
            rows += 1;
          }
        }
        assertEquals(1000, rows);
      }
    }
  }

  @Test
  public void testSplitOfOneFileIsNotPrefetched() throws Exception {
    writeFile(1, 1000);
    conf.setBoolean(OrcConf.PARALLEL_FOOTER_READ.getAttribute(), true);

    CountingInputFormat format = new CountingInputFormat();
    List<InputSplit> splits = format.getSplits(getJobContext());
    assertEquals(1, splits.size());
    assertEquals(1, format.prefetched.size(),
        "prefetchFooters is called, but it skips a single file");
    assertEquals(1, format.prefetched.get(0).intValue());
  }

  @Test
  public void testTasksUseThePrefetchedFooters() throws Exception {
    writeFile(1, 1000);
    writeFile(2, 2000);
    conf.setBoolean(OrcConf.PARALLEL_FOOTER_READ.getAttribute(), true);

    List<InputSplit> splits = new CountingInputFormat().getSplits(getJobContext());
    assertEquals(2, splits.size());

    // Nothing can read a footer out of these files again, so the tasks can
    // only read their rows if they take the footers that were prefetched.
    for (InputSplit split : splits) {
      spoilFooter(((FileSplit) split).getPath());
    }
    assertThrows(FileFormatException.class, () -> {
      try (Reader ignored = OrcFile.createReader(
          ((FileSplit) splits.get(0)).getPath(),
          OrcFile.readerOptions(conf).filesystem(fs))) {
        // a footer read that finds the spoiled file fails here
      }
    }, "the spoiled footer should be unreadable");

    TaskAttemptContext attemptContext = getAttemptContext();
    for (InputSplit split : splits) {
      int rows = 0;
      try (RecordReader<NullWritable, OrcStruct> reader =
               new OrcInputFormat<OrcStruct>().createRecordReader(split,
                   attemptContext)) {
        while (reader.nextKeyValue()) {
          rows += 1;
        }
      }
      Path path = ((FileSplit) split).getPath();
      int file = Integer.parseInt(path.getName().substring("part-".length(),
          path.getName().length() - ".orc".length()));
      assertEquals(1000 * file, rows, "the rows of " + path);
    }
  }
}
