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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.util.ParallelFooterReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An ORC input format that satisfies the org.apache.hadoop.mapreduce API.
 */
public class OrcInputFormat<V extends WritableComparable>
    extends FileInputFormat<NullWritable, V> {

  private static final Logger LOG = LoggerFactory.getLogger(OrcInputFormat.class);

  /** How many prefetched footers this virtual machine will hold for its tasks. */
  private static final int MAX_PREFETCHED_FOOTERS = 128;

  /**
   * The footers that {@link #prefetchFooters} read while the splits were being
   * made. A task running in this virtual machine takes its footer from here
   * instead of asking the filesystem for it a second time. Tasks in another
   * virtual machine never see these, so the entries are dropped as they are
   * used and the map is capped so that it cannot grow without bound.
   */
  private static final Map<String, OrcTail> PREFETCHED =
      Collections.synchronizedMap(new LinkedHashMap<String, OrcTail>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, OrcTail> eldest) {
          return size() > MAX_PREFETCHED_FOOTERS;
        }
      });

  /**
   * Put the given SearchArgument into the configuration for an OrcInputFormat.
   * @param conf the configuration to modify
   * @param sarg the SearchArgument to put in the configuration
   * @param columnNames the list of column names for the SearchArgument
   */
  public static void setSearchArgument(Configuration conf,
                                       SearchArgument sarg,
                                       String[] columnNames) {
    org.apache.orc.mapred.OrcInputFormat.setSearchArgument(conf, sarg,
        columnNames);
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    Configuration conf = job.getConfiguration();
    if (OrcConf.PARALLEL_FOOTER_READ.getBoolean(conf)) {
      prefetchFooters(conf, splits);
    }
    return splits;
  }

  /**
   * Read the footers of the files behind the splits ahead of the tasks.
   * <p>
   * A task needs the footer of its file before it can read a row, and when the
   * files sit on a remote filesystem that footer costs a chain of round trips.
   * The files are independent of each other, so all of the footers are read at
   * once here and kept for the tasks of this virtual machine.
   * <p>
   * This is only a prefetch, so a file that cannot be read here is left to the
   * task that needs it: the job fails, or does not, exactly as it did before.
   * @param conf the configuration to read the files with
   * @param splits the splits that the tasks will read
   */
  protected void prefetchFooters(Configuration conf, List<InputSplit> splits) {
    List<Path> paths = new ArrayList<>();
    for (InputSplit split : splits) {
      Path path = ((FileSplit) split).getPath();
      if (!paths.contains(path)) {
        paths.add(path);
      }
    }
    // One file has nothing to gain from being read beside another one.
    if (paths.size() < 2) {
      return;
    }
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf)
        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf));
    try {
      List<OrcTail> tails =
          ParallelFooterReader.readAllTailsInParallel(paths, options);
      for (int i = 0; i < paths.size(); ++i) {
        // A tail with no modification time came from a reader that was given a
        // length rather than asked for the status, so it does not describe the
        // whole file and a task must not be trusted with it.
        if (tails.get(i).getFileModificationTime() >= 0) {
          PREFETCHED.put(paths.get(i).toString(), tails.get(i));
        }
      }
    } catch (IOException e) {
      LOG.warn("Could not prefetch the footers of " + paths.size() +
          " files; the tasks will read them themselves", e);
    }
  }

  @Override
  public RecordReader<NullWritable, V>
      createRecordReader(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext
                         ) throws IOException, InterruptedException {
    FileSplit split = (FileSplit) inputSplit;
    Configuration conf = taskAttemptContext.getConfiguration();
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf)
        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf));
    OrcTail prefetched = PREFETCHED.remove(split.getPath().toString());
    if (prefetched != null) {
      options.orcTail(prefetched);
    }
    Reader file = OrcFile.createReader(split.getPath(), options);
    //Mapreduce supports selected vector
    Reader.Options rowOptions = org.apache.orc.mapred.OrcInputFormat.buildOptions(
        conf, file, split.getStart(), split.getLength())
        .useSelected(true);
    return new OrcMapreduceRecordReader<>(file, rowOptions);
  }

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> complete = super.listStatus(job);
    List<FileStatus> result = new ArrayList<>(complete.size());
    for(FileStatus stat: complete) {
      if (stat.getLen() != 0) {
        result.add(stat);
      }
    }
    return result;
  }
}
