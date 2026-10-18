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

package org.apache.orc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for reading the footers of many files at once.
 */
public class TestParallelFooterReader {

  private static final Path WORK_DIR = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"),
      TestParallelFooterReader.class.getSimpleName());

  private static final TypeDescription SCHEMA = TypeDescription.fromString("struct<i:int>");

  private Configuration conf;
  private FileSystem fs;

  @BeforeEach
  public void setup() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
  }

  /**
   * Write a file whose footer holds the given number of rows, so that each file
   * of a batch can be told apart from the others.
   * @param file the number of this file
   * @param rows the number of rows to write, or -1 for an empty file
   * @param compression the compression to write it with
   * @return the path of the file
   * @throws IOException if the file can't be written
   */
  private Path writeFile(int file, int rows, CompressionKind compression)
      throws IOException {
    Path path = new Path(WORK_DIR, "file-" + file + ".orc");
    fs.delete(path, false);
    if (rows < 0) {
      // Hive makes empty files and treats them as empty ORC files.
      fs.create(path).close();
      return path;
    }
    try (Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf)
        .setSchema(SCHEMA)
        .compress(compression)
        .bufferSize(4096)
        .stripeSize(64 * 1024))) {
      if (rows > 0) {
        int batchSize = Math.min(1024, rows);
        VectorizedRowBatch batch = SCHEMA.createRowBatch(batchSize);
        LongColumnVector ints = (LongColumnVector) batch.cols[0];
        for (int base = 0; base < rows; base += batchSize) {
          batch.reset();
          int size = Math.min(batchSize, rows - base);
          for (int r = 0; r < size; ++r) {
            ints.vector[r] = base + r + file * 1000;
          }
          batch.size = size;
          writer.addRowBatch(batch);
        }
      }
    }
    return path;
  }

  /**
   * Write out something that is not an ORC file.
   * @param path the file to write
   * @param text the text to put in it
   * @throws IOException if the file can't be written
   */
  private void writeText(Path path, String text) throws IOException {
    fs.delete(path, false);
    try (FSDataOutputStream out = fs.create(path)) {
      out.write(text.getBytes(StandardCharsets.UTF_8));
    }
  }

  /**
   * Read a file the plain way, one file at a time.
   * @param path the file to read
   * @return its tail
   * @throws IOException if the file can't be read
   */
  private OrcTail readSerially(Path path) throws IOException {
    try (Reader reader = OrcFile.createReader(path,
        OrcFile.readerOptions(conf).filesystem(fs))) {
      return ((ReaderImpl) reader).getTail();
    }
  }

  @Test
  public void testParallelTailsMatchSerialTails() throws Exception {
    List<Path> paths = new ArrayList<>();
    paths.add(writeFile(1, 3000, CompressionKind.ZSTD));
    paths.add(writeFile(2, 17, CompressionKind.NONE));
    paths.add(writeFile(3, -1, CompressionKind.ZSTD));
    paths.add(writeFile(4, 100000, CompressionKind.ZLIB));
    paths.add(writeFile(5, 1, CompressionKind.ZSTD));
    paths.add(writeFile(6, 25000, CompressionKind.NONE));

    List<OrcTail> parallel = ParallelFooterReader.readAllTailsInParallel(paths,
        OrcFile.readerOptions(conf).filesystem(fs));

    assertEquals(paths.size(), parallel.size());
    for (int i = 0; i < paths.size(); ++i) {
      OrcTail serial = readSerially(paths.get(i));
      OrcTail actual = parallel.get(i);
      // The tail is the whole of the file's metadata, so it has to agree with
      // the one read a file at a time field for field.
      assertEquals(serial.getFileTail(), actual.getFileTail(),
          "file tail of " + paths.get(i));
      assertEquals(serial.getSerializedTail(), actual.getSerializedTail(),
          "serialized tail of " + paths.get(i));
      assertEquals(serial.getSchema().toString(), actual.getSchema().toString(),
          "schema of " + paths.get(i));
      assertEquals(serial.getFooter().getNumberOfRows(),
          actual.getFooter().getNumberOfRows(), "rows of " + paths.get(i));
      assertEquals(serial.getMetadataOffset(), actual.getMetadataOffset(),
          "metadata offset of " + paths.get(i));
      assertEquals(serial.getStripeStatisticsOffset(),
          actual.getStripeStatisticsOffset(),
          "stripe statistics offset of " + paths.get(i));
      // The results come back in the order of the paths, not in the order in
      // which the files happened to be read.
      assertEquals(fs.getFileStatus(paths.get(i)).getLen(),
          actual.getFileLength(), "length of " + paths.get(i));
    }
  }

  @Test
  public void testTailsCanBuildReaders() throws Exception {
    List<Path> paths = new ArrayList<>();
    for (int file = 1; file <= 4; ++file) {
      paths.add(writeFile(file, 100 * file, CompressionKind.ZSTD));
    }
    List<OrcTail> tails = ParallelFooterReader.readAllTailsInParallel(paths,
        OrcFile.readerOptions(conf).filesystem(fs));
    for (int file = 0; file < paths.size(); ++file) {
      // A tail that was read up front is all that a reader needs, so a scan
      // that read all of the footers at once can still read every file.
      try (Reader reader = OrcFile.createReader(paths.get(file),
          OrcFile.readerOptions(conf).filesystem(fs).orcTail(tails.get(file)))) {
        assertEquals(100 * (file + 1), reader.getNumberOfRows());
        assertEquals(tails.get(file).getFileTail(), reader.getFileTail());
      }
    }
  }

  @Test
  public void testEmptyBatch() throws Exception {
    assertEquals(0, ParallelFooterReader.readAllTailsInParallel(
        new ArrayList<>(), OrcFile.readerOptions(conf)).size());
    assertEquals(0, ParallelFooterReader.createAllReadersInParallel(
        new ArrayList<>(), OrcFile.readerOptions(conf)).size());
  }

  @Test
  public void testReadersInParallel() throws Exception {
    List<Path> paths = new ArrayList<>();
    for (int file = 1; file <= 4; ++file) {
      paths.add(writeFile(file, 500 * file, CompressionKind.ZSTD));
    }
    List<Reader> readers =
        ParallelFooterReader.createAllReadersInParallel(paths,
            OrcFile.readerOptions(conf).filesystem(fs));
    try {
      assertEquals(paths.size(), readers.size());
      for (int file = 0; file < readers.size(); ++file) {
        assertEquals(500 * (file + 1), readers.get(file).getNumberOfRows());
      }
    } finally {
      for (Reader reader : readers) {
        reader.close();
      }
    }
  }

  @Test
  public void testOneBadFileDoesNotHideTheRest() throws Exception {
    Path good1 = writeFile(1, 500, CompressionKind.ZSTD);
    Path bad1 = new Path(WORK_DIR, "bad-1.orc");
    Path good2 = writeFile(2, 600, CompressionKind.ZSTD);
    Path bad2 = new Path(WORK_DIR, "bad-2.orc");
    writeText(bad1, "this is not an ORC file at all");
    writeText(bad2, "nor is this one");
    List<Path> paths = List.of(good1, bad1, good2, bad2);

    CountingFileSystem counter = new CountingFileSystem(fs, 0);
    ParallelFooterReader.FooterReadException error =
        assertThrows(ParallelFooterReader.FooterReadException.class, () -> {
          ParallelFooterReader.readAllTailsInParallel(paths,
              OrcFile.readerOptions(conf).filesystem(counter));
        });
    // Both bad files are reported, not just the first one to fail.
    assertEquals(2, error.getSuppressed().length);
    assertTrue(error.getMessage().contains(bad1.toString()),
        "message should name bad1: " + error.getMessage());
    assertTrue(error.getMessage().contains(bad2.toString()),
        "message should name bad2: " + error.getMessage());
    // The good files were read rather than skipped once a file had failed.
    assertEquals(paths.size(), counter.getOpens(),
        "every file should be attempted");
  }

  @Test
  public void testAllFilesBad() throws Exception {
    Path bad = new Path(WORK_DIR, "all-bad.orc");
    writeText(bad, "not an ORC file");
    ParallelFooterReader.FooterReadException error =
        assertThrows(ParallelFooterReader.FooterReadException.class, () -> {
          ParallelFooterReader.readAllTailsInParallel(List.of(bad, bad),
              OrcFile.readerOptions(conf).filesystem(fs));
        });
    assertEquals(2, error.getSuppressed().length);
  }

  @Test
  public void testFilesAreReadConcurrently() throws Exception {
    List<Path> paths = new ArrayList<>();
    for (int file = 1; file <= 4; ++file) {
      paths.add(writeFile(file, 400 * file, CompressionKind.ZSTD));
    }
    // Every thread has to reach the barrier of opens below, so the read only
    // finishes if four footers really are being read at the same time.
    CountingFileSystem counter = new CountingFileSystem(fs, 4);
    List<OrcTail> tails = ParallelFooterReader.readAllTailsInParallel(paths,
        OrcFile.readerOptions(conf).filesystem(counter));
    assertEquals(paths.size(), tails.size());
    assertEquals(4, counter.getMaxConcurrent());
  }

  @Test
  public void testParallelismIsCapped() throws Exception {
    List<Path> paths = new ArrayList<>();
    for (int file = 1; file <= 6; ++file) {
      paths.add(writeFile(file, 400 * file, CompressionKind.ZSTD));
    }

    // One file at a time.
    Configuration serial = new Configuration(conf);
    serial.setInt(OrcConf.FOOTER_READ_PARALLELISM.getAttribute(), 1);
    CountingFileSystem one = new CountingFileSystem(fs, 0);
    assertEquals(paths.size(), ParallelFooterReader.readAllTailsInParallel(
        paths, OrcFile.readerOptions(serial).filesystem(one)).size());
    assertEquals(1, one.getMaxConcurrent(),
        "one thread reads one footer at a time");

    // Two at a time: the barrier of two opens has to be reached three times.
    Configuration pair = new Configuration(conf);
    pair.setInt(OrcConf.FOOTER_READ_PARALLELISM.getAttribute(), 2);
    CountingFileSystem two = new CountingFileSystem(fs, 2);
    assertEquals(paths.size(), ParallelFooterReader.readAllTailsInParallel(
        paths, OrcFile.readerOptions(pair).filesystem(two)).size());
    assertEquals(2, two.getMaxConcurrent(),
        "two threads read two footers at a time");

    // A caller that hands over its own pool gets that pool's threads, and the
    // pool is left running because the caller owns it.
    Configuration many = new Configuration(conf);
    many.setInt(OrcConf.FOOTER_READ_PARALLELISM.getAttribute(), 8);
    ExecutorService pool = Executors.newFixedThreadPool(3);
    CountingFileSystem three = new CountingFileSystem(fs, 3);
    try {
      assertEquals(paths.size(), ParallelFooterReader.readAllTailsInParallel(
          paths, OrcFile.readerOptions(many).filesystem(three), pool).size());
      assertEquals(3, three.getMaxConcurrent(),
          "the caller's pool sets the cap");
      assertFalse(pool.isShutdown(), "the caller's pool should still be running");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testGivenOptionsAreNotWrittenTo() throws Exception {
    List<Path> paths = new ArrayList<>();
    for (int file = 1; file <= 4; ++file) {
      paths.add(writeFile(file, 300 * file, CompressionKind.ZSTD));
    }
    OrcFile.ReaderOptions options =
        OrcFile.readerOptions(conf).filesystem(fs);
    List<OrcTail> tails =
        ParallelFooterReader.readAllTailsInParallel(paths, options);
    assertEquals(paths.size(), tails.size());
    // A reader writes the filesystem and the tail it found back into the
    // options it was given, so a set of options that is shared by the threads
    // has to come back untouched.
    assertNull(options.getOrcTail());
    assertEquals(fs, options.getFilesystem());
    for (int file = 0; file < paths.size(); ++file) {
      assertEquals(fs.getFileStatus(paths.get(file)).getLen(),
          tails.get(file).getFileLength());
    }
  }

  /**
   * A filesystem that counts the files that are read at once and, when asked
   * for a party, holds each reader inside it until that many have arrived.
   */
  private static class CountingFileSystem extends FilterFileSystem {
    private final AtomicInteger opens = new AtomicInteger();
    private final AtomicInteger concurrent = new AtomicInteger();
    private final AtomicInteger maxConcurrent = new AtomicInteger();
    private final CyclicBarrier barrier;

    CountingFileSystem(FileSystem fs, int parties) {
      super(fs);
      this.barrier = parties > 0 ? new CyclicBarrier(parties) : null;
    }

    @Override
    public Configuration getConf() {
      return fs.getConf();
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
      maxConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
      opens.incrementAndGet();
      try {
        if (barrier != null) {
          barrier.await(60, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted", e);
      } catch (BrokenBarrierException | TimeoutException e) {
        // The caller is reading the files serially, which the tests check by
        // looking at how many were open at once.
      } finally {
        concurrent.decrementAndGet();
      }
      return super.open(path, bufferSize);
    }

    int getOpens() {
      return opens.get();
    }

    int getMaxConcurrent() {
      return maxConcurrent.get();
    }
  }
}
