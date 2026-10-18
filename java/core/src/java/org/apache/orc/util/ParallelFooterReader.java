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
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Read the footers of several files at once instead of one file at a time.
 * <p>
 * Reading a footer is a chain of round trips: ask the filesystem for the file
 * status, read the tail, and parse it. For a scan that touches many files those
 * round trips are serial and each one of them is paid in full, because the
 * footer of one file tells you nothing about the next one. The footers of
 * different files are independent, so reading them concurrently turns a chain
 * of round trips into a handful of rounds.
 * <p>
 * The results come back in the order of the input paths, so the caller can zip
 * them against the files it asked about.
 * <p>
 * A file that fails does not stop the others: every footer is attempted and the
 * failures are reported together as a {@link FooterReadException} once the whole
 * batch has been read.
 *
 * @since 2.2.0
 */
public final class ParallelFooterReader {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelFooterReader.class);

  private ParallelFooterReader() {
    // Not a public class
  }

  /**
   * Read the footers of the given files concurrently.
   * @param paths the files to read
   * @param conf the configuration to use
   * @return the tails in the same order as paths
   * @throws IOException if any of the footers could not be read
   */
  public static List<OrcTail> readAllTailsInParallel(List<Path> paths,
                                                     Configuration conf) throws IOException {
    return readAllTailsInParallel(paths, OrcFile.readerOptions(conf));
  }

  /**
   * Read the footers of the given files concurrently.
   * @param paths the files to read
   * @param options the options to use
   * @return the tails in the same order as paths
   * @throws IOException if any of the footers could not be read
   */
  public static List<OrcTail> readAllTailsInParallel(List<Path> paths,
                                                     OrcFile.ReaderOptions options) throws IOException {
    return readAllTailsInParallel(paths, options, null);
  }

  /**
   * Read the footers of the given files concurrently on the given threads.
   * @param paths the files to read
   * @param options the options to use
   * @param pool the threads to read on or null to use a pool of
   *             {@link OrcConf#FOOTER_READ_PARALLELISM} threads, such as
   *             {@link java.util.concurrent.ForkJoinPool#commonPool()}
   * @return the tails in the same order as paths
   * @throws IOException if any of the footers could not be read
   */
  public static List<OrcTail> readAllTailsInParallel(List<Path> paths,
                                                     OrcFile.ReaderOptions options,
                                                     ExecutorService pool) throws IOException {
    List<Callable<OrcTail>> tasks = new ArrayList<>(paths.size());
    for (Path path : paths) {
      tasks.add(() -> readTail(path, options));
    }
    return runAllInParallel(paths, tasks, options.getConfiguration(), pool);
  }

  /**
   * Open the given files concurrently, reading their footers as it goes.
   * @param paths the files to open
   * @param conf the configuration to use
   * @return the readers in the same order as paths
   * @throws IOException if any of the footers could not be read
   */
  public static List<Reader> createAllReadersInParallel(List<Path> paths,
                                                        Configuration conf) throws IOException {
    return createAllReadersInParallel(paths, OrcFile.readerOptions(conf));
  }

  /**
   * Open the given files concurrently, reading their footers as it goes.
   * @param paths the files to open
   * @param options the options to use
   * @return the readers in the same order as paths
   * @throws IOException if any of the footers could not be read
   */
  public static List<Reader> createAllReadersInParallel(List<Path> paths,
                                                        OrcFile.ReaderOptions options) throws IOException {
    return createAllReadersInParallel(paths, options, null);
  }

  /**
   * Open the given files concurrently on the given threads.
   * @param paths the files to open
   * @param options the options to use
   * @param pool the threads to read on or null to use a pool of
   *             {@link OrcConf#FOOTER_READ_PARALLELISM} threads, such as
   *             {@link java.util.concurrent.ForkJoinPool#commonPool()}
   * @return the readers in the same order as paths
   * @throws IOException if any of the footers could not be read
   */
  public static List<Reader> createAllReadersInParallel(List<Path> paths,
                                                        OrcFile.ReaderOptions options,
                                                        ExecutorService pool) throws IOException {
    List<Callable<Reader>> tasks = new ArrayList<>(paths.size());
    for (Path path : paths) {
      tasks.add(() -> new ReaderImpl(path, copyOf(options)));
    }
    // A reader that is opened but never handed back holds no file handle, since
    // the stripes are only read later, so a failed batch has nothing to close.
    return runAllInParallel(paths, tasks, options.getConfiguration(), pool);
  }

  /**
   * Read one tail and close the file again. The tail holds everything the
   * readers need in memory, so the handle is not needed afterwards.
   * @param path the file to read
   * @param options the options to copy for this file
   * @return the tail of the file
   * @throws IOException if the footer could not be read
   */
  private static OrcTail readTail(Path path,
                                  OrcFile.ReaderOptions options) throws IOException {
    try (ReaderImpl reader = new ReaderImpl(path, copyOf(options))) {
      return reader.getTail();
    }
  }

  /**
   * Make a private copy of the options for one file. The reader writes the
   * filesystem and the tail it found back into the options, so sharing a single
   * object between threads would have them race on it. A tail that was already
   * read is left out, because these methods read one file each.
   * @param options the options to copy
   * @return a copy that no other thread will touch
   */
  private static OrcFile.ReaderOptions copyOf(OrcFile.ReaderOptions options) {
    return new OrcFile.ReaderOptions(options.getConfiguration())
        .filesystem(options.getFilesystem())
        .maxLength(options.getMaxLength())
        .setKeyProvider(options.getKeyProvider())
        .useUTCTimestamp(options.getUseUTCTimestamp())
        .convertToProlepticGregorian(options.getConvertToProlepticGregorian());
  }

  /**
   * Run all of the tasks and collect the results in order.
   * @param paths the files being read, for error messages
   * @param tasks the work, one task per file
   * @param conf the configuration that decides how many threads to use
   * @param pool the caller's threads or null to make our own
   * @param <T> the type of the results
   * @return the results in the same order as the tasks
   * @throws IOException if any of the tasks failed
   */
  private static <T> List<T> runAllInParallel(List<Path> paths,
                                              List<Callable<T>> tasks,
                                              Configuration conf,
                                              ExecutorService pool) throws IOException {
    if (paths.isEmpty()) {
      return new ArrayList<>();
    }
    int parallelism = Math.max(1, OrcConf.FOOTER_READ_PARALLELISM.getInt(conf));
    boolean ownsPool = pool == null;
    ExecutorService threads = ownsPool ? newPool(parallelism) : pool;
    LOG.debug("Reading {} footers with parallelism {}", paths.size(), parallelism);
    try {
      List<Future<T>> futures = new ArrayList<>(tasks.size());
      for (Callable<T> task : tasks) {
        futures.add(threads.submit(task));
      }
      List<T> results = new ArrayList<>(Collections.<T>nCopies(tasks.size(), null));
      List<Path> broken = new ArrayList<>();
      List<IOException> errors = new ArrayList<>();
      boolean interrupted = false;
      // Every footer gets its chance to be read before we complain about any of
      // them, so one bad file does not hide the rest of the batch.
      for (int i = 0; i < futures.size(); ++i) {
        try {
          results.set(i, futures.get(i).get());
        } catch (InterruptedException e) {
          interrupted = true;
          break;
        } catch (ExecutionException e) {
          broken.add(paths.get(i));
          errors.add(asIOException(paths.get(i), e.getCause()));
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
        for (Future<T> future : futures) {
          future.cancel(true);
        }
        throw new IOException("Interrupted while reading the footers of " +
            paths.size() + " files");
      }
      if (!errors.isEmpty()) {
        throw new FooterReadException(broken, errors);
      }
      return results;
    } finally {
      if (ownsPool) {
        threads.shutdownNow();
      }
    }
  }

  /**
   * Build a pool of daemon threads so that a forgotten close can never hold up
   * the exit of the virtual machine.
   * @param parallelism the number of threads to create
   * @return the new pool
   */
  private static ExecutorService newPool(int parallelism) {
    AtomicInteger counter = new AtomicInteger();
    ThreadFactory factory = runnable -> {
      Thread thread = new Thread(runnable,
          "orc-footer-reader-" + counter.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    };
    return Executors.newFixedThreadPool(parallelism, factory);
  }

  /**
   * Wrap a failure from one file so that the message names the file.
   * @param path the file that failed
   * @param cause the failure
   * @return the error to report
   */
  private static IOException asIOException(Path path, Throwable cause) {
    if (cause instanceof IOException) {
      return (IOException) cause;
    }
    return new IOException("Problem reading file footer " + path, cause);
  }

  /**
   * The footers of some of the files in a batch could not be read.
   */
  public static class FooterReadException extends IOException {
    private static final long serialVersionUID = 1L;

    FooterReadException(List<Path> paths, List<IOException> errors) {
      super("Could not read the footers of " + paths.size() + " file(s): " +
          paths);
      for (IOException error : errors) {
        addSuppressed(error);
      }
    }
  }
}
