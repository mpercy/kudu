// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Utilities for retrieving and creating temp directories.
 */
public class TempDirUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TempDirUtils.class);

  /**
   * Match the C++ MiniCluster test functionality for overriding the tmp directory used.
   * See MakeClusterRoot in src/kudu/tools/tool_action_test.cc.
   * If the TEST_TMPDIR environment variable is defined that directory will be used
   * instead of the default temp directory.
   *
   * @param prefix a directory name to be created, in environment variable TEST_TMPDIR if defined,
   *               else within the java.io.tmpdir system property
   * @return temp directory as a file
   * @throws IOException if a temp directory cannot be created
   */
  public static File makeTempDirectory(String prefix) throws IOException {
    String testTmpdir = System.getenv("TEST_TMPDIR");
    if (testTmpdir != null) {
      LOG.info("Using the temp directory defined by TEST_TMPDIR: " + testTmpdir);
      return Files.createTempDirectory(Paths.get(testTmpdir), prefix).toFile();
    }
    return Files.createTempDirectory(prefix).toFile();
  }

  /**
   * Recursively remove the specified directory.
   * @param dir directory root to recursively delete
   * @throws IOException if there is an error deleting the directory
   */
  public static void rmTree(Path dir) throws IOException {
    Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
          throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc)
          throws IOException {
        if (exc != null) throw exc;
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Register a JVM shutdown hook to recursively delete the specified directory on JVM shutdown.
   * @param path directory to delete on shutdown
   */
  public static void registerToRecursivelyDeleteOnShutdown(Path path) {
    final Path absPath = path.toAbsolutePath();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        if (!absPath.toFile().exists()) return;
        try {
          rmTree(absPath);
        } catch (IOException exc) {
          LOG.warn("Unable to remove directory tree " + absPath.toString(), exc);
        }
      }
    });
  }
}
