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

#include "kudu/util/minidump.h"

#include <glob.h>

#include <fstream>
#include <map>
#include <string>

#include <client/linux/handler/exception_handler.h>
#include <common/linux/linux_libc_support.h>
#include <glog/logging.h>

#include "kudu/gutil/linux_syscall_support.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

using kudu::env_util::CreateDirsRecursively;
using std::ifstream;
using std::multimap;
using std::string;

DECLARE_string(log_dir);

DEFINE_bool(enable_minidumps, true, "Whether to enable minidump collection upon process crash");
TAG_FLAG(enable_minidumps, advanced);
TAG_FLAG(enable_minidumps, runtime);

DEFINE_string(minidump_path, "minidumps", "Directory to write minidump files to. This "
    "can be either an absolute path or a path relative to log_dir. Each daemon will "
    "create an additional sub-directory to prevent naming conflicts and to make it "
    "easier to identify a crashing daemon. Minidump files contain crash-related "
    "information in a compressed format and will only be written when a daemon exits "
    "unexpectedly, for example on an unhandled exception or signal. Set to empty to "
    "disable writing minidump files.");
TAG_FLAG(minidump_path, evolving);

DEFINE_int32(max_minidumps, 9, "Maximum number of minidump files to keep per daemon. "
    "Older files are removed first. Set to 0 to keep all minidump files.");
TAG_FLAG(max_minidumps, evolving);

DEFINE_int32(minidump_size_limit_hint_kb, 20480, "Size limit hint for minidump files in "
    "KB. If a minidump exceeds this value, then breakpad will reduce the stack memory it "
    "collects for each thread from 8KB to 2KB. However it will always include the full "
    "stack memory for the first 20 threads, including the thread that crashed.");
TAG_FLAG(minidump_size_limit_hint_kb, advanced);
TAG_FLAG(minidump_size_limit_hint_kb, evolving);

namespace kudu {

// Breakpad ExceptionHandler. It registers its own signal handlers to write minidump
// files during process crashes, but also can be used to write minidumps directly.
static google_breakpad::ExceptionHandler* minidump_exception_handler = nullptr;

// Called by the exception handler before minidump is produced. Minidump is only written
// if this returns true.
static bool FilterCallback(void* context) {
  return FLAGS_enable_minidumps;
}

// Callback for breakpad. It is called by breakpad whenever a minidump file has been
// written and should not be called directly. It logs the event before breakpad crashes
// the process. Due to the process being in a failed state we write to stdout/stderr and
// let the surrounding redirection make sure the output gets logged. The calls might
// still fail in unknown scenarios as the process is in a broken state. However we don't
// rely on them as the minidump file has been written already.
static bool DumpCallback(const google_breakpad::MinidumpDescriptor& descriptor,
    void* context, bool succeeded) {
  // See if a file was written successfully.
  if (succeeded) {
    // Write message to stdout/stderr, which will usually be captured in the INFO/ERROR
    // log.
    const char msg[] = "Wrote minidump to ";
    const int msg_len = sizeof(msg) / sizeof(msg[0]) - 1;
    const char* path = descriptor.path();
    // We use breakpad's reimplementation of strlen to avoid calling into libc.
    const int path_len = my_strlen(path);
    // We use the linux syscall support methods from chromium here as per the
    // recommendation of the breakpad docs to avoid calling into other shared libraries.
    sys_write(STDOUT_FILENO, msg, msg_len);
    sys_write(STDOUT_FILENO, path, path_len);
    sys_write(STDOUT_FILENO, "\n", 1);
    sys_write(STDERR_FILENO, msg, msg_len);
    sys_write(STDERR_FILENO, path, path_len);
    sys_write(STDERR_FILENO, "\n", 1);
  }
  // Return the value received in the call as described in the minidump documentation. If
  // this values is true, then no other handlers will be called. Breakpad will still crash
  // the process.
  return succeeded;
}

// Signal handler to write a minidump file outside of crashes.
static void HandleSignal(int signal) {
  minidump_exception_handler->WriteMinidump(FLAGS_minidump_path, DumpCallback, nullptr);
}

// Register our signal handler to write minidumps on SIGUSR1.
static void SetupSignalHandler() {
  DCHECK(minidump_exception_handler != nullptr);
  struct sigaction sig_action;
  memset(&sig_action, 0, sizeof(sig_action));
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_handler = &HandleSignal;
  sigaction(SIGUSR1, &sig_action, nullptr);
}

// Check the number of minidump files and removes the oldest ones to maintain an upper
// bound on the number of files.
static void CheckAndRemoveMinidumps(int max_minidumps) {
  // Disable rotation if 0 or wrong input
  if (max_minidumps <= 0) return;

  Env* env = Env::Default();

  // Search for minidumps. There could be multiple minidumps for a single second.
  multimap<int, string> timestamp_to_path;
  // Minidump filenames are created by breakpad in the following format, for example:
  // 7b57915b-ee6a-dbc5-21e59491-5c60a2cf.dmp.
  string pattern = FLAGS_minidump_path + "/*.dmp";
  glob_t result;
  glob(pattern.c_str(), GLOB_TILDE, nullptr, &result);
  for (size_t i = 0; i < result.gl_pathc; ++i) {
    string minidump_file_path(result.gl_pathv[i]);
    if (env->FileExists(minidump_file_path)) {
      ifstream stream(minidump_file_path.c_str(), std::ios::in | std::ios::binary);
      if (!stream.good()) {
        // Error opening file, probably broken, remove it.
        LOG(WARNING) << "Failed to open file " << minidump_file_path << ". Removing it.";
        stream.close();
        // Best effort, ignore error.
        WARN_NOT_OK(env->DeleteFile(minidump_file_path),
                    "Failed to delete file " + minidump_file_path);
        continue;
      }
      // Read minidump header from file.
      MDRawHeader header;
      constexpr int header_size = sizeof(header);
      stream.read(reinterpret_cast<char *>(&header), header_size);
      // Check for minidump header signature and version. We don't need to check for
      // endianness issues here since the file was written on the same machine. Ignore the
      // higher 16 bit of the version as per a comment in the breakpad sources.
      if (stream.gcount() != header_size || header.signature != MD_HEADER_SIGNATURE ||
          (header.version & 0x0000ffff) != MD_HEADER_VERSION) {
        LOG(WARNING) << "Found file in minidump folder, but it does not look like a "
            << "minidump file: " << minidump_file_path << ". Removing it.";
        WARN_NOT_OK(env->DeleteFile(minidump_file_path),
                    "Failed to delete file " + minidump_file_path);
        continue;
      }
      int timestamp = header.time_date_stamp;
      timestamp_to_path.emplace(timestamp, std::move(minidump_file_path));
    }
  }
  globfree(&result);

  // Remove oldest entries until max_minidumps are left.
  if (timestamp_to_path.size() <= max_minidumps) return;
  int files_to_delete = timestamp_to_path.size() - max_minidumps;
  DCHECK_GT(files_to_delete, 0);
  auto to_delete = timestamp_to_path.begin();
  for (int i = 0; i < files_to_delete; i++) {
    const string& minidump_file_path = to_delete->second;
    WARN_NOT_OK(env->DeleteFile(minidump_file_path),
                "Failed to delete file " + minidump_file_path);
    ++to_delete;
  }
}

Status RegisterMinidump(const char* cmd_line_path) {
  // Registration must only be called once.
  static bool registered = false;
  DCHECK(!registered);
  registered = true;

  if (!FLAGS_enable_minidumps || FLAGS_minidump_path.empty()) return Status::OK();

  if (IsRelativePath(FLAGS_minidump_path)) {
    FLAGS_minidump_path = JoinPathSegments(FLAGS_log_dir, FLAGS_minidump_path);
  }

  // Add the executable name to the path where minidumps will be written. This makes
  // identification easier and prevents name collisions between the files.
  string exe_name = BaseName(cmd_line_path);
  FLAGS_minidump_path = JoinPathSegments(FLAGS_minidump_path, exe_name);

  // Create the directory if it is not there. The minidump doesn't get written if there is
  // no directory.
  RETURN_NOT_OK_PREPEND(CreateDirsRecursively(Env::Default(), FLAGS_minidump_path),
    strings::Substitute("Could not create minidump directory $0", FLAGS_minidump_path));

  // Rotate old minidump files. We only need to do this on startup (in contrast to
  // periodically) because only process crashes will trigger the creation of new minidump
  // files.
  CheckAndRemoveMinidumps(FLAGS_max_minidumps);

  google_breakpad::MinidumpDescriptor desc(FLAGS_minidump_path.c_str());

  // Limit filesize if configured.
  if (FLAGS_minidump_size_limit_hint_kb > 0) {
    size_t size_limit = 1024 * static_cast<int64_t>(FLAGS_minidump_size_limit_hint_kb);
    LOG(INFO) << "Setting minidump size limit to " << size_limit << ".";
    desc.set_size_limit(size_limit);
  }

  // Intentionally leaked. We want this to have the lifetime of the process.
  DCHECK(!minidump_exception_handler);
  minidump_exception_handler =
      new google_breakpad::ExceptionHandler(desc, FilterCallback, DumpCallback, nullptr, true, -1);
  ANNOTATE_LEAKING_OBJECT_PTR(minidump_exception_handler);

  // Setup signal handler for SIGUSR1.
  SetupSignalHandler();

  return Status::OK();
}

} // namespace kudu
