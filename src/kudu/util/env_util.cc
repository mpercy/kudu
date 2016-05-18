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

#include <algorithm>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <sys/statvfs.h>
#include <utility>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/status.h"
#include "kudu/util/flag_tags.h"

DEFINE_int64(du_reserved_bytes, 0,
             "Number of bytes to reserve on each filesystem for non-Kudu usage");
TAG_FLAG(du_reserved_bytes, runtime);
TAG_FLAG(du_reserved_bytes, evolving);

DEFINE_int64(du_reserved_inodes, 0,
             "Number of inodes to reserve on each filesystem for non-Kudu usage");
TAG_FLAG(du_reserved_inodes, runtime);
TAG_FLAG(du_reserved_inodes, evolving);

DEFINE_int64(du_reserved_bytes_free_for_testing, -1,
             "For testing only! Set to number of bytes free on each filesystem. "
             "Set to -1 to disable this test-specific override");
TAG_FLAG(du_reserved_bytes_free_for_testing, runtime);
TAG_FLAG(du_reserved_bytes_free_for_testing, unsafe);

DEFINE_int64(du_reserved_inodes_free_for_testing, -1,
             "For testing only! Set to number of inodes free on each filesystem. "
             "Set to -1 to disable this test-specific override");
TAG_FLAG(du_reserved_inodes_free_for_testing, runtime);
TAG_FLAG(du_reserved_inodes_free_for_testing, unsafe);

DEFINE_string(du_reserved_prefixes_with_bytes_free_for_testing, "",
             "For testing only! Syntax: '/path/a:5,/path/b:7' means a has 5 bytes free, "
             "b has 7 bytes free. Set to empty string to disable this test-specific override.");
TAG_FLAG(du_reserved_prefixes_with_bytes_free_for_testing, runtime);
TAG_FLAG(du_reserved_prefixes_with_bytes_free_for_testing, unsafe);

using std::shared_ptr;
using strings::Substitute;

namespace kudu {
namespace env_util {

Status OpenFileForWrite(Env* env, const string& path,
                        shared_ptr<WritableFile>* file) {
  return OpenFileForWrite(WritableFileOptions(), env, path, file);
}

Status OpenFileForWrite(const WritableFileOptions& opts,
                        Env *env, const string &path,
                        shared_ptr<WritableFile> *file) {
  gscoped_ptr<WritableFile> w;
  RETURN_NOT_OK(env->NewWritableFile(opts, path, &w));
  file->reset(w.release());
  return Status::OK();
}

Status OpenFileForRandom(Env *env, const string &path,
                         shared_ptr<RandomAccessFile> *file) {
  gscoped_ptr<RandomAccessFile> r;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &r));
  file->reset(r.release());
  return Status::OK();
}

Status OpenFileForSequential(Env *env, const string &path,
                             shared_ptr<SequentialFile> *file) {
  gscoped_ptr<SequentialFile> r;
  RETURN_NOT_OK(env->NewSequentialFile(path, &r));
  file->reset(r.release());
  return Status::OK();
}

// If we can parse the flag value, and the flag specifies an override for the
// given path, then override the free bytes to match what is specified in the
// flag. See definition of du_reserved_prefixes_with_bytes_free_for_testing.
static void OverrideBytesFree(const string& path, const string& flag, int64_t* bytes_free) {
  for (const auto& str : strings::Split(flag, ",")) {
    pair<string, string> p = strings::Split(str, ":");
    if (HasPrefixString(path, p.first)) {
      int64_t free_override;
      if (!safe_strto64(p.second.c_str(), p.second.size(), &free_override)) return;
      *bytes_free = free_override;
      return;
    }
  }
}

Status AssertSufficientDiskSpace(Env *env, const std::string& path, int64_t bytes) {
  DCHECK_GE(bytes, 0);
  struct statvfs buf;

  RETURN_NOT_OK(env->StatVfs(path, &buf));
  int64_t bytes_free = buf.f_frsize * buf.f_bavail;
  int64_t inodes_free = buf.f_favail;

  // Allow overriding these values by tests.
  if (PREDICT_FALSE(FLAGS_du_reserved_bytes_free_for_testing > -1)) {
    bytes_free = FLAGS_du_reserved_bytes_free_for_testing;
  }
  if (PREDICT_FALSE(!FLAGS_du_reserved_prefixes_with_bytes_free_for_testing.empty())) {
    OverrideBytesFree(path, FLAGS_du_reserved_prefixes_with_bytes_free_for_testing, &bytes_free);
  }
  if (PREDICT_FALSE(FLAGS_du_reserved_inodes_free_for_testing > -1)) {
    inodes_free = FLAGS_du_reserved_inodes_free_for_testing;
  }

  if (bytes_free - bytes < FLAGS_du_reserved_bytes) {
    return Status::ServiceUnavailable(Substitute("Insufficient disk space to allocate $0 bytes "
                                                 "under path $1 "
                                                 "($2 bytes free vs $3 bytes reserved)",
                                                 bytes, path, bytes_free, FLAGS_du_reserved_bytes));
  }
  if (inodes_free <= FLAGS_du_reserved_inodes) {
    return Status::ServiceUnavailable(Substitute("Insufficient inodes under path $0 "
                                                 "($1 inodes free vs $2 inodes reserved)",
                                                 path, inodes_free, FLAGS_du_reserved_inodes));
  }
  return Status::OK();
}

Status ReadFully(RandomAccessFile* file, uint64_t offset, size_t n,
                 Slice* result, uint8_t* scratch) {

  bool first_read = true;

  int rem = n;
  uint8_t* dst = scratch;
  while (rem > 0) {
    Slice this_result;
    RETURN_NOT_OK(file->Read(offset, rem, &this_result, dst));
    DCHECK_LE(this_result.size(), rem);
    if (this_result.size() == 0) {
      // EOF
      return Status::IOError(Substitute("EOF trying to read $0 bytes at offset $1",
                                        n, offset));
    }

    if (first_read && this_result.size() == n) {
      // If it's the first read, we can return a zero-copy array.
      *result = this_result;
      return Status::OK();
    }
    first_read = false;

    // Otherwise, we're going to have to do more reads and stitch
    // each read together.
    this_result.relocate(dst);
    dst += this_result.size();
    rem -= this_result.size();
    offset += this_result.size();
  }
  DCHECK_EQ(0, rem);
  *result = Slice(scratch, n);
  return Status::OK();
}

Status CreateDirIfMissing(Env* env, const string& path, bool* created) {
  Status s = env->CreateDir(path);
  if (created != nullptr) {
    *created = s.ok();
  }
  return s.IsAlreadyPresent() ? Status::OK() : s;
}

Status CopyFile(Env* env, const string& source_path, const string& dest_path,
                WritableFileOptions opts) {
  gscoped_ptr<SequentialFile> source;
  RETURN_NOT_OK(env->NewSequentialFile(source_path, &source));
  uint64_t size;
  RETURN_NOT_OK(env->GetFileSize(source_path, &size));

  gscoped_ptr<WritableFile> dest;
  RETURN_NOT_OK(env->NewWritableFile(opts, dest_path, &dest));
  RETURN_NOT_OK(dest->PreAllocate(size));

  const int32_t kBufferSize = 1024 * 1024;
  gscoped_ptr<uint8_t[]> scratch(new uint8_t[kBufferSize]);

  uint64_t bytes_read = 0;
  while (bytes_read < size) {
    uint64_t max_bytes_to_read = std::min<uint64_t>(size - bytes_read, kBufferSize);
    Slice data;
    RETURN_NOT_OK(source->Read(max_bytes_to_read, &data, scratch.get()));
    RETURN_NOT_OK(dest->Append(data));
    bytes_read += data.size();
  }
  return Status::OK();
}

ScopedFileDeleter::ScopedFileDeleter(Env* env, std::string path)
    : env_(DCHECK_NOTNULL(env)), path_(std::move(path)), should_delete_(true) {}

ScopedFileDeleter::~ScopedFileDeleter() {
  if (should_delete_) {
    bool is_dir;
    Status s = env_->IsDirectory(path_, &is_dir);
    WARN_NOT_OK(s, Substitute(
        "Failed to determine if path is a directory: $0", path_));
    if (!s.ok()) {
      return;
    }
    if (is_dir) {
      WARN_NOT_OK(env_->DeleteDir(path_),
                  Substitute("Failed to remove directory: $0", path_));
    } else {
      WARN_NOT_OK(env_->DeleteFile(path_),
          Substitute("Failed to remove file: $0", path_));
    }
  }
}

} // namespace env_util
} // namespace kudu
