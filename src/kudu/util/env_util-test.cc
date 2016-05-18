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

#include "kudu/util/env_util.h"

#include <gflags/gflags.h>
#include <memory>
#include <sys/statvfs.h>

#include "kudu/util/test_util.h"

DECLARE_int64(du_reserved_bytes);
DECLARE_int64(du_reserved_inodes);

DECLARE_int64(du_reserved_bytes_free_for_testing);
DECLARE_int64(du_reserved_inodes_free_for_testing);

namespace kudu {

using std::string;
using std::unique_ptr;

class TestEnvUtil: public KuduTest {
};

TEST_F(TestEnvUtil, TestDiskSpaceCheck) {
  Env* env = Env::Default();
  string path = "/";

  struct statvfs buf;
  ASSERT_OK(env->StatVfs(path, &buf));
  int64_t frag_size = buf.f_frsize;

  Status s = env_util::AssertSufficientDiskSpace(env, path, 0);
  ASSERT_OK(s);

  FLAGS_du_reserved_bytes_free_for_testing = 0;
  FLAGS_du_reserved_bytes = frag_size * 200;
  s = env_util::AssertSufficientDiskSpace(env, path, 0);
  ASSERT_TRUE(s.IsServiceUnavailable());
  ASSERT_STR_CONTAINS(s.ToString(), "Insufficient disk space");
  FLAGS_du_reserved_bytes_free_for_testing = -1;

  FLAGS_du_reserved_bytes = 0;
  FLAGS_du_reserved_inodes = 200;
  FLAGS_du_reserved_inodes_free_for_testing = 0;
  s = env_util::AssertSufficientDiskSpace(env, path, 0);
  ASSERT_TRUE(s.IsServiceUnavailable());
  ASSERT_STR_CONTAINS(s.ToString(), "Insufficient inodes");
}

} // namespace kudu
