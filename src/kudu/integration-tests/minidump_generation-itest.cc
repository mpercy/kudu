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

#include <string>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/util/logging.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"

using std::string;
using std::vector;

namespace kudu {

// Test the creation of minidumps upon process crash.
class MinidumpGenerationITest : public ExternalMiniClusterITestBase {
 protected:
  void WaitForMinidump(const string& dir, MonoDelta timeout);
};

void MinidumpGenerationITest::WaitForMinidump(const string& dir, MonoDelta timeout) {
  vector<string> children;
  MonoTime deadline = MonoTime::Now() + timeout;
  while (children.size() <= 2 && MonoTime::Now() < deadline) {
    ASSERT_OK(env_->GetChildren(dir, &children));
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  bool found = false;
  for (auto f : children) {
    if (HasSuffixString(f, ".dmp")) {
      LOG(INFO) << "Found minidump at " << JoinPathSegments(dir, f);
      found = true;
      break;
    }
  }
  ASSERT_TRUE(found) << "Unable to find minidump after " << timeout.ToString();
}

TEST_F(MinidumpGenerationITest, TestCreateMinidump) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  // Minidumps are disabled by default in the ExternalMiniCluster.
  NO_FATALS(StartCluster({"--enable_minidumps"}, {"--enable_minidumps"}, 1));

  // Test kudu-tserver.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  string dir = JoinPathSegments(JoinPathSegments(ts->log_dir(), "minidumps"), "kudu-tserver");
  ts->process()->Kill(SIGABRT);
  NO_FATALS(WaitForMinidump(dir, kTimeout));

  // Test kudu-master.
  ExternalMaster* master = cluster_->master();
  dir = JoinPathSegments(JoinPathSegments(master->log_dir(), "minidumps"), "kudu-master");
  master->process()->Kill(SIGABRT);
  NO_FATALS(WaitForMinidump(dir, kTimeout));
  FAIL();
}

} // namespace kudu
