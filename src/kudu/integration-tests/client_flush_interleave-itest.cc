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

#include <memory>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/client-test-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"

using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduUpdate;
using kudu::client::sp::shared_ptr;
using kudu::tablet::TabletPeer;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;
using strings::SubstituteAndAppend;

namespace kudu {

class ClientFlushInterleaveITest : public MiniClusterITestBase {
};

// Attempt to trigger client flush interleaving, resulting in reordered writes
// on the server side. Regression test for KUDU-1767.
TEST_F(ClientFlushInterleaveITest, TestInterleave) {
  const int kNumTabletServers = 1;
  NO_FATALS(StartCluster(kNumTabletServers));

  // Create a tablet so we can write to it.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.Setup();

  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(TestWorkload::kDefaultTableName, &table));
  shared_ptr<KuduSession> session = client_->NewSession();
  ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  {
    // Insert a row with id = 1.
    unique_ptr<KuduInsert> insert(table->NewInsert());
    ASSERT_OK(insert->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(insert->mutable_row()->SetInt32("int_val", -1));
    ASSERT_OK(session->Apply(insert.release()));
    ASSERT_OK(session->Flush());
  }

  // Now update that row many times.
  const int kMaxUpdates = 500;
  vector<unique_ptr<KuduUpdate>> updates;
  updates.reserve(kMaxUpdates);
  for (int i = 0; i < kMaxUpdates; i++) {
    unique_ptr<KuduUpdate> update(table->NewUpdate());
    ASSERT_OK(update->mutable_row()->SetInt32("key", 1));
    ASSERT_OK(update->mutable_row()->SetInt32("int_val", i));
    updates.push_back(std::move(update));
  }
  for (auto& update : updates) {
    ASSERT_OK(session->Apply(update.release()));
    session->FlushAsync(nullptr);
  }
  updates.clear();

  // Do a final synchronous flush and print any errors found.
  Status s = session->Flush();
  if (!s.ok()) {
    vector<KuduError *> errors;
    bool overflowed;
    session->GetPendingErrors(&errors, &overflowed);
    string error_msg = Substitute("Flush error: $0: $1 errors: ",
                                  s.ToString(), errors.size());
    for (auto* error : errors) {
      SubstituteAndAppend(&error_msg, "$0: $1; ",
                          error->failed_op().ToString(), error->status().ToString());
    }
    STLDeleteElements(&errors);
    FAIL() << error_msg;
  }

  // Dump the MVCC history and check that everything was applied in the
  // expected order.
  vector<string> lines;
  vector<scoped_refptr<TabletPeer>> peers;
  ASSERT_EQ(1, cluster_->num_tablet_servers());
  cluster_->mini_tablet_server(0)->server()->tablet_manager()->GetTabletPeers(&peers);
  ASSERT_EQ(1, peers.size());
  ASSERT_OK(peers[0]->tablet()->DebugDump(&lines));
  ASSERT_EQ(4, lines.size());
  for (const auto& line : lines) {
    VLOG(1) << line;
  }

  // First 3 lines are preamble.
  vector<string> pieces = Split(lines[3], "(SET ");
  for (int i = 0; i < pieces.size() - 1; i++) {
    ASSERT_STR_CONTAINS(pieces[i+1], Substitute("int_val=$0)", i));
  }
  ASSERT_EQ(kMaxUpdates + 1, pieces.size());
}

} // namespace kudu
