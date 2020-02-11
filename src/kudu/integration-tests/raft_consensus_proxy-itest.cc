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
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using kudu::consensus::ConsensusRequestPB;
using kudu::consensus::ConsensusResponsePB;
using kudu::consensus::MakeOpId;
using kudu::consensus::ReplicateMsg;
using kudu::cluster::ExternalTabletServer;
using kudu::cluster::ScopedResumeExternalDaemon;
using kudu::itest::TServerDetails;
using kudu::rpc::RpcController;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

// Test Raft consensus proxying behavior.
class RaftConsensusProxyITest : public ExternalMiniClusterITestBase {};

// Test that we can delete the leader replica while scanning it and still get
// results back.
TEST_F(RaftConsensusProxyITest, ProxyFakeLeader) {
  const int kNumReplicas = 2;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  // FIXME(mpercy): Temporarily disabling encryption / auth due to DNS issues
  // on my dev box.
  vector<string> ts_flags = { "--enable_leader_failure_detection=false", "--rpc_encryption=disabled", "--rpc_authentication=disabled" };
  vector<string> master_flags = { "--catalog_manager_wait_for_new_tablets_to_elect_leader=false", "--rpc_encryption=disabled", "--rpc_authentication=disabled", "--allow_unsafe_replication_factor" };

  NO_FATALS(StartCluster(ts_flags, master_flags, /*num_tablet_servers=*/ kNumReplicas));

  // Create the test table.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kNumReplicas);
  workload.Setup();

  // Determine the generated tablet id.
  ASSERT_OK(inspect_->WaitForReplicaCount(kNumReplicas));
  vector<string> tablets = inspect_->ListTablets();
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0];

  // We will treat the first server as the proxy, the test will act as a fake leader.
  // Replicate 1 no-op to node 1, then proxy that op to node 2 via node 1.
  // Validate that both nodes 1 and 2 received the no-op.

  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, /*minimum_index=*/0));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(ts_map_, &tservers);
  TServerDetails* proxy_ts = tservers[0];
  TServerDetails* downstream_ts = tservers[1];
  SCOPED_TRACE(Substitute("proxy ts uuid: $0", proxy_ts->uuid()));
  SCOPED_TRACE(Substitute("downstream ts uuid: $0", downstream_ts->uuid()));

  const string kFakeLeaderUuid = "fake-leader";
  const int kTerm = 1;

  // Construct and send a replicate message to the proxy, via the proxy to the
  // downstream ts.
  ConsensusRequestPB req;
  req.set_dest_uuid(proxy_ts->uuid());
  req.set_tablet_id(tablet_id);
  req.set_caller_uuid(kFakeLeaderUuid);
  req.set_caller_term(kTerm);
  req.set_all_replicated_index(0);
  req.set_committed_index(0);

  ReplicateMsg* msg = req.mutable_ops()->Add();
  *msg->mutable_id() = MakeOpId(1, 1);
  msg->set_timestamp(0);
  msg->set_op_type(consensus::NO_OP);
  msg->mutable_noop_request()->set_payload_for_tests(kFakeLeaderUuid);

  ConsensusResponsePB resp;
  RpcController controller;
  ASSERT_OK(proxy_ts->consensus_proxy->UpdateConsensus(req, &resp, &controller));
  ASSERT_FALSE(resp.has_error()) << resp.ShortDebugString();
  controller.Reset();

  req.set_dest_uuid(downstream_ts->uuid());
  req.set_proxy_dest_uuid(proxy_ts->uuid());
  msg->set_op_type(consensus::PROXY_OP);
  msg->clear_noop_request();
  ASSERT_OK(proxy_ts->consensus_proxy->UpdateConsensus(req, &resp, &controller));
  ASSERT_FALSE(resp.has_error()) << resp.ShortDebugString();

  // TODO: validate that all the servers have 1.1

  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, /*minimum_index=*/1));
}

} // namespace kudu
