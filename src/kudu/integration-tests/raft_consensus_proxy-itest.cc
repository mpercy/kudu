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

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/internal_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(allow_unsafe_replication_factor);
DECLARE_bool(catalog_manager_wait_for_new_tablets_to_elect_leader);
DECLARE_bool(enable_leader_failure_detection);
DECLARE_bool(raft_enable_multi_hop_proxy_routing);
DECLARE_string(rpc_encryption);
DECLARE_string(rpc_authentication);

using boost::optional;
using kudu::consensus::ConsensusRequestPB;
using kudu::consensus::ConsensusResponsePB;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::MakeOpId;
using kudu::consensus::OpId;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::ReplicateMsg;
using kudu::cluster::ExternalTabletServer;
using kudu::cluster::ScopedResumeExternalDaemon;
using kudu::itest::TServerDetails;
using kudu::rpc::RpcController;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

// Test Raft consensus proxying behavior.
class RaftConsensusProxyITest : public MiniClusterITestBase {
 public:
  RaftConsensusProxyITest() {
    FLAGS_rpc_encryption = "disabled";
    FLAGS_rpc_authentication = "disabled";
    FLAGS_allow_unsafe_replication_factor = true;
  }

 protected:
  // FIXME(mpercy): Temporarily disabling encryption / auth due to DNS issues
  // on my dev box.

  void SendNoOp(string tablet_id, string src, string dest_uuid,
                optional<string> proxy_uuid, OpId opid, string payload);
};

void RaftConsensusProxyITest::SendNoOp(string tablet_id, string src_uuid, string dest_uuid,
                                       optional<string> proxy_uuid, OpId opid, string payload) {

  TServerDetails* dest_ts = FindOrDie(ts_map_, dest_uuid);
  TServerDetails* next_ts = dest_ts;
  if (proxy_uuid) {
    next_ts = FindOrDie(ts_map_, *proxy_uuid);
  }

  // Construct and send a replicate message to the proxy, via the proxy to the
  // downstream ts.
  ConsensusRequestPB req;
  req.set_dest_uuid(std::move(dest_uuid));
  req.set_tablet_id(std::move(tablet_id));
  req.set_caller_uuid(std::move(src_uuid));
  req.set_caller_term(opid.term());
  req.set_all_replicated_index(0);
  req.set_committed_index(0);

  ReplicateMsg* msg = req.mutable_ops()->Add();
  *msg->mutable_id() = std::move(opid);
  msg->set_timestamp(0);
  if (!proxy_uuid) {
    msg->set_op_type(consensus::NO_OP);
    msg->mutable_noop_request()->set_payload_for_tests(payload);
  } else {
    msg->set_op_type(consensus::PROXY_OP);
    req.set_proxy_dest_uuid(*proxy_uuid);
  }

  ConsensusResponsePB resp;
  RpcController controller;
  ASSERT_OK(next_ts->consensus_proxy->UpdateConsensus(req, &resp, &controller));
  ASSERT_FALSE(resp.has_error()) << resp.ShortDebugString();
}

// Test that we can delete the leader replica while scanning it and still get
// results back.
TEST_F(RaftConsensusProxyITest, ProxyFakeLeaderNoRouting) {
  const int kNumReplicas = 2;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  FLAGS_raft_enable_multi_hop_proxy_routing = false;

  NO_FATALS(StartCluster(/*num_tablet_servers=*/ kNumReplicas));
  FLAGS_enable_leader_failure_detection = false;
  FLAGS_catalog_manager_wait_for_new_tablets_to_elect_leader = false;

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
  auto opid = MakeOpId(1, 1);

  NO_FATALS(SendNoOp(tablet_id, kFakeLeaderUuid, proxy_ts->uuid(),
                     /*proxy_uuid=*/ boost::none, opid, /*payload=*/ kFakeLeaderUuid));
  NO_FATALS(SendNoOp(tablet_id, kFakeLeaderUuid, downstream_ts->uuid(),
                     proxy_ts->uuid(), opid, /*payload=*/ kFakeLeaderUuid));
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, /*minimum_index=*/1));
}

// Write another test with a fake leader
TEST_F(RaftConsensusProxyITest, ProxyFakeLeaderWithRouting) {
  // Desired test topology, with A* as the leader:
  //
  //      A* -> B -> C -> D
  //
  const int kNumReplicas = 4;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  NO_FATALS(StartCluster(/*num_tablet_servers=*/ kNumReplicas));
  FLAGS_enable_leader_failure_detection = false;
  FLAGS_catalog_manager_wait_for_new_tablets_to_elect_leader = false;

  // Create the test table.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kNumReplicas);
  workload.Setup();

  // Determine the generated tablet id.
  ASSERT_OK(inspect_->WaitForReplicaCount(kNumReplicas));
  vector<string> tablets = inspect_->ListTablets();
  ASSERT_EQ(1, tablets.size());
  const string& tablet_id = tablets[0];

  const int kLeaderIndex = 0; // first ts is the leader
  TServerDetails* leader = ts_map_[cluster_->mini_tablet_server(kLeaderIndex)->uuid()];
  ASSERT_OK(itest::StartElection(leader, tablet_id, kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, /*minimum_index=*/ 1));

  // Change the toplogy to look like TS0, TS1 -> TS2 -> TS3.
  ConsensusStatePB cstate;
  ASSERT_OK(WaitUntilNoPendingConfig(leader, tablet_id, kTimeout, &cstate));

  RaftConfigPB new_config = cstate.committed_config();
  ASSERT_EQ(4, new_config.peers_size());

  vector<consensus::BulkChangeConfigRequestPB::ConfigChangeItemPB> changes;
  for (int i = 2; i < kNumReplicas; i++) {
    consensus::BulkChangeConfigRequestPB::ConfigChangeItemPB change_pb;
    change_pb.set_type(consensus::MODIFY_PEER);
    change_pb.mutable_peer()->set_permanent_uuid(cluster_->mini_tablet_server(i)->uuid());
    change_pb.mutable_peer()->mutable_attrs()->set_proxy_from(
        cluster_->mini_tablet_server(i - 1)->uuid());
  }
  ASSERT_OK(BulkChangeConfig(leader, tablet_id, changes, kTimeout));

  // Now kill the leader, pretend we are the leader, and proxy messages to the
  // non-leaders.
  string leader_uuid = cluster_->mini_tablet_server(kLeaderIndex)->uuid();
  string proxy_uuid = cluster_->mini_tablet_server(1)->uuid();
  cluster_->mini_tablet_server(kLeaderIndex)->Shutdown();
  for (int i = 1; i < kNumReplicas; i++) {
    string dest_uuid = cluster_->mini_tablet_server(i)->uuid();
    NO_FATALS(SendNoOp(tablet_id, leader_uuid, dest_uuid,
                       proxy_uuid != dest_uuid ? optional<string>(proxy_uuid) : boost::none,
                       MakeOpId(1, 1), /*payload=*/ leader_uuid));
  }

  auto follower_map = ts_map_;
  follower_map.erase(leader_uuid);
  ASSERT_OK(WaitForServersToAgree(kTimeout, follower_map, tablet_id, /*minimum_index=*/1));
}

} // namespace itest
} // namespace kudu
