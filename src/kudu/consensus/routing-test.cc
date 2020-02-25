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

#include "kudu/consensus/routing.h"

#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "kudu/consensus/consensus-test-util.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;

namespace kudu {
namespace consensus {

TEST(RoutingTest, TestRoutingTable) {
  RaftConfigPB config = BuildRaftConfigPBForTests(/*num_voters=*/6);
  for (int i = 0; i < config.peers_size(); i++) {
    auto* peer = config.mutable_peers(i);
    const string& uuid = peer->permanent_uuid();
    if (uuid == "peer-1") peer->set_proxy_from("peer-0");
    if (uuid == "peer-3") peer->set_proxy_from("peer-2");
    if (uuid == "peer-4") peer->set_proxy_from("peer-3");
    if (uuid == "peer-5") peer->set_proxy_from("peer-3");
  }

  // Specify a leader that has a parent (proxy_from).
  string leader_uuid = "peer-3";

  RoutingTable route;
  ASSERT_OK(route.Init(config, leader_uuid));

  ASSERT_EQ("peer-5", route.NextHop("peer-3", "peer-5"));
  ASSERT_EQ("peer-0", route.NextHop("peer-3", "peer-1"));
  ASSERT_EQ("peer-3", route.NextHop("peer-5", "peer-1"));
  ASSERT_EQ("peer-3", route.NextHop("peer-2", "peer-4"));
}

} // namespace consensus
} // namespace kudu
