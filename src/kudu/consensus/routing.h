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

#pragma once

#include <memory>
#include <string>

#include "kudu/consensus/metadata.pb.h"

namespace kudu {

class Status;

namespace consensus {

// A class that calculates the route that a message should take when being
// proxied across a topology, given a Raft config and a leader.
//
// For example, given the following topology, where parents in the tree are
// defined by setting the proxy_from field in the Raft config:
//
//              A            G
//             / \          / \
//            B   C*       H   I
//           / \   \
//          D   E   F
//
// and given that C is the leader, this implementation will assume there is a
// direct route from C to G and thus construct a single-tree topology that
// looks like the following:
//
//               A
//             /   \
//            B     C*
//           / \   / \
//          D   E F   G
//                   / \
//                  H   I
//
// Of course, the route from C to F will be C -> F.
// Similarly, the route from C to I will be C -> G -> I.
// To reach D from C, the route will be C -> A -> B -> D.
// Naturally, the next hop from A to E will be B.
//
// This class is NOT thread-safe and must be externally synchronized..
class RoutingTable {
 public:
  // Initialize the routing table. Safe to call multiple times.
  Status Init(const RaftConfigPB& config, const std::string& leader_uuid);

  // Return the UUID of the next hop, given the UUIDs of the current source
  // and the ultimate destination.
  // TODO(mpercy): Change NextHop() return value to boost::optional so that
  // boost::none will indicate no route found, or return a Status.
  std::string NextHop(const std::string& src_uuid,
                      const std::string& dest_uuid) const;

 private:
  // A node representing a raft peer in a hierarchy with associated routing
  // rules for proxied messages.
  struct Node {
    explicit Node(RaftPeerPB peer_pb) : peer_pb(peer_pb) {}
    const std::string& id() const { return peer_pb.permanent_uuid(); }
    RaftPeerPB peer_pb;
    Node* parent = nullptr;
    // children: child_uuid -> child
    std::unordered_map<std::string, std::unique_ptr<Node>> children;
    // routes: dest -> next_hop
    std::unordered_map<std::string, std::string> routes;
  };

  // Construct a forest of trees that represent the proxy relationships,
  // with non-proxied nodes at the root of each tree.
  Status ConstructForest(
      const RaftConfigPB& config,
      std::unordered_map<std::string, Node*>* index,
      std::unordered_map<std::string, std::unique_ptr<Node>>* trees);

  // Reorganize the trees into a single tree by moving the roots of
  // trees that don't include the leader under the leader as children.
  Status MergeTrees(
      const std::string& leader_uuid,
      const std::unordered_map<std::string, Node*>& index,
      std::unordered_map<std::string, std::unique_ptr<Node>>* trees);

  // Recursively construct the next-hop indices at each node. We run DFS to
  // determine routes because there is only one route to each node from the
  // root.
  void ConstructNextHopIndicesRec(Node* cur);

  std::unique_ptr<Node> topology_root_;
  std::unordered_map<std::string, Node*> index_;
};

}  // namespace consensus
}  // namespace kudu
