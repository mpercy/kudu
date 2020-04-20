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

#include <boost/optional/optional.hpp>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/util/rwc_lock.h"

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
  Status Init(const RaftConfigPB& raft_config,
              const ProxyGraphPB& proxy_graph,
              const std::string& leader_uuid);

  // Find the UUID of the next hop, given the UUIDs of the current source
  // and the ultimate destination.
  Status NextHop(const std::string& src_uuid,
                      const std::string& dest_uuid,
                      std::string* next_hop) const;

 private:
  // A node representing a raft peer in a hierarchy with associated routing
  // rules for proxied messages.
  struct Node {
    Node(RaftPeerPB peer_pb, boost::optional<ProxyEdgePB> proxy_edge)
        : peer_pb(peer_pb),
          proxy_edge(proxy_edge) {
    }

    const std::string& id() const {
      return peer_pb.permanent_uuid();
    }

    RaftPeerPB peer_pb;
    boost::optional<ProxyEdgePB> proxy_edge;
    Node* parent = nullptr;
    // children: child_uuid -> child
    std::unordered_map<std::string, std::unique_ptr<Node>> children;
    // routes: dest -> next_hop
    std::unordered_map<std::string, std::string> routes;
  };

  // Construct a forest of trees that represent the proxy relationships,
  // with non-proxied nodes at the root of each tree.
  Status ConstructForest(
      const RaftConfigPB& raft_config,
      const ProxyGraphPB& proxy_graph,
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

// Thread-safe and durable metadata layer on top of RoutingTable. Only keeps
// the ProxyGraphPB durable. Ensures that (at most) a single instance of
// RoutingTable is active at any given moment.
//
//
// TODO(mpercy): Think about the case where consensus has just been initialized
// and we don't know the leader locally yet. In this case, how should we route
// proxied messages? We have at least two options:
//
// 1) Simply route messages "directly".
//
// 2) The receipt of a proxied message triggers an update of the leader, so we
//    block on updating the routing table before asking for a routing decision.
//    Is this an invariant we can always rely on?
//
// Maybe we can support both of the above. Nominally, we route messages for
// unknown senders / receivers directly, but in practice we always update the
// routing table when we get a new message, to avoid flip / flopping in our
// routing decisions.
//
class DurableRoutingTable {
 public:
  // Initialize for the first time and write to disk.
  static Status Create(FsManager* fs_manager,
                       std::string tablet_id,
                       RaftConfigPB raft_config,
                       ProxyGraphPB proxy_graph,
                       std::shared_ptr<DurableRoutingTable>* drt);

  // Read from disk.
  static Status Load(FsManager* fs_manager,
                     std::string tablet_id,
                     RaftConfigPB raft_config,
                     std::shared_ptr<DurableRoutingTable>* drt);

  // Called when the proxy graph changes.
  Status UpdateProxyGraph(ProxyGraphPB proxy_graph);

  // Called when the Raft config changes.
  Status UpdateRaftConfig(RaftConfigPB raft_config);

  // Called when the leader changes.
  Status UpdateLeader(std::string leader_uuid);

  // If the leader is unknown, returns Status::NotInitialized().
  Status NextHop(const std::string& src_uuid,
                 const std::string& dest_uuid,
                 std::string* next_hop) const;

 private:
  DurableRoutingTable(FsManager* fs_manager,
                      std::string tablet_id,
                      ProxyGraphPB proxy_graph,
                      RaftConfigPB raft_config);

  // We flush a new ProxyGraphPB to disk before committing the updated version to memory.
  // This method is not thread-safe and must be synchronized by taking the lock or similar.
  Status Flush() const;

  FsManager* fs_manager_;
  const std::string tablet_id_;

  mutable RWCLock lock_; // read-write-commit lock protecting the below fields
  ProxyGraphPB proxy_graph_;
  RaftConfigPB raft_config_;
  boost::optional<std::string> leader_uuid_; // We don't always know who is leader.
  boost::optional<RoutingTable> routing_table_; // When leader is unknown, the route is undefined.
};

}  // namespace consensus
}  // namespace kudu
