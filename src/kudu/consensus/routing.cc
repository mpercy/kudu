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

#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>

#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/locks.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

using google::protobuf::util::MessageDifferencer;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace consensus {

Status RoutingTable::Init(const RaftConfigPB& raft_config,
                          const ProxyGraphPB& proxy_graph,
                          const std::string& leader_uuid) {
  unordered_map<string, Node*> index;
  unordered_map<string, unique_ptr<Node>> trees;

  RETURN_NOT_OK(ConstructForest(raft_config, proxy_graph, &index, &trees));
  RETURN_NOT_OK(MergeTrees(leader_uuid, index, &trees));
  ConstructNextHopIndicesRec(trees.begin()->second.get());

  index_ = std::move(index);
  topology_root_ = std::move(trees.begin()->second);

  return Status::OK();
}

Status RoutingTable::ConstructForest(
    const RaftConfigPB& raft_config,
    const ProxyGraphPB& proxy_graph,
    std::unordered_map<std::string, Node*>* index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* trees) {

  // Key the graph by peer_uuid.
  unordered_map<string, ProxyEdgePB> dest_to_edge;
  for (const auto& edge : proxy_graph.proxy_edges()) {
    DCHECK(!ContainsKey(dest_to_edge, edge.peer_uuid())) << edge.peer_uuid();
    InsertOrDie(&dest_to_edge, edge.peer_uuid(), edge);
  }

  // Construct the peers.
  for (const RaftPeerPB& peer : raft_config.peers()) {
    boost::optional<ProxyEdgePB> opt_edge;
    const ProxyEdgePB* edge = FindOrNull(dest_to_edge, peer.permanent_uuid());
    if (edge) opt_edge = *edge;

    unique_ptr<Node> hop(new Node(peer, opt_edge));
    index->emplace(peer.permanent_uuid(), hop.get());
    auto result = trees->emplace(peer.permanent_uuid(), std::move(hop));
    if (!result.second) {
      return Status::InvalidArgument("invalid config: duplicate uuid",
                                     peer.permanent_uuid());
    }
  }
  // Organize the peers into a forest of trees.
  for (const RaftPeerPB& peer : raft_config.peers()) {
    const ProxyEdgePB* edge = FindOrNull(dest_to_edge, peer.permanent_uuid());
    if (!edge || edge->proxy_from_uuid().empty()) {
      continue;
    }

    // Node has a parent so we must link them and assign ownership to the
    // parent.
    const string& parent_uuid = edge->proxy_from_uuid();
    Node* parent_ptr = FindWithDefault(*index, parent_uuid, nullptr);
    if (!parent_ptr) {
      return Status::InvalidArgument("invalid config: cannot find proxy",
                                     parent_uuid);
    }

    // Move out of trees map and into parent as a child.
    const string& node_uuid = peer.permanent_uuid();
    auto iter = trees->find(node_uuid);
    DCHECK(iter != trees->end());
    unique_ptr<Node> node = std::move(iter->second);
    trees->erase(iter->first);
    node->parent = parent_ptr;
    auto result = parent_ptr->children.emplace(node_uuid, std::move(node));
    if (!result.second) {
      return Status::InvalidArgument("invalid config: duplicate uuid",
                                     node_uuid);
    }
  }
  return Status::OK();
}

Status RoutingTable::MergeTrees(
    const std::string& leader_uuid,
    const std::unordered_map<std::string, Node*>& index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* trees) {
  Node* leader = FindWithDefault(index, leader_uuid, nullptr);
  if (!leader) {
    return Status::InvalidArgument("invalid config: cannot find leader",
                                   leader_uuid);
  }

  // Find the ultimate proxy root of the leader, if the leader as a proxy
  // assigned to it.
  Node* source_root = leader;
  while (source_root->parent) {
    source_root = source_root->parent;
  }

  // Make all trees, except the one the leader is in, children of the leader.
  // The result is a single tree.
  auto iter = trees->begin();
  while (iter != trees->end()) {
    if (iter->first == source_root->id()) {
      ++iter;
      continue;
    }
    const string& child_uuid = iter->first;
    iter->second->parent = leader;
    leader->children.emplace(child_uuid, std::move(iter->second));
    iter = trees->erase(iter);
  }

  DCHECK_EQ(1, trees->size());
  return Status::OK();
}

void RoutingTable::ConstructNextHopIndicesRec(Node* cur) {
  for (const auto& child_entry : cur->children) {
    const string& child_uuid = child_entry.first;
    const auto& child = child_entry.second;
    ConstructNextHopIndicesRec(child.get());
    // Absorb child routes.
    for (const auto& child_route : child->routes) {
      const string& dest_uuid = child_route.first;
      cur->routes.emplace(dest_uuid, child_uuid);
    }
  }
  // Add self-route as a base case.
  cur->routes.emplace(cur->id(), cur->id());
}

Status RoutingTable::NextHop(const string& src_uuid,
                             const string& dest_uuid,
                             string* next_hop) const {
  Node* src = FindWithDefault(index_, src_uuid, nullptr);
  if (!src) {
    return Status::InvalidArgument(Substitute("unknown source uuid: $0", src_uuid));
  }
  Node* dest = FindWithDefault(index_, dest_uuid, nullptr);
  if (!dest) {
    return Status::InvalidArgument(Substitute("unknown destination uuid: $0", dest_uuid));
  }

  // Search children.
  string* next_uuid = FindOrNull(src->routes, dest_uuid);
  if (next_uuid) {
    *next_hop = *next_uuid;
    return Status::OK();
  }

  // If we can't route via a child, route via a parent.
  DCHECK(src->parent);
  *next_hop = src->parent->id();
  return Status::OK();
}

Status DurableRoutingTable::UpdateProxyGraph(ProxyGraphPB proxy_graph) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  // Rebuild the routing table.
  RoutingTable routing_table;
  if (leader_uuid_) {
    RETURN_NOT_OK(routing_table.Init(raft_config_, proxy_graph, *leader_uuid_));
  }

  // Only flush the proxy graph protobuf to disk when it changes.
  if (!MessageDifferencer::Equals(proxy_graph, proxy_graph_)) {
    RETURN_NOT_OK(Flush());
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock.cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  proxy_graph_ = std::move(proxy_graph);

  if (leader_uuid_) {
    routing_table_ = std::move(routing_table);
  } else {
    routing_table_ = boost::none;
  }

  return Status::OK();
}

Status DurableRoutingTable::UpdateRaftConfig(RaftConfigPB raft_config) {
  //bool leader_in_config = IsRaftConfigMember(leader_uuid, raft_config);

  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  // Rebuild the routing table.
  RoutingTable routing_table;
  bool leader_in_config = false;
  if (leader_uuid_) {
    leader_in_config = IsRaftConfigMember(*leader_uuid_, raft_config);
  }
  if (leader_in_config) {
    RETURN_NOT_OK(routing_table.Init(raft_config, proxy_graph_, *leader_uuid_));
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock.cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  raft_config_ = std::move(raft_config);

  if (leader_in_config) {
    routing_table_ = std::move(routing_table);
  } else {
    routing_table_ = boost::none;
  }

  return Status::OK();
}

Status DurableRoutingTable::UpdateLeader(string leader_uuid) {
  // Take the write lock (does not block readers) and do the slow stuff here.
  lock_.WriteLock();
  auto release_write_lock = MakeScopedCleanup([&] { lock_.WriteUnlock(); });

  RoutingTable routing_table;
  bool leader_in_config = IsRaftConfigMember(leader_uuid, raft_config_);
  if (leader_in_config) {
    // Rebuild the routing table.
    RETURN_NOT_OK(routing_table.Init(raft_config_, proxy_graph_, leader_uuid));
  }

  // Upgrade to an exclusive commit lock and make atomic changes here.
  lock_.UpgradeToCommitLock();
  release_write_lock.cancel(); // Unlocking the commit lock releases the write lock.
  auto release_commit_lock = MakeScopedCleanup([&] { lock_.CommitUnlock(); });

  leader_uuid_ = std::move(leader_uuid);
  if (leader_in_config) {
    routing_table_ = std::move(routing_table);
  } else {
    routing_table_ = boost::none;
  }

  return Status::OK();
}

Status DurableRoutingTable::NextHop(const std::string& src_uuid,
                                    const std::string& dest_uuid,
                                    std::string* next_hop) const {
  shared_lock<RWCLock> l(lock_);
  if (routing_table_) {
    return routing_table_->NextHop(src_uuid, dest_uuid, next_hop);
  }
  if (!IsRaftConfigMember(dest_uuid, raft_config_)) {
    return Status::NotFound(
        Substitute("peer with uuid $0 not found in consensus config", dest_uuid));
  }

  *next_hop = dest_uuid;
  return Status::OK();
}

Status DurableRoutingTable::Flush() const {
  // TODO(mpercy): flush protobuf to disk
  return Status::OK();
}

} // namespace consensus
} // namespace kudu
