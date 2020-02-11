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

#include "kudu/gutil/map-util.h"
#include "kudu/util/status.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace consensus {

Status RoutingTable::Init(RaftConfigPB& config,
                          const std::string& leader_uuid) {
  unordered_map<string, Node*> index;
  unordered_map<string, unique_ptr<Node>> trees;

  RETURN_NOT_OK(ConstructForest(config, &index, &trees));
  RETURN_NOT_OK(MergeTrees(leader_uuid, index, &trees));
  ConstructNextHopIndicesRec(trees.begin()->second.get());

  index_ = std::move(index);
  topology_root_ = std::move(trees.begin()->second);

  return Status::OK();
}

Status RoutingTable::ConstructForest(
    RaftConfigPB& config,
    std::unordered_map<std::string, Node*>* index,
    std::unordered_map<std::string, std::unique_ptr<Node>>* trees) {

  // Construct the peers.
  for (const RaftPeerPB& peer : config.peers()) {
    unique_ptr<Node> hop(new Node(peer));
    index->emplace(peer.permanent_uuid(), hop.get());
    auto result = trees->emplace(peer.permanent_uuid(), std::move(hop));
    if (!result.second) {
      return Status::InvalidArgument("invalid config: duplicate uuid",
                                     peer.permanent_uuid());
    }
  }
  // Organize the peers into a forest of trees.
  for (const RaftPeerPB& peer : config.peers()) {
    if (peer.attrs().proxy_from().empty()) {
      continue;
    }

    // Node has a parent so we must link them and assign ownership to the
    // parent.
    const string& parent_uuid = peer.attrs().proxy_from();
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
  Node* source_root = leader;
  while (source_root->parent) {
    source_root = source_root->parent;
  }
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

string RoutingTable::NextHop(const string& src_uuid,
                             const string& dest_uuid) {
  Node* src = FindWithDefault(index_, src_uuid, nullptr);
  Node* dest = FindWithDefault(index_, dest_uuid, nullptr);
  if (!src || !dest) return nullptr; // Does not exist.

  // Search children.
  string* next_uuid = FindOrNull(src->routes, dest_uuid);
  if (next_uuid) {
    return *next_uuid;
  }

  // If we can't route via a child, route via a parent.
  DCHECK(src->parent);
  return src->parent->id();
}

} // namespace consensus
} // namespace kudu
