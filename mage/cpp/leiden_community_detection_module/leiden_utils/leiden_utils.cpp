// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "leiden_utils.hpp"
#include <cstdint>
namespace leiden_alg {

// create new intermediary community ids -> nodes that are in community i are children of the new intermediary community
// id
void CreateIntermediaryCommunities(Dendrogram &intermediary_communities,
                                   const std::vector<std::vector<std::uint64_t>> &communities,
                                   std::uint64_t current_level) {
  for (std::uint64_t i = 0; i < communities.size(); i++) {
    const auto new_intermediary_community_id = std::make_shared<IntermediaryCommunityId>(
        IntermediaryCommunityId{.community_id = i, .level = current_level + 1, .parent = nullptr});
    for (const auto &node_id : communities[i]) {
      intermediary_communities[current_level][node_id]->parent = new_intermediary_community_id;
    }
    intermediary_communities[current_level + 1].push_back(new_intermediary_community_id);
  }
}

}  // namespace leiden_alg
