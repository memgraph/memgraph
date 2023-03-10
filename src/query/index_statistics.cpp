// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/index_statistics.hpp"

#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/index_statistics.hpp"
#include "query/stream.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "utils/algorithm.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/temporal.hpp"

namespace {
struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
};

}  // namespace

namespace memgraph::query {

PullPlanIndexStatistics::PullPlanIndexStatistics(DbAccessor *dba)
    : dba_(dba), vertices_iterable_(dba->Vertices(storage::View::OLD)) {}

bool PullPlanIndexStatistics::Pull(AnyStream *stream, std::optional<int> n) {
  CollectStatistics();
  //   CreateLabelPropertyIndicesPullChunk();
  //   CreateVertexPullChunk();
  return true;
}

void PullPlanIndexStatistics::CollectStatistics() {
  if (!indices_info_) {
    indices_info_.emplace(dba_->ListAllIndices());
  }

  using LPIndexPair = std::pair<storage::LabelId, storage::PropertyId>;

  // I think this can all be optimized and put under the indices.hpp
  std::unordered_map<LPIndexPair, std::unordered_map<int64_t, int64_t>, pair_hash> histogram;
  // this data structure saves maximum number of different property values for each pair: label_id-property id that is
  // in the index
  std::unordered_map<LPIndexPair, uint64_t, pair_hash> max_values;

  // iterates over a vector containing pair label_id-property_id
  for (const auto &info : indices_info_->label_property) {
    histogram.insert(std::make_pair(info, std::unordered_map<int64_t, int64_t>()));
    max_values.insert(std::make_pair(info, 0));
  }

  // iterate over vertices
  auto current_iter = vertices_iterable_.begin();

  while (current_iter != vertices_iterable_.end()) {
    const auto &vertex = *current_iter;

    // fetch and check labels and properties
    const auto maybe_labels = vertex.Labels(storage::View::OLD);

    if (maybe_labels.HasError()) {
      continue;
    }

    const auto maybe_props = vertex.Properties(storage::View::OLD);

    if (maybe_props.HasError()) {
      continue;
    }

    for (const auto &label : *maybe_labels) {
      for (const auto &props : *maybe_props) {
        auto key = std::make_pair(label, props.first);

        // if doesn't exist in the histogram continue
        // that's possible because histogram saves only those pairs that are saved in index
        if (histogram.find(key) == histogram.end()) {
          continue;
        }
        // what if the property value isn't integer?
        auto prop_value = props.second.ValueInt();

        // perform simple counting with c++ maps
        // TODO: this can be optimized
        auto &label_property_map = histogram[key];
        if (label_property_map.find(prop_value) == label_property_map.end()) {
          label_property_map[prop_value] = 0;
        }
        label_property_map[prop_value] += 1;

        // update max values
        if (max_values[key] < histogram[key][prop_value]) {
          max_values[key] = histogram[key][prop_value];
        }
      }
    }

    ++current_iter;
  }

  // and then just cache it under the db_acessor structure
  for (const auto &[key, value] : max_values) {
    dba_->SetIndexStats(key.first, key.second, IndexStats{.max_number_of_vertices_with_same_value = value});
  }
}
}  // namespace memgraph::query
