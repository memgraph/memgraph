// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/indices/point_index.hpp"

namespace memgraph::storage {

bool PointIndexStorage::CreatePointIndex(LabelId label, PropertyId property,
                                         memgraph::utils::SkipList<Vertex>::Accessor vertices) {
  return indexes_.WithLock([&](index_container_t &indexes) {
    auto key = LabelPropKey{label, property};
    if (indexes.contains(key)) return false;

    auto points_2d_WGS = std::vector<Entry<IndexPointWGS2d>>{};
    auto points_2d_Crt = std::vector<Entry<IndexPointCartesian2d>>{};
    auto points_3d_WGS = std::vector<Entry<IndexPointWGS3d>>{};
    auto points_3d_Crt = std::vector<Entry<IndexPointCartesian3d>>{};

    for (auto const &v : vertices) {
      if (v.deleted) continue;
      if (!utils::Contains(v.labels, label)) continue;
      auto value = v.properties.GetProperty(property);
      switch (value.type()) {
        case PropertyValueType::Point2d: {
          auto val = value.ValuePoint2d();
          if (IsWGS(val.crs())) {
            points_2d_WGS.emplace_back(IndexPointWGS2d{val}, &v);
          } else {
            points_2d_Crt.emplace_back(IndexPointCartesian2d{val}, &v);
          }
          break;
        }
        case PropertyValueType::Point3d: {
          auto val = value.ValuePoint3d();
          if (IsWGS(val.crs())) {
            points_3d_WGS.emplace_back(IndexPointWGS3d{val}, &v);
          } else {
            points_3d_Crt.emplace_back(IndexPointCartesian3d{val}, &v);
          }
          break;
        }
        default:
          continue;
      }
    }
    auto new_index = std::make_shared<PointIndex>(points_2d_WGS, points_2d_Crt, points_3d_WGS, points_3d_Crt);
    auto [_, inserted] = indexes.try_emplace(key, std::move(new_index));
    return inserted;
  });
}

bool PointIndexStorage::DropPointIndex(LabelId label, PropertyId property) {
  return indexes_.WithLock([&](index_container_t &indexes) {
    auto it = indexes.find(LabelPropKey{label, property});
    if (it == indexes.end()) return false;
    indexes.erase(it);
    return true;
  });
}

}  // namespace memgraph::storage
