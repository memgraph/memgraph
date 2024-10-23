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

#pragma once

#include "storage/v2/point.hpp"

namespace memgraph::storage {

double Haversine(const Point2d &point1, const Point2d &point2);

double Haversine(const Point3d &point1, const Point3d &point2);

double Distance(const Point2d &point1, const Point2d &point2);

double Distance(const Point3d &point1, const Point3d &point2);

bool WithinBBox(const Point2d &point, const Point2d &lower_bound, const Point2d &upper_bound);

bool WithinBBox(const Point3d &point, const Point3d &lower_bound, const Point3d &upper_bound);

}  // namespace memgraph::storage
