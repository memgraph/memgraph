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

#include "storage/v2/property_disk_store.hpp"

#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <optional>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <utility>

#include "storage/v2/temporal.hpp"
#include "utils/cast.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

PDS *PDS::ptr_ = nullptr;

}  // namespace memgraph::storage
