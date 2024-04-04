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

#include "slk/serialization.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/concepts.hpp"

namespace memgraph::slk {

void Save(const storage::Gid &gid, slk::Builder *builder);
void Load(storage::Gid *gid, slk::Reader *reader);

void Save(const storage::PropertyValue &value, slk::Builder *builder);
void Load(storage::PropertyValue *value, slk::Reader *reader);

}  // namespace memgraph::slk
