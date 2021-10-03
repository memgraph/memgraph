// Copyright 2021 Memgraph Ltd.
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

#include "io/network/endpoint.hpp"
#include "slk/serialization.hpp"

namespace slk {

inline void Save(const io::network::Endpoint &endpoint, slk::Builder *builder) {
  slk::Save(endpoint.address_, builder);
  slk::Save(endpoint.port_, builder);
  slk::Save(endpoint.family_, builder);
}

inline void Load(io::network::Endpoint *endpoint, slk::Reader *reader) {
  slk::Load(&endpoint->address_, reader);
  slk::Load(&endpoint->port_, reader);
  slk::Load(&endpoint->family_, reader);
}

}  // namespace slk
