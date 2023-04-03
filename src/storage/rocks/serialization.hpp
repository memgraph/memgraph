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

#pragma once

#include <numeric>
#include <optional>

#include "query/db_accessor.hpp"
#include "slk/serialization.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::slk {

class Encoder final {
 public:
  explicit Encoder(slk::Builder *builder) : builder_(builder) {}

  // too serious, will be used later in the future probably
  void SerializeVertex(const query::VertexAccessor &vertex_acc) {
    // storage::LabelId label = vertex_acc.Labels(storage::View::OLD)->at(0);
    // int num_in_edges = *vertex_acc.InDegree(storage::View::OLD);
    // int num_out_edges = *vertex_acc.OutDegree(storage::View::OLD);
    // slk::Save(vertex_acc.Gid().AsUint(), builder_);
  }

 private:
  slk::Builder *builder_;
};

class Decoder final {
 public:
  explicit Decoder(slk::Reader *reader) : reader_(reader) {}

  storage::Vertex ReadVertex() {
    int64_t id = 1234;
    slk::Load(&id, reader_);
    return {storage::Gid::FromUint(id), nullptr};
  }

 private:
  slk::Reader *reader_;
};

}  // namespace memgraph::slk
