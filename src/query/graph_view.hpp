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

#pragma once

#include <string>
#include <string_view>
#include <utility>

#include "query/db_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::query {

// A range over the vertices yielded by one scan of a GraphView. The boundary is
// crossed once to obtain the range; iteration is then over concrete elements, so
// the real-graph read path is not regressed per row.
//
// Today the range wraps the real accessor's VerticesIterable and yields
// VertexAccessor. It is a distinct type (not VerticesIterable) so a projection
// view can later widen what it yields without changing the operators that scan
// through it.
class VertexRange final {
  VerticesIterable iterable_;

 public:
  explicit VertexRange(VerticesIterable iterable) : iterable_(std::move(iterable)) {}

  class Iterator final {
    VerticesIterable::Iterator it_;

   public:
    explicit Iterator(VerticesIterable::Iterator it) : it_(std::move(it)) {}

    VertexAccessor operator*() const { return *it_; }

    Iterator &operator++() {
      ++it_;
      return *this;
    }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return it_ != other.it_; }
  };

  Iterator begin() { return Iterator(iterable_.begin()); }

  Iterator end() { return Iterator(iterable_.end()); }
};

// The single graph abstraction the read operators run against: scan the vertices
// and map names to/from ids. Property reads and edge expansion stay on the
// element (VertexAccessor / VirtualNode), not here.
//
// The real DbAccessor is the identity view (DbAccessorGraphView); a subgraph or
// projection is a view layered over it. ExecutionContext binds the ambient view,
// and a `CALL { USE ... }` scope rebinds it for the block.
class GraphView {
 public:
  GraphView() = default;
  GraphView(const GraphView &) = default;
  GraphView(GraphView &&) = default;
  GraphView &operator=(const GraphView &) = default;
  GraphView &operator=(GraphView &&) = default;
  virtual ~GraphView() = default;

  // Full scan of the view's vertices. No index/label overload: inside a
  // projection scope a label-filtered match is a full scan plus a filter, since
  // a projection exposes no index.
  virtual VertexRange Vertices(storage::View view) = 0;

  virtual storage::LabelId NameToLabel(std::string_view name) = 0;
  virtual const std::string &LabelToName(storage::LabelId label) const = 0;
  virtual storage::PropertyId NameToProperty(std::string_view name) = 0;
  virtual const std::string &PropertyToName(storage::PropertyId prop) const = 0;
  virtual storage::EdgeTypeId NameToEdgeType(std::string_view name) = 0;
  virtual const std::string &EdgeTypeToName(storage::EdgeTypeId type) const = 0;
};

// The identity view: the real graph seen as a GraphView. Every call forwards to
// the underlying DbAccessor, so a real-graph scan through this view produces the
// same vertices as scanning the accessor directly.
class DbAccessorGraphView final : public GraphView {
  DbAccessor *dba_;

 public:
  explicit DbAccessorGraphView(DbAccessor *dba) : dba_(dba) {}

  VertexRange Vertices(storage::View view) override { return VertexRange{dba_->Vertices(view)}; }

  storage::LabelId NameToLabel(std::string_view name) override { return dba_->NameToLabel(name); }

  const std::string &LabelToName(storage::LabelId label) const override { return dba_->LabelToName(label); }

  storage::PropertyId NameToProperty(std::string_view name) override { return dba_->NameToProperty(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const override { return dba_->PropertyToName(prop); }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) override { return dba_->NameToEdgeType(name); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const override { return dba_->EdgeTypeToName(type); }
};

}  // namespace memgraph::query
