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

#include <utility>

#include "query/typed_value.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/view.hpp"
#include "utils/memory.hpp"

namespace memgraph::query {

/// What a read of a property needs in order to hand the value back the way the query asked for it.
struct PropertyValueShape {
  /// The view the read was made at, which a reference has to be read at too for it to answer what
  /// the read would have.
  storage::View view;
  /// Whether a value the read left as an index id should stay a reference to the record rather than
  /// becoming the floats behind it.
  bool references_wanted;
  storage::NameIdMapper *names;
  utils::MemoryResource *memory;
};

/// The value a read of `property` on `record` yields, and the one place a reference to an indexed
/// property is made.
///
/// Every value a read hands back comes through here, because the two faults that prevents are
/// opposite. A compact read whose values skip this answers with an embedding of nothing, since such
/// a read leaves the floats behind. A read that was not compact and skips this is held whole by
/// every row that keeps it, which is what a reference exists to avoid.
template <class TRecordAccessor, class TPropertyValue>
TypedValue PropertyAsValue(TPropertyValue &&raw, TRecordAccessor const &record, storage::PropertyId property,
                           PropertyValueShape const &shape) {
  if (shape.references_wanted && raw.IsVectorIndexId()) {
    return TypedValue(VectorRefValue{record.impl_, property, shape.view}, shape.memory);
  }
  return TypedValue(std::forward<TPropertyValue>(raw), shape.names, shape.memory);
}

}  // namespace memgraph::query
