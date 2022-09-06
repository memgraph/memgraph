// Copyright 2022 Memgraph Ltd.
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

#include "storage/v3/bindings/bindings.hpp"

#include "expr/interpret/eval.hpp"
#include "storage/v3/bindings/typed_value.hpp"
#include "storage/v3/conversions.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_store.hpp"
#include "storage/v3/view.hpp"

namespace memgraph::storage::v3 {

inline const auto lam = [](const auto &val) { return memgraph::storage::v3::PropertyToTypedValue<TypedValue>(val); };

using ExpressionEvaluator =
    memgraph::expr::ExpressionEvaluator<TypedValue, EvaluationContext, DbAccessor, storage::v3::View,
                                        storage::v3::LabelId, storage::v3::PropertyStore, decltype(lam),
                                        memgraph::storage::v3::Error>;

}  // namespace memgraph::storage::v3
