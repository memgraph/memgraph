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

#include "query/procedure/mg_procedure_helpers.hpp"
#include "query/procedure/fmt.hpp"

namespace memgraph::query::procedure {
MgpUniquePtr<mgp_value> GetStringValueOrSetError(const char *string, mgp_memory *memory, mgp_result *result) {
  procedure::MgpUniquePtr<mgp_value> value{nullptr, mgp_value_destroy};
  const auto success =
      TryOrSetError([&] { return procedure::CreateMgpObject(value, mgp_value_make_string, string, memory); }, result);
  if (!success) {
    value.reset();
  }

  return value;
}

bool InsertResultOrSetError(mgp_result *result, mgp_result_record *record, const char *result_name, mgp_value *value) {
  if (const auto err = mgp_result_record_insert(record, result_name, value); err != mgp_error::MGP_ERROR_NO_ERROR) {
    const auto error_msg = fmt::format("Unable to set the result for {}, error = {}", result_name, err);
    static_cast<void>(mgp_result_set_error_msg(result, error_msg.c_str()));
    return false;
  }

  return true;
}

}  // namespace memgraph::query::procedure
