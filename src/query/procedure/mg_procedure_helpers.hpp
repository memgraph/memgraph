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

#include <fmt/format.h>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "mg_procedure.h"
#include "query/procedure/fmt.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::procedure {
template <typename TResult, typename TFunc, typename... TArgs>
TResult Call(TFunc func, TArgs... args) {
  static_assert(std::is_trivially_copyable_v<TFunc>);
  static_assert((std::is_trivially_copyable_v<std::remove_reference_t<TArgs>> && ...));
  TResult result{};
  MG_ASSERT(func(args..., &result) == mgp_error::MGP_ERROR_NO_ERROR);
  return result;
}

template <typename TFunc, typename... TArgs>
bool CallBool(TFunc func, TArgs... args) {
  return Call<int>(func, args...) != 0;
}

template <typename TObj>
using MgpRawObjectDeleter = void (*)(TObj *);

template <typename TObj>
using MgpUniquePtr = std::unique_ptr<TObj, MgpRawObjectDeleter<TObj>>;

template <typename TObj, typename TFunc, typename... TArgs>
mgp_error CreateMgpObject(MgpUniquePtr<TObj> &obj, TFunc func, TArgs &&...args) {
  TObj *raw_obj{nullptr};
  const auto err = func(std::forward<TArgs>(args)..., &raw_obj);
  obj.reset(raw_obj);
  return err;
}

template <typename Fun>
[[nodiscard]] bool TryOrSetError(Fun &&func, mgp_result *result) {
  if (const auto err = func(); err == mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE) {
    static_cast<void>(mgp_result_set_error_msg(result, "Not enough memory!"));
    return false;
  } else if (err != mgp_error::MGP_ERROR_NO_ERROR) {
    const auto error_msg = fmt::format("Unexpected error ({})!", err);
    static_cast<void>(mgp_result_set_error_msg(result, error_msg.c_str()));
    return false;
  }
  return true;
}

[[nodiscard]] MgpUniquePtr<mgp_value> GetStringValueOrSetError(const char *string, mgp_memory *memory,
                                                               mgp_result *result);

[[nodiscard]] bool InsertResultOrSetError(mgp_result *result, mgp_result_record *record, const char *result_name,
                                          mgp_value *value);

// Extracts the `data.<property>` fields a specified-property text query references. Boolean structure is
// ignored on purpose: a hit can be produced by any referenced field, so all must be readable —
// over-extraction only over-restricts, whereas missing a field would leak. Empty for a fieldless/blob
// query (search_all/regex), on which the caller falls back to the index's full property set.
inline std::vector<std::string_view> ReferencedTextQueryFields(std::string_view query) {
  constexpr std::string_view kFieldPrefix = "data.";
  const auto is_ident = [](const char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
  };
  std::vector<std::string_view> fields;
  for (auto pos = query.find(kFieldPrefix); pos != std::string_view::npos; pos = query.find(kFieldPrefix, pos)) {
    const auto name_start = pos + kFieldPrefix.size();
    auto name_end = name_start;
    while (name_end < query.size() && is_ident(query[name_end])) ++name_end;
    pos = name_end;
    if (name_end == name_start || name_end >= query.size() || query[name_end] != ':') continue;
    fields.push_back(query.substr(name_start, name_end - name_start));
  }
  return fields;
}
}  // namespace memgraph::query::procedure
