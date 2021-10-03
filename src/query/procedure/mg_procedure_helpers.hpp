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

#include <memory>
#include <type_traits>
#include <utility>

#include "mg_procedure.h"

namespace query::procedure {
template <typename TResult, typename TFunc, typename... TArgs>
TResult Call(TFunc func, TArgs... args) {
  static_assert(std::is_trivially_copyable_v<TFunc>);
  static_assert((std::is_trivially_copyable_v<std::remove_reference_t<TArgs>> && ...));
  TResult result{};
  MG_ASSERT(func(args..., &result) == MGP_ERROR_NO_ERROR);
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
}  // namespace query::procedure
