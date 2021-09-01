#pragma once

#include <memory>
#include <utility>

#include "mg_procedure.h"

namespace query::procedure {
template <typename TResult, typename TFunc, typename... TArgs>
TResult Call(TFunc func, TArgs &&...args) {
  TResult result{};
  MG_ASSERT(func(std::forward<TArgs>(args)..., &result) == MGP_ERROR_NO_ERROR);
  return result;
}

template <typename TFunc, typename... TArgs>
bool CallBool(TFunc func, TArgs &&...args) {
  int result{0};
  MG_ASSERT(func(std::forward<TArgs>(args)..., &result) == MGP_ERROR_NO_ERROR);
  return result != 0;
}

template <typename TObj>
using MgpRawObjectDeleter = void (*)(TObj *);

template <typename TObj>
using MgpUniquePtr = std::unique_ptr<TObj, MgpRawObjectDeleter<TObj>>;

template <typename TObj, typename TFunc, typename... TArgs>
mgp_error_code CreateMgpObject(MgpUniquePtr<TObj> &obj, TFunc func, TArgs &&...args) {
  TObj *raw_obj{nullptr};
  const auto err = func(std::forward<TArgs>(args)..., &raw_obj);
  obj.reset(raw_obj);
  return err;
}
}  // namespace query::procedure
