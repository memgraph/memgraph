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

#include <functional>
#include <string>
#include <unordered_map>

#include "storage/v2/view.hpp"
#include "utils/memory.hpp"

namespace query {

class DbAccessor;
class TypedValue;

namespace {
const char kStartsWith[] = "STARTSWITH";
const char kEndsWith[] = "ENDSWITH";
const char kContains[] = "CONTAINS";
const char kId[] = "ID";
}  // namespace

struct FunctionContext {
  DbAccessor *db_accessor;
  utils::MemoryResource *memory;
  int64_t timestamp;
  std::unordered_map<std::string, int64_t> *counters;
  storage::View view;
};

/// Return the function implementation with the given name.
///
/// Note, returned function signature uses C-style access to an array to allow
/// having an array stored anywhere the caller likes, as long as it is
/// contiguous in memory. Since most functions don't take many arguments, it's
/// convenient to have them stored in the calling stack frame.
std::function<TypedValue(const TypedValue *arguments, int64_t num_arguments, const FunctionContext &context)>
NameToFunction(const std::string &function_name);

}  // namespace query
