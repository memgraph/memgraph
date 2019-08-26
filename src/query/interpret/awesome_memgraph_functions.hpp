/// @file
#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "utils/memory.hpp"

namespace database {
class GraphDbAccessor;
}

namespace query {

class TypedValue;

namespace {
const char kStartsWith[] = "STARTSWITH";
const char kEndsWith[] = "ENDSWITH";
const char kContains[] = "CONTAINS";
}  // namespace

struct FunctionContext {
  database::GraphDbAccessor *db_accessor;
  utils::MemoryResource *memory;
  int64_t timestamp;
  std::unordered_map<std::string, int64_t> *counters;
};

/// Return the function implementation with the given name.
///
/// Note, returned function signature uses C-style access to an array to allow
/// having an array stored anywhere the caller likes, as long as it is
/// contiguous in memory. Since most functions don't take many arguments, it's
/// convenient to have them stored in the calling stack frame.
std::function<TypedValue(const TypedValue *arguments, int64_t num_arguments,
                         const FunctionContext &context)>
NameToFunction(const std::string &function_name);

}  // namespace query
