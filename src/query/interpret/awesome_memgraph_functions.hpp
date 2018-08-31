/// @file
#pragma once

#include <vector>

#include "query/typed_value.hpp"

namespace query {

class Context;

namespace {
const char kStartsWith[] = "STARTSWITH";
const char kEndsWith[] = "ENDSWITH";
const char kContains[] = "CONTAINS";
}  // namespace

/// Return the function implementation with the given name.
///
/// Note, returned function signature uses C-style access to an array to allow
/// having an array stored anywhere the caller likes, as long as it is
/// contiguous in memory. Since most functions don't take many arguments, it's
/// convenient to have them stored in the calling stack frame.
std::function<TypedValue(TypedValue *arguments, int64_t num_arguments,
                         Context *context)>
NameToFunction(const std::string &function_name);

}  // namespace query
