/// @file
#pragma once

#include <cstdint>
#include <string>

namespace query::frontend {

// These are the functions for parsing literals and parameter names from
// opencypher query.
int64_t ParseIntegerLiteral(const std::string &s);
std::string ParseStringLiteral(const std::string &s);
double ParseDoubleLiteral(const std::string &s);
std::string ParseParameter(const std::string &s);

}  // namespace query::frontend
