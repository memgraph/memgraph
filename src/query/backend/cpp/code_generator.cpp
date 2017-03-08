#include "query/backend/cpp/code_generator.hpp"

#include <string>
#include <vector>

#include "query/backend/cpp/named_antlr_tokens.hpp"
#include "utils/assert.hpp"

namespace {
std::string kLogicalOr = "||";
// OpenCypher supports xor operator only for booleans, so we can use binary xor
// instead.
std::string kLogicalXor = "^";
std::string kLogicalAnd = "&&";
std::string kLogicalNot = "!";
std::string kEq = "=";
std::string kNe = "!=";
std::string kLt = "<";
std::string kGt = ">";
std::string kLe = "<=";
std::string kGe = ">=";
std::string kPlus = "+";
std::string kMinus = "-";
std::string kMult = "*";
std::string kDiv = "/";
std::string kMod = "%";
std::string kUnaryMinus = "-";
std::string kUnaryPlus = "";  // No need to generate anything.
}
