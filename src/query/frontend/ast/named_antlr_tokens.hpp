#pragma once

#include "query/frontend/opencypher/generated/CypherBaseVisitor.h"

using antlropencypher::CypherParser;

// List of unnamed tokens visitor needs to use. This should be reviewed on every
// grammar change since even changes in ordering of rules will cause antlr to
// generate different constants for unnamed tokens.
const auto kReturnAllTokenId = CypherParser::T__4;   // *
const auto kDotsTokenId = CypherParser::T__11;       // ..
const auto kEqTokenId = CypherParser::T__2;          // =
const auto kNeTokenId1 = CypherParser::T__18;        // <>
const auto kNeTokenId2 = CypherParser::T__19;        // !=
const auto kLtTokenId = CypherParser::T__20;         // <
const auto kGtTokenId = CypherParser::T__21;         // >
const auto kLeTokenId = CypherParser::T__22;         // <=
const auto kGeTokenId = CypherParser::T__23;         // >=
const auto kPlusTokenId = CypherParser::T__12;       // +
const auto kMinusTokenId = CypherParser::T__13;      // -
const auto kMultTokenId = CypherParser::T__4;        // *
const auto kDivTokenId = CypherParser::T__14;        // /
const auto kModTokenId = CypherParser::T__15;        // %
const auto kUnaryPlusTokenId = CypherParser::T__12;  // +
const auto kUnaryMinusTokenId = CypherParser::T__13; // -
