#pragma once

// TODO: refactor build state machine instead of ifs

#include "query_engine/code_generator/handlers/create.hpp"
#include "query_engine/code_generator/handlers/delete.hpp"
#include "query_engine/code_generator/handlers/match.hpp"
#include "query_engine/code_generator/handlers/return.hpp"
#include "query_engine/code_generator/handlers/set.hpp"
#include "query_engine/code_generator/handlers/transaction_begin.hpp"
#include "query_engine/code_generator/handlers/transaction_commit.hpp"
