#pragma once

// TODO: refactor build state machine instead of ifs

#include "query/backend/cpp_old/handlers/create.hpp"
#include "query/backend/cpp_old/handlers/delete.hpp"
#include "query/backend/cpp_old/handlers/match.hpp"
#include "query/backend/cpp_old/handlers/return.hpp"
#include "query/backend/cpp_old/handlers/set.hpp"
#include "query/backend/cpp_old/handlers/transaction_begin.hpp"
#include "query/backend/cpp_old/handlers/transaction_commit.hpp"
