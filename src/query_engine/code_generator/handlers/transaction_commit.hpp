#pragma once

#include "query_engine/code_generator/handlers/includes.hpp"

auto transaction_commit_action = [](CypherStateData &,
                                    const QueryActionData &) -> std::string {
    return LINE(code::transaction_commit) + LINE(code::return_empty_result);
};
