#pragma once

#include "includes.hpp"

auto transaction_commit_action = [](CypherStateData &,
                                    const QueryActionData &) -> std::string {
    return code_line(code::transaction_commit) +
           code_line(code::return_true);
};
