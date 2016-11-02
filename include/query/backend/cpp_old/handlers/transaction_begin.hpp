#pragma once

#include "includes.hpp"

auto transaction_begin_action = [](CypherStateData &,
                                   const QueryActionData &) -> std::string {
    return code_line(code::transaction_begin);
};
