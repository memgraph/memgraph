#pragma once

#include <vector>

#include "utils/command_line/arguments.hpp"
#include "utils/string/file.hpp"

auto extract_queries(const std::vector<std::string>& arguments)
{
    std::vector<std::string> queries;

    // load single query
    if (contains_argument(arguments, "-q"))
    {
        queries.emplace_back(get_argument(arguments, "-q", "CREATE (n) RETURN n"));
        return queries;
    }

    // load multiple queries from file
    auto default_file = "queries.cypher";
    auto file = get_argument(arguments, "-f", default_file);
    return utils::read_lines(file.c_str());
}

