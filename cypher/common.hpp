#pragma once

#include "utils/command_line/arguments.hpp"
#include "utils/string/filereader.hpp"

std::string extract_query(const vector_str& arguments)
{
    if (contain_argument(arguments, "-q"))
        return get_argument(arguments, "-q", "CREATE (n) RETURN n");
    auto default_file = "query.cypher";
    auto file = get_argument(arguments, "-f", default_file);
    // TODO: error handling
    return read_file(file.c_str());
}

