#pragma once

#include <algorithm>
#include <string>
#include <vector>
#include "utils/option.hpp"

namespace
{

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"

auto all_arguments(int argc, char *argv[])
{
    return std::vector<std::string>(argv + 1, argv + argc);
}

bool contains_argument(const std::vector<std::string> &all,
                       const std::string &flag)
{
    return std::find(all.begin(), all.end(), flag) != all.end();
}

// just returns argument value
auto get_argument(const std::vector<std::string> &all, const std::string &flag,
                  const std::string &default_value)
{
    auto it = std::find(all.begin(), all.end(), flag);

    if (it == all.end()) return default_value;

    return all[std::distance(all.begin(), it) + 1];
}

// removes argument value from the all vector
Option<std::string> take_argument(std::vector<std::string> &all,
                                  const std::string &flag)
{
    auto it = std::find(all.begin(), all.end(), flag);

    if (it == all.end()) return make_option<std::string>();

    auto s = std::string(all[std::distance(all.begin(), it) + 1]);
    it++;
    it++;
    all.erase(std::find(all.begin(), all.end(), flag), it);

    return make_option<std::string>(std::move(s));
}

#pragma clang diagnostic pop
}
