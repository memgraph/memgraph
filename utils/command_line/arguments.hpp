#pragma once

#include <string>
#include <vector>
#include <algorithm>

namespace
{

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"

auto all_arguments(int argc, char *argv[])
{
    return std::vector<std::string>(argv + 1, argv + argc);
}

bool contains_argument(const std::vector<std::string>& all,
                       const std::string& flag)
{
    return std::find(all.begin(), all.end(), flag) != all.end();
}

auto get_argument(const std::vector<std::string>& all,
                  const std::string& flag,
                  const std::string& default_value)
{
    auto it = std::find(all.begin(), all.end(), flag);

    if(it == all.end())
        return default_value;

    return all[std::distance(all.begin(), it) + 1];
}

#pragma clang diagnostic pop

}
