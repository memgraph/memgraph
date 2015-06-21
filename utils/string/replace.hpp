#ifndef MEMGRAPH_UTILS_REPLACE_HPP
#define MEMGRAPH_UTILS_REPLACE_HPP

#include <string>

namespace utils
{

// replaces all occurences of <match> in <src> with <replacement>

std::string replace(std::string src,
                    const std::string& match,
                    const std::string& replacement)
{
    for(size_t pos = src.find(match);
        pos != std::string::npos;
        pos = src.find(match, pos + replacement.size()))
            src.erase(pos, match.length()).insert(pos, replacement);

    return src;
}

}

#endif
