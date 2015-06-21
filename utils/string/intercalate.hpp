#ifndef MEMGRAPH_UTILS_INTERCALATE_HPP
#define MEMGRAPH_UTILS_INTERCALATE_HPP

#include <sstream>
#include <string>

namespace utils
{

template <typename It>
std::string intercalate(It first, It last,
                        const std::string& separator)
{
    if(first == last)
        return "";

    std::stringstream ss;
    It second(first);

    // append the first N-1 elements with a separator
    for(second++; second != last; ++first, ++second)
        ss << *first << separator;

    // append the last element
    ss << *first;

    return ss.str();
}

}

#endif
