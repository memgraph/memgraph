#include "utils/string/join.hpp"

namespace utils
{

std::string join(const std::vector<std::string>& strings, const char *separator)
{
    std::ostringstream oss;
    std::copy(strings.begin(), strings.end(),
              std::ostream_iterator<std::string>(oss, separator));
    return oss.str();
}

}
