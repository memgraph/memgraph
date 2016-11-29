#pragma once

#include <string>

namespace utils
{

class StringBuffer
{
public:
    StringBuffer &operator<<(const std::string &str)
    {
        data += str;
        return *this;
    }

    StringBuffer &operator<<(const char *str)
    {
        data += str;
        return *this;
    }

    StringBuffer &operator<<(char c)
    {
        data += c;
        return *this;
    }

    std::string &str() { return data; }

private:
    std::string data;
};

}
