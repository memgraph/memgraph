#pragma once

#include <string>
#include <random>

class RandomString
{
    static constexpr char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

public:
    template <class Rg>
    std::string operator()(Rg&& gen, size_t len)
    {
        auto str = std::string();
        str.reserve(len + 2);
        str.push_back('\'');

        while(str.size() < len)
            str.push_back(charset[rnd(std::forward<Rg>(gen))]);

        str.push_back('\'');
        return str;
    }

private:
    std::uniform_int_distribution<> rnd {0, sizeof(charset) - 1};
};

constexpr char RandomString::charset[];
