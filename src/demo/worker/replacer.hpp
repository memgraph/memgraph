#pragma once

#include <vector>
#include <string>
#include <functional>

class Replacer
{
    struct Rule
    {
        std::string match;
        std::function<std::string()> generator;
    };

public:
    Replacer() {}

    template <class F>
    Replacer& replace(const std::string& match, F&& generator)
    {
        rules.push_back({match, std::forward<F>(generator)});
        return *this;
    }

    std::string& operator()(std::string& str)
    {
        size_t n;

        for(auto& rule : rules)
        {
            while((n = str.find_first_of(rule.match)) != std::string::npos)
                str.replace(n, rule.match.size(), rule.generator());
        }

        return str;
    }

private:
    std::vector<Rule> rules;
};
