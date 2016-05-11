#pragma once

#include <random>
#include <atomic>

#include "replacer.hpp"
#include "random.hpp"

static std::atomic<uint64_t> counter {0};
static thread_local std::mt19937 mt;

class CypherReplacer
{
    std::uniform_int_distribution<uint64_t> random_int;
    RandomString random_string;

public:
    CypherReplacer()
    {
        replacer
            .replace("#", [&]() {
                return std::to_string(random_int(mt) % (counter.load() + 1));
            })
            .replace("^", [&]() {
                return random_string(mt, 15);
            })
            .replace("@", [&]() {
                return std::to_string(counter.fetch_add(1));
            });
    }

    std::string& operator()(std::string& query)
    {
        return replacer(query);
    }

private:
    Replacer replacer;
};
