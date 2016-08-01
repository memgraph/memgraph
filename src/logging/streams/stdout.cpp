#include "stdout.hpp"

#include <iostream>
#include <fmt/format.h>

void Stdout::emit(const Log::Record& record)
{
    auto s = fmt::format("{} {:<5} [{}] {}\n", static_cast<std::string>(
                         record.when()), record.level_str(), record.where(),
                         record.text());

    std::cout << s;

    /* fmt::printf("{} {:<5} [{}] {}\n", static_cast<std::string>(record.when()), */
    /*             record.level_str(), record.where(), record.text()); */
}

