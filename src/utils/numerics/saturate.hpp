#pragma once

#include <type_traits>

namespace num {

constexpr std::size_t size_t_HIGHEST_BIT_SETED =
    ((std::size_t)1) << ((sizeof(std::size_t) * 8) - 1);

std::size_t saturating_add(std::size_t a, std::size_t b);
};
