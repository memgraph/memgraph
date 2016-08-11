#pragma once

#include <cstddef>

namespace bolt
{

namespace config
{
    static constexpr size_t N = 65535; /* chunk size */
    static constexpr size_t C = N + 2; /* end mark */
}

}
