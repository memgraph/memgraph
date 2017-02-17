#pragma once

#include <cstddef>

namespace bolt
{

namespace config
{
    /** chunk size */
    static constexpr size_t N = 65535; 

    /** end mark */
    static constexpr size_t C = N + 2;
}

}
