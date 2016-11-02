#pragma once

#include "query/i_plan_cpu.hpp"
#include "dc/dynamic_lib.hpp"

namespace
{

template <typename Stream>
class MemgraphDynamicLib
{
public:
    using produce = produce_t<Stream>;
    using destruct = destruct_t<Stream>;
    using lib_object = IPlanCPU<Stream>;
};

template <typename Stream>
using CodeLib = DynamicLib<MemgraphDynamicLib<Stream>>;

}
