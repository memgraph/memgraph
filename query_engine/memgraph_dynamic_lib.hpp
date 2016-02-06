#pragma once

#include "i_code_cpu.hpp"
#include "dc/dynamic_lib.hpp"

class MemgraphDynamicLib
{
public:
    const static std::string produce_name;
    const static std::string destruct_name;
    using produce = produce_t;
    using destruct = destruct_t;
    using lib_object = ICodeCPU;
};
const std::string MemgraphDynamicLib::produce_name = "produce";
const std::string MemgraphDynamicLib::destruct_name = "destruct";

using CodeLib = DynamicLib<MemgraphDynamicLib>;
