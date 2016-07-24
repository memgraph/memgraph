#include <iostream>

#include "query_engine/code_generator/query_action_data.hpp"
#include "utils/assert.hpp"

using ParameterIndexKey::Type::InternalId;
using ParameterIndexKey::Type::Projection;

auto main() -> int
{
    std::map<ParameterIndexKey, uint64_t> parameter_index; 

    parameter_index[ParameterIndexKey(InternalId, "n1")] = 0;
    parameter_index[ParameterIndexKey(InternalId, "n2")] = 1;

    permanent_assert(parameter_index.size() == 2, "Parameter index size should be 2");

    return 0;
}
