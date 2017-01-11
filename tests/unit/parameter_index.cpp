#include "gtest/gtest.h"

#include "query/backend/cpp_old/query_action_data.hpp"
#include "utils/assert.hpp"

using ParameterIndexKey::Type::InternalId;
using ParameterIndexKey::Type::Projection;

TEST(ParameterIndexKey, Basic)
{
    std::map<ParameterIndexKey, uint64_t> parameter_index;

    parameter_index[ParameterIndexKey(InternalId, "n1")] = 0;
    parameter_index[ParameterIndexKey(InternalId, "n2")] = 1;

    permanent_assert(parameter_index.size() == 2,
                     "Parameter index size should be 2");
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
