#include <iostream>

#include "gtest/gtest.h"

#include "query/backend/cpp_old/entity_search.hpp"
#include "utils/assert.hpp"
#include "utils/underlying_cast.hpp"

TEST(CypherStateMachine, Basic)
{
    // initialize cypher state machine
    CypherStateMachine csm;

    // set cost for label index
    auto test_cost = static_cast<uint64_t>(30);
    csm.search_cost("n", entity_search::search_label_index, test_cost);

    // check all costs
    auto max_cost = entity_search::max<uint64_t>();
    permanent_assert(csm.search_cost("n", entity_search::search_internal_id) ==
                         max_cost,
                     "Search internal id cost should be max cost value");
    permanent_assert(csm.search_cost("n", entity_search::search_label_index) ==
                         test_cost,
                     "Search label index cost should be test cost value");
    permanent_assert(
        csm.search_cost("n", entity_search::search_property_index) == max_cost,
        "Search property index should be max cost value");

    // check minimum cost
    permanent_assert(csm.min("n") == entity_search::search_label_index,
                     "Search place should be label index");
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
