#include <iostream>
#include <array>

#include "catch.hpp"

#include "data_structures/skiplist/new_height.hpp"

TEST_CASE("New height distribution must be approx. 1/2 1/4 1/8 ...")
{
    // NEVER forget to do this.
    xorshift::init();

    constexpr int N = 1e8;
    constexpr int max_height = 8;
    
    // 2% is a good margin to start with, no?
    // depends on the max_height and N, beware.
    constexpr double error_margin = 0.02;

    // array to store the number of i-height towers generated
    std::array<int, max_height> heights;
    heights.fill(0);

    // generate a tower and put it in a box with his same-height brothers
    for(int i = 0; i < N; ++i)
        heights[new_height(max_height) - 1]++;

    // evaluate the number of towers in all of the boxes
    for(int i = 1; i < max_height; ++i)
    {
        // compute how much towers should be in this box
        int x = N / (2 << i);

        // the actual number of towers
        int xx = heights[i];

        // relative error
        double relative_error = (double)std::abs(xx - x) / xx;

        // this might fail actually in some cases, especially if N is not big
        // enough. it's probabilistic after all :D
        REQUIRE(relative_error < error_margin);
    }
}
