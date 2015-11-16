#pragma once

#include "xorshift128plus.hpp"
#include "utils/likely.hpp"

template <size_t N, class R=Xorshift128plus>
class FastBinomial
{
    // fast binomial draws coin tosses from a single generated random number
    // let's draw a random 4 bit number and count trailing ones
    //
    // 1  0000 -> 1 =
    // 2  0001 -> 2 ==      8 x =     p = 8/16 = 1/2
    // 3  0010 -> 1 =       4 x ==    p = 4/16 = 1/4     p_total = 15/16
    // 4  0011 -> 3 ===     2 x ===   p = 2/16 = 1/8
    // 5  0100 -> 1 =       1 x ====  p = 1/16 = 1/16
    // 6  0101 -> 2 ==     --------------------------
    // 7  0110 -> 1 =       1 x ===== p = 1/16 invalid value, retry!
    // 8  0111 -> 4 ====
    // 9  1000 -> 1 =
    // 10 1001 -> 2 ==
    // 11 1010 -> 1 =
    // 12 1011 -> 3 ===
    // 13 1100 -> 1 =
    // 14 1101 -> 2 ==
    // 15 1110 -> 1 =
    // ------------------
    // 16 1111 -> 5 =====

    static constexpr uint64_t mask = (1 << N) - 1;

public:
    FastBinomial() = default;

    unsigned operator()()
    {
        while(true)
        {
            // couting trailing ones is equal to counting trailing zeros
            // since the probability for both is 1/2 and we're going to
            // count zeros because they are easier to work with

            // generate a random number
            auto x = random() & mask;

            // if we have all zeros, then we have an invalid case and we
            // need to generate again
            if(UNLIKELY(!x))
                continue;

            // ctzl = count trailing zeros from long
            //        ^     ^        ^          ^
            return __builtin_ctzl(x) + 1;
        }
    }

private:
    R random;
};
