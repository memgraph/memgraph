#pragma once

#include "integral.hpp"

class Int32 : public Integral<Int32>
{
public:
    static constexpr Flags type = Flags::Int32;

    Int32(int32_t value) : Integral(Flags::Int32), value(value) {}

    /* friend constexpr bool operator==(const Int32& lhs, const Int32& rhs) */
    /* { */
    /*     return lhs.value == rhs.value; */
    /* } */

    /* friend constexpr bool operator<(const Int32& lhs, const Int32& rhs) */
    /* { */
    /*     return lhs.value < rhs.value; */
    /* } */

    /* friend constexpr bool operator==(const Int32& lhs, int32_t rhs) */
    /* { */
    /*     return lhs.value == rhs; */
    /* } */

    /* friend constexpr bool operator<(const Int32& lhs, int32_t rhs) */
    /* { */
    /*     return lhs.value < rhs; */
    /* } */

    /* friend constexpr bool operator==(int32_t lhs, const Int32& rhs) */
    /* { */
    /*     return lhs == rhs.value; */
    /* } */

    /* friend constexpr bool operator<(int32_t lhs, const Int32& rhs) */
    /* { */
    /*     return lhs < rhs.value; */
    /* } */

    int32_t value;
};

