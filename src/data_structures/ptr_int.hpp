#pragma once

#include <cinttypes>

#include "utils/numerics/log2.hpp"

template <typename PtrT>
struct PointerPackTraits
{
    // here is a place to embed something like platform specific things
    // TODO: cover more cases
    constexpr static int free_bits = utils::log2(alignof(PtrT));

    static auto get_ptr(uintptr_t value) { return (PtrT)(value); }
};

template <typename PtrT, int IntBits, typename IntT = unsigned,
          typename PtrTraits = PointerPackTraits<PtrT>>
class PtrInt
{
private:
    constexpr static int int_shift = PtrTraits::free_bits - IntBits;
    constexpr static uintptr_t ptr_mask =
        ~(uintptr_t)(((intptr_t)1 << PtrTraits::free_bits) - 1);
    constexpr static uintptr_t int_mask =
        (uintptr_t)(((intptr_t)1 << IntBits) - 1);

    uintptr_t value {0};

public:
    PtrInt(PtrT pointer, IntT integer)
    {
        set_ptr(pointer);
        set_int(integer);
    }

    auto set_ptr(PtrT pointer)
    {
        auto integer = static_cast<uintptr_t>(get_int());
        auto ptr = reinterpret_cast<uintptr_t>(pointer);
        value = (ptr_mask & ptr) | (integer << int_shift);
    }

    auto set_int(IntT integer)
    {
        auto ptr = reinterpret_cast<uintptr_t>(get_ptr());
        auto int_shifted = static_cast<uintptr_t>(integer << int_shift);
        value = (int_mask & int_shifted) | ptr;
    }

    auto get_ptr() const { return PtrTraits::get_ptr(value & ptr_mask); }

    auto get_int() const { return (IntT)((value >> int_shift) & int_mask); }
};
