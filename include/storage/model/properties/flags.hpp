#pragma once

#include <cassert>
#include <ostream>
#include <string>
#include <vector>

#include "utils/total_ordering.hpp"
#include "utils/underlying_cast.hpp"

// TODO: Revise this
// NOTE: Why not just count all the types?
enum class Flags : unsigned
{
    // Type       | Mask
    // -----------+----------------------------------------
    // Null       | 0000 0000 0000 0000 0000 0000 0000 0000
    // -----------+----------------------------------------
    // Bool       | 0000 0000 0000 0000 0000 0000 0000 0001
    // + True     | 0000 0000 0000 0000 0000 0000 0000 0011
    // + False    | 0000 0000 0000 0000 0000 0000 0000 0101
    // -----------+----------------------------------------
    // String     | 0000 0000 0000 0000 0000 0000 0000 1000
    // -----------+----------------------------------------
    // Number     | 0000 0000 0000 0000 0000 0000 0001 0000
    // + Integral | 0000 0000 0000 0000 0000 0000 0011 0000
    //  + Int32   | 0000 0000 0000 0000 0000 0000 0111 0000
    //  + Int64   | 0000 0000 0000 0000 0000 0000 1011 0000
    // + Floating | 0000 0000 0000 0000 0000 0001 0001 0000
    //  + Float   | 0000 0000 0000 0000 0000 0011 0001 0000
    //  + Double  | 0000 0000 0000 0000 0000 0101 0001 0000
    // -----------+----------------------------------------
    // Array      | 0000 0000 0000 0000 0001 0000 0000 0000
    // -----------+----------------------------------------

    Null = 0x0,
    Bool = 0x1,

    // TODO remove this two values
    True = 0x2 | Bool,
    False = 0x4 | Bool,

    String = 0x8,

    Number = 0x10,
    Integral = 0x20 | Number,
    Int32 = 0x40 | Integral,
    Int64 = 0x80 | Integral,

    Floating = 0x100 | Number,
    Float = 0x200 | Floating,
    Double = 0x400 | Floating,

    Array = 0x1000,
    ArrayBool = (Bool << 13) | Array,
    ArrayString = (String << 13) | Array,
    ArrayInt32 = (Int32 << 13) | Array,
    ArrayInt64 = (Int64 << 13) | Array,
    ArrayFloat = (Float << 13) | Array,
    ArrayDouble = (Double << 13) | Array,

    type_mask = 0xFFF
};

// Mask to turn flags into type. It passes all bits except 0x2 and 0x4 which
// correspond
// to True and False.
const unsigned flags_equal_mask = 0xFF9;

class Type : public TotalOrdering<Type>
{
public:
    constexpr Type(Flags f) : value(underlying_cast(f) & flags_equal_mask)
    {
        Flags o = Flags(value);
        assert(o == Flags::Null || o == Flags::Bool || o == Flags::String ||
               o == Flags::Int32 || o == Flags::Int64 || o == Flags::Float ||
               o == Flags::Double || o == Flags::Array);
    }

    const std::string to_str()
    {
        switch (flags()) {
        case Flags::Null:
            return "null";
        case Flags::Bool:
            return "bool";
        case Flags::String:
            return "str";
        case Flags::Int32:
            return "int32";
        case Flags::Int64:
            return "int64";
        case Flags::Float:
            return "float";
        case Flags::Double:
            return "double";
        case Flags::Array:
            return "array";
        default:
            assert(false);
            return "err_unknown_type_" + std::to_string(value);
        }
    }

    Flags flags() const { return Flags(value); }

    Type get_type() const { return *this; }

    template <class T>
    bool is() const
    {
        return *this == T::type;
    }

    bool operator==(Flags other) const { return *this == Type(other); }

    bool operator!=(Flags other) const { return *this != Type(other); }

    friend bool operator<(const Type &lhs, const Type &rhs)
    {
        return lhs.value < rhs.value;
    }

    friend bool operator==(const Type &lhs, const Type &rhs)
    {
        return lhs.value == rhs.value;
    }

private:
    const unsigned value; // TODO: turn this to flags
};
