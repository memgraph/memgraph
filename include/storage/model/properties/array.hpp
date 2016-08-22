#pragma once

#include "storage/model/properties/property.hpp"

template <class T, Flags f_type>
class Array : public Property
{
public:
    static constexpr Flags type = f_type;
    using Arr = std::vector<T>;

    Array(const Array &) = default;
    Array(Array &&) = default;

    Array(const Arr &value);
    Array(Arr &&value);

    operator const Arr &() const;

    bool operator==(const Property &other) const override;

    bool operator==(const Array &other) const;

    bool operator==(const Arr &other) const;

    friend std::ostream &operator<<(std::ostream &stream, const Array &prop);

    std::ostream &print(std::ostream &stream) const override;

    Arr const &value_ref() const { return value; }

    Arr value;
};

class ArrayString : public Array<std::string, Flags::ArrayString>
{
public:
    using Array::Array;
};

class ArrayBool : public Array<bool, Flags::ArrayBool>
{
public:
    using Array::Array;
};

class ArrayInt32 : public Array<int32_t, Flags::ArrayInt32>
{
public:
    using Array::Array;
};

class ArrayInt64 : public Array<int64_t, Flags::ArrayInt64>
{
public:
    using Array::Array;
};

class ArrayFloat : public Array<float, Flags::ArrayFloat>
{
public:
    using Array::Array;
};

class ArrayDouble : public Array<double, Flags::ArrayDouble>
{
public:
    using Array::Array;
};
