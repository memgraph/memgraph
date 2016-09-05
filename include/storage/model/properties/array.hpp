#pragma once

#include <memory>
#include <vector>

#include "storage/model/properties/flags.hpp"

template <class T>
using ArrayStore = std::vector<T>;

template <class T, Flags flag_t>
class Array
{
public:
    const static Type type;

    using Arr = ArrayStore<T>;

    Array(Arr &&value) : data(std::make_shared<Arr>(std::move(value))) {}

    Arr &value() { return *data.get(); }

    Arr const &value() const { return *data.get(); }

    std::ostream &print(std::ostream &stream) const
    {
        stream << "[";
        for (auto e : value()) {
            stream << e << ",";
        }
        stream << "]";
        return stream;
    }

    friend std::ostream &operator<<(std::ostream &stream, const Array &prop)
    {
        return prop.print(stream);
    }

    bool operator==(const Array &other) const { return *this == other.value(); }

    bool operator==(const Arr &other) const
    {
        auto arr = value();
        if (arr.size() != other.size()) {
            return false;
        }

        auto n = arr.size();
        for (size_t i = 0; i < n; i++) {
            if (arr[i] != other[i]) {
                return false;
            }
        }

        return true;
    }

    operator const Arr &() const { return value(); };

private:
    std::shared_ptr<Arr> data;
};

using ArrayString = Array<std::string, Flags::ArrayString>;

using ArrayBool = Array<bool, Flags::ArrayBool>;

using ArrayInt32 = Array<int32_t, Flags::ArrayInt32>;

using ArrayInt64 = Array<int64_t, Flags::ArrayInt64>;

using ArrayFloat = Array<float, Flags::ArrayFloat>;

using ArrayDouble = Array<double, Flags::ArrayDouble>;
