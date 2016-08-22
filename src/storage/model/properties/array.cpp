#include "storage/model/properties/array.hpp"

template <class T, Flags f_type>
Array<T, f_type>::Array(const Arr &value) : Property(f_type), value(value)
{
}

template <class T, Flags f_type>
Array<T, f_type>::Array(Arr &&value) : Property(f_type), value(value)
{
}

template <class T, Flags f_type>
Array<T, f_type>::operator const Arr &() const
{
    return value;
}

template <class T, Flags f_type>
bool Array<T, f_type>::operator==(const Property &other) const
{
    return other.is<Array>() && operator==(other.as<Array>());
}

template <class T, Flags f_type>
bool Array<T, f_type>::operator==(const Array &other) const
{
    return this->operator==(other.value);
}

template <class T, Flags f_type>
bool Array<T, f_type>::operator==(const Arr &other) const
{
    if (value.size() != other.size()) {
        return false;
    }

    auto n = value.size();
    for (size_t i = 0; i < n; i++) {
        if (value[i] != other[i]) {
            return false;
        }
    }

    return true;
}

template <class T, Flags f_type>
std::ostream &operator<<(std::ostream &stream, const Array<T, f_type> &prop)
{
    return prop.print(stream);
}

template <class T, Flags f_type>
std::ostream &Array<T, f_type>::print(std::ostream &stream) const
{
    stream << "[";
    for (auto e : value) {
        stream << e << ",";
    }
    stream << "]";
    return stream;
}

template class Array<std::string, Flags::ArrayString>;
template class Array<bool, Flags::ArrayBool>;
template class Array<int32_t, Flags::ArrayInt32>;
template class Array<int64_t, Flags::ArrayInt64>;
template class Array<float, Flags::ArrayFloat>;
template class Array<double, Flags::ArrayDouble>;
