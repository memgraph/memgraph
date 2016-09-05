#include "storage/model/properties/array.hpp"

template <class T, Flags flag_t>
const Type Array<T, flag_t>::type = Type(flag_t);

template class Array<std::string, Flags::ArrayString>;

template class Array<bool, Flags::ArrayBool>;

template class Array<int32_t, Flags::ArrayInt32>;

template class Array<int64_t, Flags::ArrayInt64>;

template class Array<float, Flags::ArrayFloat>;

template class Array<double, Flags::ArrayDouble>;
