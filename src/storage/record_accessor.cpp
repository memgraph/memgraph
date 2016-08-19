#include "storage/record_accessor.hpp"

template <class T, class Derived, class vlist_t>
template <class V>
auto RecordAccessor<T, Derived, vlist_t>::at(type_key_t<V> &key) const
{
    return properties().template at<V>(key);
}
