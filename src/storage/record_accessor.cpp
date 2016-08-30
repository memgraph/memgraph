#include "storage/record_accessor.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/edge_record.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/vertex_record.hpp"

// template <class T, class Derived>
// template <class V>
// auto RecordAccessor<T, Derived>::at(type_key_t<T, V> &key) const
// {
//     return properties().template at<V>(key);
// }

template <class T, class Derived>
void RecordAccessor<T, Derived>::remove() const
{
    assert(!empty());

    vlist->remove(record, db.trans);
}

template class RecordAccessor<TypeGroupEdge, EdgeAccessor>;
template class RecordAccessor<TypeGroupVertex, VertexAccessor>;
