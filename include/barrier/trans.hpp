#pragma once

#include "barrier/barrier.hpp"

// This is the place for imports from memgraph .hpp
#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "communication/bolt/v1/serialization/record_stream.hpp"
#include "database/db.hpp"
#include "database/db_accessor.hpp"
#include "io/network/socket.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/label/label.hpp"

// This is the place for imports from memgraph .cpp

// **************************** HELPER DEFINES *******************************//
// Creates 8 functions for transformation of refereces between types x and y.
// Where x is a border type.
// DANGEROUS
#define TRANSFORM_REF(x, y)                                                    \
    x &trans(y &l) { return ref_as<x>(l); }                                    \
    x const &trans(y const &l) { return ref_as<x const>(l); }                  \
    y &trans(x &l) { return ref_as<y>(l); }                                    \
    y const &trans(x const &l) { return ref_as<y const>(l); }                  \
    x *trans(y *l) { return ptr_as<x>(l); }                                    \
    x const *trans(y const *l) { return ptr_as<x const>(l); }                  \
    y *trans(x *l) { return ptr_as<y>(l); }                                    \
    y const *trans(x const *l) { return ptr_as<y const>(l); }

// Creates 4 functions for transformation of refereces between types x and y.
// Where x is a sized border type.
// DANGEROUS
#define TRANSFORM_REF_SIZED(x, y)                                              \
    y &trans(x &l) { return ref_as<y>(l._data_ref()); }                        \
    y const &trans(x const &l)                                                 \
    {                                                                          \
        return ref_as<y const>(l._data_ref_const());                           \
    }                                                                          \
    y *trans(x *l) { return ptr_as<y>(&(l->_data_ref())); }                    \
    y const *trans(x const *l)                                                 \
    {                                                                          \
        return ptr_as<y const>(&(l->_data_ref_const()));                       \
    }

// Creates 8 functions for transformation of refereces between types x and y.
// Where both x and y are templates over T. Where x is a border type.
// DANGEROUS
#define TRANSFORM_REF_TEMPLATED(x, y)                                          \
    x &trans(y &l) { return ref_as<x>(l); }                                    \
    template <class T>                                                         \
    x const &trans(y const &l)                                                 \
    {                                                                          \
        return ref_as<const x>(l);                                             \
    }                                                                          \
    template <class T>                                                         \
    y &trans(x &l)                                                             \
    {                                                                          \
        return ref_as<y>(l);                                                   \
    }                                                                          \
    template <class T>                                                         \
    y const &trans(x const &l)                                                 \
    {                                                                          \
        return ref_as<const y>(l);                                             \
    }                                                                          \
    template <class T>                                                         \
    x *trans(y *l)                                                             \
    {                                                                          \
        return ptr_as<x>(l);                                                   \
    }                                                                          \
    template <class T>                                                         \
    const x *trans(const y *l)                                                 \
    {                                                                          \
        return ptr_as<const x>(l);                                             \
    }                                                                          \
    template <class T>                                                         \
    y *trans(x *l)                                                             \
    {                                                                          \
        return ptr_as<y>(l);                                                   \
    }                                                                          \
    template <class T>                                                         \
    const y *trans(const x *l)                                                 \
    {                                                                          \
        return ptr_as<const y>(l);                                             \
    }

// For certain classes trans evaluates into ref which doesnt work for Sized
// constructor. This Move forces type y.
// DANGEROUS
#define MOVE_CONSTRUCTOR_FORCED(x, y)                                          \
    x::x(x &&other) : Sized(value_as<y>(std::move(other))) {}

// Creates constructor x(y) where x is border type and y shpuld be his original
// type.
// DANGEROUS
#define VALID_CONSTRUCTION(x, y)                                               \
    template <>                                                                \
    barrier::x::x(y &&d) : Sized(std::move(d))                                 \
    {                                                                          \
    }

// Creates const constructor x(y) where x is border type and y shpuld be his
// original type.
// DANGEROUS
#define VALID_CONSTRUCTION_CONST(x, y)                                         \
    template <>                                                                \
    barrier::x::x(y const &&d) : Sized(std::move(d))                           \
    {                                                                          \
    }

// Creates value transformation function from original type y to border type x.
// DANGEROUS
#define TRANSFORM_VALUE_ONE_RAW(x, y)                                          \
    x trans(y &&d) { return x(std::move(d)); }

// Creates value transformation function and constructor from original type y to
// border type x.
// DANGEROUS
#define TRANSFORM_VALUE_ONE(x, y)                                              \
    VALID_CONSTRUCTION(x, y)                                                   \
    x trans(y &&d) { return x(std::move(d)); }

// Creates value transformation functions and constructor between border type x
// and original type y. Only mutable values.
// DANGEROUS
#define TRANSFORM_VALUE_MUT(x, y)                                              \
    TRANSFORM_VALUE_ONE(x, y)                                                  \
    y trans(x &&d) { return value_as<y>(std::move(d)); }

// Creates value transformation functions and constructors between border type x
// and original type y.
// DANGEROUS
#define TRANSFORM_VALUE(x, y)                                                  \
    TRANSFORM_VALUE_MUT(x, y)                                                  \
    VALID_CONSTRUCTION_CONST(x, y)                                             \
    const x trans(const y &&d) { return x(std::move(d)); }                     \
    const y trans(const x &&d) { return value_as<const y>(std::move(d)); }

// Duplicates given x to call y with x and ::x
#define DUP(x, y) y(x, ::x)

// ********************** TYPES OF AUTO
using vertex_access_iterator_t =
    decltype(((::DbAccessor *)(std::nullptr_t()))->vertex_access());

using edge_access_iterator_t =
    decltype(((::DbAccessor *)(std::nullptr_t()))->edge_access());

using out_edge_iterator_t =
    decltype(((::VertexAccessor *)(std::nullptr_t()))->out());

using in_edge_iterator_t =
    decltype(((::VertexAccessor *)(std::nullptr_t()))->in());

// ******************** OVERLOADED trans FUNCTIONS.
// This enclosure is the only dangerous part of barrier except of Sized class in
// common.hpp
namespace barrier
{
// Blueprint for valid transformation of references:
// TRANSFORM_REF(, ::);
// template <class T> TRANSFORM_REF_TEMPLATED(<T>,::<T>);
// TODO: Strongest assurance that evertyhing is correct is for all of following
// transformation to use DUP for defining theres transformation. This would mean
// that names of classes exported in barrier and names from real class in
// database are equal.

// ***************** TRANSFORMS of reference and pointers
DUP(Label, TRANSFORM_REF);
DUP(EdgeType, TRANSFORM_REF);
DUP(VertexPropertyFamily, TRANSFORM_REF);
DUP(EdgePropertyFamily, TRANSFORM_REF);
DUP(VertexAccessor, TRANSFORM_REF);
DUP(EdgeAccessor, TRANSFORM_REF);
DUP(Db, TRANSFORM_REF);
DUP(DbAccessor, TRANSFORM_REF);
TRANSFORM_REF(std::vector<label_ref_t>, std::vector<::label_ref_t>);
TRANSFORM_REF(VertexPropertyKey,
              ::VertexPropertyFamily::PropertyType::PropertyFamilyKey);
TRANSFORM_REF(EdgePropertyKey,
              ::EdgePropertyFamily::PropertyType::PropertyFamilyKey);
TRANSFORM_REF(VertexStoredProperty, ::StoredProperty<TypeGroupVertex>);
TRANSFORM_REF(EdgeStoredProperty, ::StoredProperty<TypeGroupEdge>);

// ITERATORS
TRANSFORM_REF(VertexIterator, ::iter::Virtual<const ::VertexAccessor>);
TRANSFORM_REF(EdgeIterator, ::iter::Virtual<const ::EdgeAccessor>);
TRANSFORM_REF(VertexAccessIterator, vertex_access_iterator_t);
TRANSFORM_REF(EdgeAccessIterator, edge_access_iterator_t);
TRANSFORM_REF(OutEdgesIterator, out_edge_iterator_t);
TRANSFORM_REF(InEdgesIterator, in_edge_iterator_t);

template <class T>
TRANSFORM_REF_TEMPLATED(VertexIndex<T>, VertexIndexBase<T>);
template <class T>
TRANSFORM_REF_TEMPLATED(EdgeIndex<T>, EdgeIndexBase<T>);
template <class T>
TRANSFORM_REF_TEMPLATED(RecordStream<T>, ::bolt::RecordStream<T>);

template <class T>
TRANSFORM_REF_TEMPLATED(
    VertexPropertyType<T>,
    ::VertexPropertyFamily::PropertyType::PropertyTypeKey<T>);

template <class T>
TRANSFORM_REF_TEMPLATED(EdgePropertyType<T>,
                        ::EdgePropertyFamily::PropertyType::PropertyTypeKey<T>);

// ****************** TRANSFORMS of value
// Blueprint for valid transformation of value:
// TRANSFORM_VALUE(, ::);
DUP(VertexAccessor, TRANSFORM_VALUE);
DUP(EdgeAccessor, TRANSFORM_VALUE);
TRANSFORM_VALUE(EdgePropertyKey,
                ::EdgePropertyFamily::PropertyType::PropertyFamilyKey);
TRANSFORM_VALUE(VertexPropertyKey,
                ::VertexPropertyFamily::PropertyType::PropertyFamilyKey);
TRANSFORM_VALUE_ONE(VertexAccessIterator, vertex_access_iterator_t);
MOVE_CONSTRUCTOR_FORCED(VertexAccessIterator, vertex_access_iterator_t);
TRANSFORM_VALUE_ONE(EdgeAccessIterator, edge_access_iterator_t);
MOVE_CONSTRUCTOR_FORCED(EdgeAccessIterator, edge_access_iterator_t);
TRANSFORM_VALUE_ONE(OutEdgesIterator, out_edge_iterator_t);
MOVE_CONSTRUCTOR_FORCED(OutEdgesIterator, out_edge_iterator_t);
TRANSFORM_VALUE_ONE(InEdgesIterator, in_edge_iterator_t);
MOVE_CONSTRUCTOR_FORCED(InEdgesIterator, in_edge_iterator_t);
TRANSFORM_VALUE_ONE(VertexIterator, ::iter::Virtual<const ::VertexAccessor>);
MOVE_CONSTRUCTOR_FORCED(VertexIterator,
                        ::iter::Virtual<const ::VertexAccessor>);
TRANSFORM_VALUE_ONE(EdgeIterator, ::iter::Virtual<const ::EdgeAccessor>);
MOVE_CONSTRUCTOR_FORCED(EdgeIterator, ::iter::Virtual<const ::EdgeAccessor>);

template <class T>
TRANSFORM_VALUE_ONE_RAW(
    VertexPropertyType<T>,
    ::VertexPropertyFamily::PropertyType::PropertyTypeKey<T>);
template <class T>
TRANSFORM_VALUE_ONE_RAW(EdgePropertyType<T>,
                        ::EdgePropertyFamily::PropertyType::PropertyTypeKey<T>)

template <class T>
TRANSFORM_VALUE_ONE_RAW(RecordStream<T>, ::bolt::RecordStream<T>)

// ********************* SPECIAL CONSTRUCTORS
#define VertexPropertyType_constructor(x)                                      \
    template <>                                                                \
    template <>                                                                \
    VertexPropertyType<x>::VertexPropertyType(                                 \
        ::VertexPropertyFamily::PropertyType::PropertyTypeKey<x> &&d)          \
        : Sized(std::move(d))                                                  \
    {                                                                          \
    }
INSTANTIATE_FOR_PROPERTY(VertexPropertyType_constructor);

#define EdgePropertyType_constructor(x)                                        \
    template <>                                                                \
    template <>                                                                \
    EdgePropertyType<x>::EdgePropertyType(                                     \
        ::EdgePropertyFamily::PropertyType::PropertyTypeKey<x> &&d)            \
        : Sized(std::move(d))                                                  \
    {                                                                          \
    }
INSTANTIATE_FOR_PROPERTY(EdgePropertyType_constructor);

DbAccessor::DbAccessor(Db &db) : Sized(::DbAccessor(trans(db))) {}
}
