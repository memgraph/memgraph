#pragma once
#include "barrier/barrier.hpp"

// This is the place for imports from memgraph .hpp
#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "database/db.hpp"
#include "database/db_accessor.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/label/label.hpp"

// This is the place for imports from memgraph .cpp
// #include "database/db_accessor.cpp"
#include "storage/vertex_accessor.cpp"

// TODO: Extract trans functions to other hpp for easy including into other
// code.

// **************************** HELPER DEFINES *******************************//
// returns transformed pointer
#define THIS (trans(this))
// Performs call x on transformed border class.
#define HALF_CALL(x) (THIS->x)
// Performs call x on transformed border class and returns transformed output.
#define CALL(x) trans(HALF_CALL(x))

#define TRANSFORM_REF(x, y)                                                    \
    x &trans(y &l) { return ref_as<x>(l); }                                    \
    x const &trans(y const &l) { return ref_as<x const>(l); }                  \
    y &trans(x &l) { return ref_as<y>(l); }                                    \
    y const &trans(x const &l) { return ref_as<y const>(l); }                  \
    x *trans(y *l) { return ptr_as<x>(l); }                                    \
    x const *trans(y const *l) { return ptr_as<x const>(l); }                  \
    y *trans(x *l) { return ptr_as<y>(l); }                                    \
    y const *trans(x const *l) { return ptr_as<y const>(l); }

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
#define DESTRUCTOR(x, y)                                                       \
    x::~x() { HALF_CALL(~y()); }
#define COPY_CONSTRUCTOR_MUT(x, y)                                             \
    x::x(x &other) : Sized(y(trans(other))) {}
#define COPY_CONSTRUCTOR(x, y)                                                 \
    x::x(const x &other) : Sized(y(trans(other))) {}
#define MOVE_CONSTRUCTOR(x)                                                    \
    x::x(x &&other) : Sized(trans(std::move(other))) {}
#define MOVE_CONST_CONSTRUCTOR(x)                                              \
    x::x(x const &&other) : Sized(trans(std::move(other))) {}

// For certain classes trans evaluates into ref which doesnt work for Sized
// constructor. This Move forces type.
#define MOVE_CONSTRUCTOR_FORCED(x, y)                                          \
    x::x(x &&other) : Sized(value_as<y>(std::move(other))) {}

#define COPY_OPERATOR(x)                                                       \
    x &x::operator=(const x &other)                                            \
    {                                                                          \
        HALF_CALL(operator=(trans(other)));                                    \
        return *this;                                                          \
    }
#define MOVE_OPERATOR(x)                                                       \
    x &x::operator=(x &&other)                                                 \
    {                                                                          \
        HALF_CALL(operator=(trans(std::move(other))));                         \
        return *this;                                                          \
    }

#define VALID_CONSTRUCTION(x, y)                                               \
    template <>                                                                \
    barrier::x::x(y &&d) : Sized(std::move(d))                                 \
    {                                                                          \
    }

#define VALID_CONSTRUCTION_CONST(x, y)                                         \
    template <>                                                                \
    barrier::x::x(y const &&d) : Sized(std::move(d))                           \
    {                                                                          \
    }

// Generates transformation function from original class to border class.
#define TRANSFORM_VALUE_ONE_RAW(x, y)                                          \
    x trans(y &&d) { return x(std::move(d)); }

// Generates transformation function from original class to border class.
#define TRANSFORM_VALUE_ONE(x, y)                                              \
    VALID_CONSTRUCTION(x, y)                                                   \
    x trans(y &&d) { return x(std::move(d)); }

// Generates transformation functions between border class x and original class
// y by value. Only mutable values.
#define TRANSFORM_VALUE_MUT(x, y)                                              \
    TRANSFORM_VALUE_ONE(x, y)                                                  \
    y trans(x &&d) { return value_as<y>(std::move(d)); }

// Generates transformation functions between border class x and original class
// y by value.
#define TRANSFORM_VALUE(x, y)                                                  \
    TRANSFORM_VALUE_MUT(x, y)                                                  \
    VALID_CONSTRUCTION_CONST(x, y)                                             \
    const x trans(const y &&d) { return x(std::move(d)); }                     \
    const y trans(const x &&d) { return value_as<const y>(std::move(d)); }

// Duplicates given first name to call second given with name and ::name
#define DUP(x, y) y(x, ::x)

// ********************** TYPES OF AUTO
using vertex_access_iterator_t =
    decltype(((::DbAccessor *)(std::nullptr_t()))->vertex_access());

using out_edge_iterator_t =
    decltype(((::VertexAccessor *)(std::nullptr_t()))->out());

using in_edge_iterator_t =
    decltype(((::VertexAccessor *)(std::nullptr_t()))->in());

// This file should contain all implementations of methods from barrier classes
// defined in barrier.hpp.
// Implementations should follow the form:
// border_return_type border_class::method_name(arguments){
//      return
//      CALL(method_name(trans(arguments)))/HALF_CALL(method_name(trans(arguments)));
// }

// ********************** ALL VALID CONVERSIONS ***************************** //
// Implementations must use exclusivly trans functions to cast between types.

// ******************** OVERLOADED trans FUNCTIONS.
// This enclosure is the only dangerous part of barrier except of Sized class in
// common.hpp
namespace barrier
{
// Blueprint for valid transformation of references:
// TRANSFORM_REF(, ::);
// template <class T> TRANSFORM_REF_TEMPLATED(<T>,::<T>);

// ***************** TRANSFORMS of reference
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
TRANSFORM_REF(VertexIterator,
              std::unique_ptr<IteratorBase<const ::VertexAccessor>>);
TRANSFORM_REF(EdgeIterator,
              std::unique_ptr<IteratorBase<const ::EdgeAccessor>>);
TRANSFORM_REF(VertexAccessIterator, vertex_access_iterator_t);
TRANSFORM_REF(OutEdgesIterator, out_edge_iterator_t);
TRANSFORM_REF(InEdgesIterator, in_edge_iterator_t);

template <class T>
TRANSFORM_REF_TEMPLATED(VertexIndex<T>, VertexIndexBase<T>);
template <class T>
TRANSFORM_REF_TEMPLATED(EdgeIndex<T>, EdgeIndexBase<T>);
template <class T>
TRANSFORM_REF_TEMPLATED(BoltSerializer<T>, ::bolt::BoltSerializer<T>);

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
TRANSFORM_VALUE_ONE(OutEdgesIterator, out_edge_iterator_t);
MOVE_CONSTRUCTOR_FORCED(OutEdgesIterator, out_edge_iterator_t);
TRANSFORM_VALUE_ONE(InEdgesIterator, in_edge_iterator_t);
MOVE_CONSTRUCTOR_FORCED(InEdgesIterator, in_edge_iterator_t);
TRANSFORM_VALUE_ONE(VertexIterator,
                    std::unique_ptr<IteratorBase<const ::VertexAccessor>>);
MOVE_CONSTRUCTOR_FORCED(VertexIterator,
                        std::unique_ptr<IteratorBase<const ::VertexAccessor>>);
TRANSFORM_VALUE_ONE(EdgeIterator,
                    std::unique_ptr<IteratorBase<const ::EdgeAccessor>>);
MOVE_CONSTRUCTOR_FORCED(EdgeIterator,
                        std::unique_ptr<IteratorBase<const ::EdgeAccessor>>);

template <class T>
TRANSFORM_VALUE_ONE_RAW(
    VertexPropertyType<T>,
    ::VertexPropertyFamily::PropertyType::PropertyTypeKey<T>);
template <class T>
TRANSFORM_VALUE_ONE_RAW(EdgePropertyType<T>,
                        ::EdgePropertyFamily::PropertyType::PropertyTypeKey<T>)

template <class T>
TRANSFORM_VALUE_ONE_RAW(BoltSerializer<T>, ::bolt::BoltSerializer<T>)

// ********************* SPECIAL SIZED CONSTRUCTORS
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
