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

// ************************* Implementations
namespace barrier
{

// ************************* EdgePropertyType
#define FOR_ALL_PROPS_delete_EdgePropertyType(x)                               \
    template <>                                                                \
    EdgePropertyType<x>::~EdgePropertyType()                                   \
    {                                                                          \
        HALF_CALL(~PropertyTypeKey());                                         \
    }
INSTANTIATE_FOR_PROPERTY(FOR_ALL_PROPS_delete_EdgePropertyType)

// ************************* VertexPropertyType
#define FOR_ALL_PROPS_delete_VertexPropertyType(x)                             \
    template <>                                                                \
    VertexPropertyType<x>::~VertexPropertyType()                               \
    {                                                                          \
        HALF_CALL(~PropertyTypeKey());                                         \
    }
INSTANTIATE_FOR_PROPERTY(FOR_ALL_PROPS_delete_VertexPropertyType)

// ***************** Label
VertexIndex<std::nullptr_t> &Label::index() const { return CALL(index()); }

// **************** EdgeType
EdgeIndex<std::nullptr_t> &EdgeType::index() const { return CALL(index()); }

// **************** VertexIndex
template <class K>
VertexIterator VertexIndex<K>::for_range(DbAccessor &t, Border<K> from,
                                         Border<K> to)
{
    return CALL(for_range(trans(t), std::move(from), std::move(to)));
}

template <class K>
bool VertexIndex<K>::unique()
{
    return HALF_CALL(unique());
}

template <class K>
Order VertexIndex<K>::order()
{
    return HALF_CALL(order());
}

// **************** EdgeIndex
template <class K>
EdgeIterator EdgeIndex<K>::for_range(DbAccessor &t, Border<K> from,
                                     Border<K> to)
{
    return CALL(for_range(trans(t), std::move(from), std::move(to)));
}

template <class K>
bool EdgeIndex<K>::unique()
{
    return HALF_CALL(unique());
}

template <class K>
Order EdgeIndex<K>::order()
{
    return HALF_CALL(order());
}

// ************************* DbAccessor
DESTRUCTOR(DbAccessor, DbAccessor);

VertexAccessIterator DbAccessor::vertex_access()
{
    return CALL(vertex_access());
}

Option<const VertexAccessor> DbAccessor::vertex_find(const Id &id)
{
    return HALF_CALL(vertex_find(id)).map<const VertexAccessor>();
}

VertexAccessor DbAccessor::vertex_insert() { return CALL(vertex_insert()); }

Option<const EdgeAccessor> DbAccessor::edge_find(const Id &id)
{
    return HALF_CALL(edge_find(id)).map<const EdgeAccessor>();
}

EdgeAccessor DbAccessor::edge_insert(VertexAccessor const &from,
                                     VertexAccessor const &to)
{
    return CALL(edge_insert(trans(from), trans(to)));
}

const Label &DbAccessor::label_find_or_create(const char *name)
{
    return CALL(label_find_or_create(name));
}

bool DbAccessor::label_contains(const char *name)
{
    return HALF_CALL(label_contains(name));
}

const EdgeType &DbAccessor::type_find_or_create(const char *name)
{
    return CALL(type_find_or_create(name));
}

bool DbAccessor::type_contains(const char *name)
{
    return HALF_CALL(type_contains(name));
}

VertexPropertyFamily &
DbAccessor::vertex_property_family_get(const std::string &name)
{
    return CALL(vertex_property_family_get(name));
}

EdgePropertyFamily &
DbAccessor::edge_property_family_get(const std::string &name)
{
    return CALL(edge_property_family_get(name));
}

VertexPropertyKey DbAccessor::vertex_property_key(const std::string &name,
                                                  Type type)
{
    return CALL(vertex_property_key(name, type));
}

EdgePropertyKey DbAccessor::edge_property_key(const std::string &name,
                                              Type type)
{
    return CALL(edge_property_key(name, type));
}

template <class T>
VertexPropertyType<T> DbAccessor::vertex_property_key(const std::string &name)
{
    return CALL(vertex_property_key<T>(name));
}
#define DbAccessor_vertex_property_key(x)                                      \
    template VertexPropertyType<x> DbAccessor::vertex_property_key<x>(         \
        const std::string &name);
INSTANTIATE_FOR_PROPERTY(DbAccessor_vertex_property_key)

template <class T>
EdgePropertyType<T> DbAccessor::edge_property_key(const std::string &name)
{
    return CALL(edge_property_key<T>(name));
}
#define DbAccessor_edge_property_key(x)                                        \
    template EdgePropertyType<x> DbAccessor::edge_property_key<x>(             \
        const std::string &name);
INSTANTIATE_FOR_PROPERTY(DbAccessor_edge_property_key)

bool DbAccessor::commit() { return HALF_CALL(commit()); }
void DbAccessor::abort() { HALF_CALL(abort()); }

// ************************** VertexAccessor
DUP(VertexAccessor, COPY_CONSTRUCTOR);
MOVE_CONSTRUCTOR(VertexAccessor);
MOVE_CONST_CONSTRUCTOR(VertexAccessor);
DESTRUCTOR(VertexAccessor, VertexAccessor);
COPY_OPERATOR(VertexAccessor);
MOVE_OPERATOR(VertexAccessor);

size_t VertexAccessor::out_degree() const { return HALF_CALL(out_degree()); }

size_t VertexAccessor::in_degree() const { return HALF_CALL(in_degree()); }

size_t VertexAccessor::degree() const { return HALF_CALL(degree()); }

bool VertexAccessor::add_label(const Label &label)
{
    return HALF_CALL(add_label(trans(label)));
}

bool VertexAccessor::remove_label(const Label &label)
{
    return HALF_CALL(remove_label(trans(label)));
}

bool VertexAccessor::has_label(const Label &label) const
{
    return HALF_CALL(has_label(trans(label)));
}

const std::vector<label_ref_t> &VertexAccessor::labels() const
{
    return CALL(labels());
}

OutEdgesIterator VertexAccessor::out() const { return CALL(out()); }

InEdgesIterator VertexAccessor::in() const { return CALL(in()); }

bool VertexAccessor::in_contains(VertexAccessor const &other) const
{
    return HALF_CALL(in_contains(trans(other)));
}

bool VertexAccessor::empty() const { return HALF_CALL(empty()); }

bool VertexAccessor::fill() const { return HALF_CALL(fill()); }

const Id &VertexAccessor::id() const { return HALF_CALL(id()); }

VertexAccessor VertexAccessor::update() const { return CALL(update()); }

bool VertexAccessor::remove() const { return HALF_CALL(remove()); }

const Property &VertexAccessor::at(VertexPropertyFamily &key) const
{
    return HALF_CALL(at(trans(key)));
}

const Property &VertexAccessor::at(VertexPropertyKey &key) const
{
    return HALF_CALL(at(trans(key)));
}

template <class V>
OptionPtr<V> VertexAccessor::at(VertexPropertyType<V> &key) const
{
    return HALF_CALL(at(trans<V>(key)));
}
#define VertexAccessor_at(x)                                                   \
    template OptionPtr<x> VertexAccessor::at(VertexPropertyType<x> &key) const;
INSTANTIATE_FOR_PROPERTY(VertexAccessor_at);

// NOTE: I am not quite sure if this method will have any use
template <class V, class... Args>
void VertexAccessor::set(VertexPropertyType<V> &key, Args &&... args)
{
    HALF_CALL(set(trans(key), args...));
}

void VertexAccessor::set(VertexPropertyKey &key, Property::sptr value)
{
    HALF_CALL(set(trans(key), std::move(value)));
}

void VertexAccessor::clear(VertexPropertyKey &key)
{
    HALF_CALL(clear(trans(key)));
}

void VertexAccessor::clear(VertexPropertyFamily &key)
{
    HALF_CALL(clear(trans(key)));
}

// NOTE: I am not quite sure if this method will have any use
template <class Handler>
void VertexAccessor::accept(Handler &handler) const
{
    HALF_CALL(accept(handler));
}

VertexAccessor::operator bool() const { return HALF_CALL(operator bool()); }

bool operator==(const VertexAccessor &a, const VertexAccessor &b)
{
    return trans(a) == trans(b);
}

bool operator!=(const VertexAccessor &a, const VertexAccessor &b)
{
    return trans(a) != trans(b);
}

// ************************** EdgeAccessor
DUP(EdgeAccessor, COPY_CONSTRUCTOR);
MOVE_CONSTRUCTOR(EdgeAccessor);
MOVE_CONST_CONSTRUCTOR(EdgeAccessor);
DESTRUCTOR(EdgeAccessor, EdgeAccessor);
COPY_OPERATOR(EdgeAccessor);
MOVE_OPERATOR(EdgeAccessor);

void EdgeAccessor::edge_type(const EdgeType &edge_type)
{
    HALF_CALL(edge_type(trans(edge_type)));
}

const EdgeType &EdgeAccessor::edge_type() const { return CALL(edge_type()); }

const VertexAccessor EdgeAccessor::from() const { return CALL(from()); }

const VertexAccessor EdgeAccessor::to() const { return CALL(to()); }

bool EdgeAccessor::empty() const { return HALF_CALL(empty()); }

bool EdgeAccessor::fill() const { return HALF_CALL(fill()); }

const Id &EdgeAccessor::id() const { return HALF_CALL(id()); }

EdgeAccessor EdgeAccessor::update() const { return CALL(update()); }

bool EdgeAccessor::remove() const { return HALF_CALL(remove()); }

const Property &EdgeAccessor::at(EdgePropertyFamily &key) const
{
    return HALF_CALL(at(trans(key)));
}

const Property &EdgeAccessor::at(EdgePropertyKey &key) const
{
    return HALF_CALL(at(trans(key)));
}

template <class V>
OptionPtr<V> EdgeAccessor::at(EdgePropertyType<V> &key) const
{
    return HALF_CALL(at(trans<V>(key)));
}
#define EdgeAccessor_at(x)                                                     \
    template OptionPtr<x> EdgeAccessor::at(EdgePropertyType<x> &key) const;
INSTANTIATE_FOR_PROPERTY(EdgeAccessor_at);

// NOTE: I am not quite sure if this method will have any use
template <class V, class... Args>
void EdgeAccessor::set(EdgePropertyType<V> &key, Args &&... args)
{
    HALF_CALL(set(trans(key), args...));
}

void EdgeAccessor::set(EdgePropertyKey &key, Property::sptr value)
{
    HALF_CALL(set(trans(key), std::move(value)));
}

void EdgeAccessor::clear(EdgePropertyKey &key) { HALF_CALL(clear(trans(key))); }

void EdgeAccessor::clear(EdgePropertyFamily &key)
{
    HALF_CALL(clear(trans(key)));
}

// NOTE: I am not quite sure if this method will have any use
template <class Handler>
void EdgeAccessor::accept(Handler &handler) const
{
    HALF_CALL(accept(handler));
}

EdgeAccessor::operator bool() const { return HALF_CALL(operator bool()); }

bool operator==(const EdgeAccessor &a, const EdgeAccessor &b)
{
    return trans(a) == trans(b);
}

bool operator!=(const EdgeAccessor &a, const EdgeAccessor &b)
{
    return trans(a) != trans(b);
}

// ************************* VertexIterator
DESTRUCTOR(VertexIterator, unique_ptr);

Option<const VertexAccessor> VertexIterator::next()
{
    return HALF_CALL(get()->next()).map<const VertexAccessor>();
}

// ************************* EdgeIterator
DESTRUCTOR(EdgeIterator, unique_ptr);

Option<const EdgeAccessor> EdgeIterator::next()
{
    return HALF_CALL(get()->next()).map<const EdgeAccessor>();
}

// ************************* OutEdgesIterator
DESTRUCTOR(OutEdgesIterator, out_edge_iterator_t);

Option<const EdgeAccessor> OutEdgesIterator::next()
{
    return HALF_CALL(next()).map<const EdgeAccessor>();
}

// ************************* InEdgesIterator
DESTRUCTOR(InEdgesIterator, in_edge_iterator_t);

Option<const EdgeAccessor> InEdgesIterator::next()
{
    return HALF_CALL(next()).map<const EdgeAccessor>();
}

// ************************* VertexAccessIterator
DESTRUCTOR(VertexAccessIterator, vertex_access_iterator_t);

Option<const VertexAccessor> VertexAccessIterator::next()
{
    return HALF_CALL(next()).map<const VertexAccessor>();
}

// ************************* VertexPropertyKey
DESTRUCTOR(VertexPropertyKey, PropertyFamilyKey);

// ************************* EdgePropertyKey
DESTRUCTOR(EdgePropertyKey, PropertyFamilyKey);

// ************************* VertexPropertyFamily
OptionPtr<VertexIndex<std::nullptr_t>> VertexPropertyFamily::index()
{
    OptionPtr<IndexBase<TypeGroupVertex, std::nullptr_t>> ret =
        THIS->index.get_read();
    if (ret.is_present()) {
        return OptionPtr<VertexIndex<std::nullptr_t>>(&trans(*ret.get()));
    } else {
        return OptionPtr<VertexIndex<std::nullptr_t>>();
    }
}

// ************************* VertexPropertyFamily
OptionPtr<EdgeIndex<std::nullptr_t>> EdgePropertyFamily::index()
{
    OptionPtr<IndexBase<TypeGroupEdge, std::nullptr_t>> ret =
        THIS->index.get_read();
    if (ret.is_present()) {
        return OptionPtr<EdgeIndex<std::nullptr_t>>(&trans(*ret.get()));
    } else {
        return OptionPtr<EdgeIndex<std::nullptr_t>>();
    }
}

// ************************* BOLT SERIALIZER
template <class Stream>
BoltSerializer<Stream>::~BoltSerializer()
{
    THIS->~BoltSerializer();
}

template <class Stream>
void BoltSerializer<Stream>::write(const VertexAccessor &vertex)
{
    HALF_CALL(write(trans(vertex)));
}

template <class Stream>
void BoltSerializer<Stream>::write(const EdgeAccessor &edge)
{
    HALF_CALL(write(trans(edge)));
}

template <class Stream>
void BoltSerializer<Stream>::write(const Property &prop)
{
    HALF_CALL(write(prop));
}

template <class Stream>
void BoltSerializer<Stream>::write_null()
{
    HALF_CALL(write_null());
}

template <class Stream>
void BoltSerializer<Stream>::write(const Bool &prop)
{
    HALF_CALL(write(prop));
}

template <class Stream>
void BoltSerializer<Stream>::write(const Float &prop)
{
    HALF_CALL(write(prop));
}

template <class Stream>
void BoltSerializer<Stream>::write(const Double &prop)
{
    HALF_CALL(write(prop));
}

template <class Stream>
void BoltSerializer<Stream>::write(const Int32 &prop)
{
    HALF_CALL(write(prop));
}

template <class Stream>
void BoltSerializer<Stream>::write(const Int64 &prop)
{
    HALF_CALL(write(prop));
}

template <class Stream>
void BoltSerializer<Stream>::write(const std::string &value)
{
    HALF_CALL(write(value));
}

template <class Stream>
void BoltSerializer<Stream>::write(const String &prop)
{
    HALF_CALL(write(prop));
}

template <class Stream>
template <class T>
void BoltSerializer<Stream>::handle(const T &prop)
{
    HALF_CALL(template handle<T>(prop));
}
}

// **************************** ERROR EXAMPLES ****************************** //
// **************************** COMPILE TIME
/*
error:
../libmemgraph.a(barrier.cpp.o): In function `Option<barrier::VertexAccessor
const> Option<VertexAccessor const>::map<barrier::VertexAccessor const>()':
/home/ktf/Workspace/memgraph/include/utils/option.hpp:111: undefined reference
to `barrier::VertexAccessor::VertexAccessor<VertexAccessor const>(VertexAccessor
const&&)'

description:
Constructor VertexAccessor<::VertexAccessor const>(::VertexAccessor const&&)
isn't written.


error:
../libmemgraph.a(barrier.cpp.o): In function `barrier::EdgeAccessor::from()
const':
/home/ktf/Workspace/memgraph/src/barrier/barrier.cpp:501: undefined reference to
`barrier::VertexAccessor::VertexAccessor<barrier::VertexAccessor
const>(barrier::VertexAccessor const&&)'

description:
Move constructor VertexAccessor<VertexAccessor const>(VertexAccessor const&&)
isn't defined.





*/
