#pragma once

#include "database/db_transaction.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/border.hpp"
// #include "utils/iterator/iterator.hpp"
#include "utils/option.hpp"

namespace tx
{
class Transaction;
}

/*
* DbAccessor
* -Guarantees that access to Vertex and Edge is possible only through
* Vertex::Accessor and Edge::Accessor.
* -Guarantees that changing Vertex and Edge is possible only using
* Vertex::Accessor returned by vertex_insert() method and
* Edge::Accessor returned by edge_insert() method.
* -Offers CRUD for Vertex and Edge except iterating over all edges.
*
* Vertex::Accessor
* By default Vertex::accessor is empty. Caller has to call fill() method
* to fetch valid data and check it's return value. fill() method returns
* true if there is valid data for current transaction false otherwise.
* Only exception to this rule is vertex_insert() method in DbAccessor
* which returns by default filled Vertex::Accessor.
*
* Edge::Accessor
* By default Edge::accessor is empty. Caller has to call fill() method
* to
* fetch valid data and check it's return value. fill() method returns
* true
* if there is valid data for current transaction false otherwise.
* Only exception to this rule is edge_insert() method in DbAccessor
* which
* returns by default filled Edge::Accessor.
*/
class DbAccessor
{

public:
    DbAccessor(Db &db);

    //*******************VERTEX METHODS

    auto vertex_access();

    Option<const Vertex::Accessor> vertex_find(const Id &id);

    // Creates new Vertex and returns filled Vertex::Accessor.
    Vertex::Accessor vertex_insert();

    // ******************* EDGE METHODS

    Option<const Edge::Accessor> edge_find(const Id &id);

    // Creates new Edge and returns filled Edge::Accessor.
    Edge::Accessor edge_insert(Vertex::Accessor const &from,
                               Vertex::Accessor const &to);

    // ******************* LABEL METHODS

    const Label &label_find_or_create(const char *name);

    bool label_contains(const char *name);

    // ******************** TYPE METHODS

    const EdgeType &type_find_or_create(const char *name);

    bool type_contains(const char *name);

    // ******************** PROPERTY METHODS

    PropertyFamily &vertex_property_family_get(const std::string &name);

    PropertyFamily &edge_property_family_get(const std::string &name);

    // ******************** PROPERTY HELPER METHODS
    PropertyFamily::PropertyType::PropertyFamilyKey
    vertex_property_key(const std::string &name, Type type);

    PropertyFamily::PropertyType::PropertyFamilyKey
    edge_property_key(const std::string &name, Type type);

    // ******************** TRANSACTION METHODS

    void commit();
    void abort();

private:
    template <class T, class K>
    friend class NonUniqueUnorderedIndex;

    DbTransaction db_transaction;
};

// ********************** CONVENIENT FUNCTIONS

template <class R>
bool option_fill(Option<R> &o)
{
    return o.is_present() && o.get().fill();
}
