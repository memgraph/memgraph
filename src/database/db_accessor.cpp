#include "database/db_accessor.hpp"

#include "database/db.hpp"

#include "utils/iterator/iterator.hpp"

DbAccessor::DbAccessor(Db &db)
    : db_transaction(DbTransaction(db, db.tx_engine.begin()))
{
}

// VERTEX METHODS
auto DbAccessor::vertex_access()
{
    return iter::make_map(
        iter::make_iter(this->db_transaction.db.graph.vertices.access()),
        [&](auto e) -> auto {
            return Vertex::Accessor(&(e->second), db_transaction);
        });
}

Option<const Vertex::Accessor> DbAccessor::vertex_find(const Id &id)
{
    return this->db_transaction.db.graph.vertices.find(db_transaction, id);
}

Vertex::Accessor DbAccessor::vertex_insert()
{
    return this->db_transaction.db.graph.vertices.insert(db_transaction);
}

// EDGE METHODS

Option<const Edge::Accessor> DbAccessor::edge_find(const Id &id)
{
    return db_transaction.db.graph.edges.find(db_transaction, id);
}

Edge::Accessor DbAccessor::edge_insert(Vertex::Accessor const &from,
                                       Vertex::Accessor const &to)
{
    auto edge_accessor = db_transaction.db.graph.edges.insert(
        db_transaction, from.vlist, to.vlist);
    from.update()->data.out.add(edge_accessor.vlist);
    to.update()->data.in.add(edge_accessor.vlist);
    return edge_accessor;
}

// LABEL METHODS
const Label &DbAccessor::label_find_or_create(const std::string &name)
{
    return db_transaction.db.graph.label_store.find_or_create(
        std::forward<const std::string &>(name));
}

bool DbAccessor::label_contains(const std::string &name)
{
    return db_transaction.db.graph.label_store.contains(
        std::forward<const std::string &>(name));
}

// TYPE METHODS
const EdgeType &DbAccessor::type_find_or_create(const std::string &name)
{
    return db_transaction.db.graph.edge_type_store.find_or_create(
        std::forward<const std::string &>(name));
}

bool DbAccessor::type_contains(const std::string &name)
{
    return db_transaction.db.graph.edge_type_store.contains(
        std::forward<const std::string &>(name));
}

//********************PROPERTY METHODS
// Vertices::prop_familys_t::Accessor
// DbAccessor::vertex_property_family_access()
// {
//     return db.db.graph.vertices.property_family_access();
// }
//
// auto DbAccessor::edge_property_family_access()
// {
//     return db.db.graph.edges.property_family_access();
// }

auto edge_property_family_access();

PropertyFamily &DbAccessor::vertex_property_family_get(const std::string &name)
{
    return db_transaction.db.graph.vertices.property_family_find_or_create(
        name);
}

PropertyFamily &DbAccessor::edge_property_family_get(const std::string &name)
{
    return db_transaction.db.graph.edges.property_family_find_or_create(name);
}

// TRANSACTION METHODS
void DbAccessor::commit() { db_transaction.trans.commit(); }
void DbAccessor::abort() { db_transaction.trans.abort(); }

// // EASE OF USE METHODS
// tx::Transaction &DbAccessor::operator*() { return db.trans; }
