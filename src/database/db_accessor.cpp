#include "database/db_accessor.hpp"

DbAccessor::DbAccessor(Db &db) : db(DbTransaction(db, db.tx_engine.begin())) {}

// VERTEX METHODS
Vertices::vertices_t::Accessor DbAccessor::vertex_access()
{
    return this->db.db.graph.vertices.access();
}

const Vertex::Accessor DbAccessor::vertex_find(const Id &id)
{
    return this->db.db.graph.vertices.find(db, id);
}

const Vertex::Accessor DbAccessor::vertex_first()
{
    return this->db.db.graph.vertices.first(db);
}

Vertex::Accessor DbAccessor::vertex_insert()
{
    return this->db.db.graph.vertices.insert(db);
}

// EDGE METHODS

Edge::Accessor DbAccessor::edge_find(const Id &id)
{
    return db.db.graph.edges.find(db, id);
}

Edge::Accessor DbAccessor::edge_insert(VertexRecord *from, VertexRecord *to)
{
    auto edge_accessor = db.db.graph.edges.insert(db, from, to);
    from->update(db.trans)->data.out.add(edge_accessor.vlist);
    to->update(db.trans)->data.in.add(edge_accessor.vlist);
    return edge_accessor;
}

// LABEL METHODS
const Label &DbAccessor::label_find_or_create(const std::string &name)
{
    return db.db.graph.label_store.find_or_create(
        std::forward<const std::string &>(name));
}

bool DbAccessor::label_contains(const std::string &name)
{
    return db.db.graph.label_store.contains(
        std::forward<const std::string &>(name));
}

VertexIndexRecordCollection &DbAccessor::label_find_index(const Label &label)
{
    return db.db.graph.vertices.find_label_index(label);
}

// TYPE METHODS
const EdgeType &DbAccessor::type_find_or_create(const std::string &name)
{
    return db.db.graph.edge_type_store.find_or_create(
        std::forward<const std::string &>(name));
}

bool DbAccessor::type_contains(const std::string &name)
{
    return db.db.graph.edge_type_store.contains(
        std::forward<const std::string &>(name));
}

// TRANSACTION METHODS
void DbAccessor::commit() { db.trans.commit(); }
void DbAccessor::abort() { db.trans.abort(); }

// EASE OF USE METHODS
tx::Transaction &DbAccessor::operator*() { return db.trans; }
