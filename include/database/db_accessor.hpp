#pragma once

#include "database/db.hpp"
#include "database/db_accessor.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/vertices.hpp"
#include "transactions/transaction.hpp"

class DbAccessor
{
public:
    DbAccessor(Db &db);

    // VERTEX METHODS
    Vertices::vertices_t::Accessor vertex_access();

    const Vertex::Accessor vertex_find(const Id &id);

    const Vertex::Accessor vertex_first();

    Vertex::Accessor vertex_insert();

    // EDGE METHODS
    Edge::Accessor edge_find(const Id &id);

    Edge::Accessor edge_insert(VertexRecord *from, VertexRecord *to);

    // LABEL METHODS
    const Label &label_find_or_create(const std::string &name);

    bool label_contains(const std::string &name);

    VertexIndexRecordCollection &label_find_index(const Label &label);

    // TYPE METHODS
    const EdgeType &type_find_or_create(const std::string &name);

    bool type_contains(const std::string &name);

    // TRANSACTION METHODS
    void commit();
    void abort();

    // EASE OF USE METHODS
    tx::Transaction &operator*();

    DbTransaction db;
};
