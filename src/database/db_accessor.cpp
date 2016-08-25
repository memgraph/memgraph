#include "database/db.hpp"
#include "database/db_accessor.hpp"
#include "utils/iterator/iterator.hpp"

DbAccessor::DbAccessor(Db &db)
    : db_transaction(DbTransaction(db, db.tx_engine.begin()))
{
}

DbAccessor::DbAccessor(Db &db, tx::Transaction &t)
    : db_transaction(DbTransaction(db, t))
{
}

// VERTEX METHODS
auto DbAccessor::vertex_access()
{
    return iter::make_map(
        iter::make_iter(this->db_transaction.db.graph.vertices.access()),
        [&](auto e) -> auto {
            return VertexAccessor(&(e->second), db_transaction);
        });
}

Option<const VertexAccessor> DbAccessor::vertex_find(const Id &id)
{
    return this->db_transaction.db.graph.vertices.find(db_transaction, id);
}

VertexAccessor DbAccessor::vertex_insert()
{
    return this->db_transaction.db.graph.vertices.insert(db_transaction);
}

// EDGE METHODS

Option<const EdgeAccessor> DbAccessor::edge_find(const Id &id)
{
    return db_transaction.db.graph.edges.find(db_transaction, id);
}

EdgeAccessor DbAccessor::edge_insert(VertexAccessor const &from,
                                     VertexAccessor const &to)
{
    auto edge_accessor = db_transaction.db.graph.edges.insert(
        db_transaction, from.vlist, to.vlist);
    from.update()->data.out.add(edge_accessor.vlist);
    to.update()->data.in.add(edge_accessor.vlist);
    return edge_accessor;
}

// LABEL METHODS
const Label &DbAccessor::label_find_or_create(const char *name)
{
    return db_transaction.db.graph.label_store.find_or_create(name);
}

bool DbAccessor::label_contains(const char *name)
{
    return db_transaction.db.graph.label_store.contains(name);
}

// TYPE METHODS
const EdgeType &DbAccessor::type_find_or_create(const char *name)
{
    return db_transaction.db.graph.edge_type_store.find_or_create(name);
}

bool DbAccessor::type_contains(const char *name)
{
    return db_transaction.db.graph.edge_type_store.contains(name);
}

// PROPERTY METHODS
VertexPropertyFamily &
DbAccessor::vertex_property_family_get(const std::string &name)
{
    return db_transaction.db.graph.vertices.property_family_find_or_create(
        name);
}

EdgePropertyFamily &
DbAccessor::edge_property_family_get(const std::string &name)
{
    return db_transaction.db.graph.edges.property_family_find_or_create(name);
}

// PROPERTY HELPER METHODS
VertexPropertyFamily::PropertyType::PropertyFamilyKey
DbAccessor::vertex_property_key(const std::string &name, Type type)
{
    return vertex_property_family_get(name).get(type).family_key();
}

EdgePropertyFamily::PropertyType::PropertyFamilyKey
DbAccessor::edge_property_key(const std::string &name, Type type)
{
    return edge_property_family_get(name).get(type).family_key();
}

template <class T>
VertexPropertyFamily::PropertyType::PropertyTypeKey<T>
DbAccessor::vertex_property_key(const std::string &name)
{
    return vertex_property_family_get(name).get(T::type).template type_key<T>();
}

template <class T>
EdgePropertyFamily::PropertyType::PropertyTypeKey<T>
DbAccessor::edge_property_key(const std::string &name)
{
    return edge_property_family_get(name).get(T::type).template type_key<T>();
}

// TRANSACTION METHODS
bool DbAccessor::commit()
{
    if (db_transaction.update_indexes()) {
        db_transaction.trans.commit();
        return true;
    } else {
        db_transaction.trans.abort();
        return false;
    }
}
void DbAccessor::abort() { db_transaction.trans.abort(); }
