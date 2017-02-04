#include "database/graph_db.hpp"
#include "database/db_accessor.hpp"

DbAccessor::DbAccessor(Db &db)
    : db_transaction(std::move(DbTransaction(db, db.tx_engine.begin()))) {
}

DbAccessor::DbAccessor(Db &db, tx::Transaction &t)
    : db_transaction(std::move(DbTransaction(db, t))) {
}

// VERTEX METHODS

Option<const VertexAccessor> DbAccessor::vertex_find(const Id &id) {
  return this->db_transaction.db.graph.vertices.find(db_transaction, id);
}

VertexAccessor DbAccessor::vertex_insert() {
  return this->db_transaction.db.graph.vertices.insert(db_transaction);
}

// EDGE METHODS

Option<const EdgeAccessor> DbAccessor::edge_find(const Id &id) {
  return db_transaction.db.graph.edges.find(db_transaction, id);
}

EdgeAccessor DbAccessor::edge_insert(VertexAccessor &from, VertexAccessor &to) {
  auto edge_accessor = db_transaction.db.graph.edges.insert(
      db_transaction, from.vlist, to.vlist);

  // Connect edge with from,to vertices.
  from->data.out.add(edge_accessor.vlist);
  to->data.in.add(edge_accessor.vlist);

  return edge_accessor;
}

EdgeAccessor DbAccessor::edge_insert(VertexAccessor const &from,
                                     VertexAccessor const &to) {
  auto edge_accessor = db_transaction.db.graph.edges.insert(
      db_transaction, from.vlist, to.vlist);

  // Connect edge with updated from,to vertices.
  from.update()->data.out.add(edge_accessor.vlist);
  to.update()->data.in.add(edge_accessor.vlist);

  return edge_accessor;
}

// LABEL METHODS
const Label &DbAccessor::label_find_or_create(const char *name) {
  return db_transaction.db.graph.label_store.find_or_create(name);
}

bool DbAccessor::label_contains(const char *name) {
  return db_transaction.db.graph.label_store.contains(name);
}

// TYPE METHODS
const EdgeType &DbAccessor::type_find_or_create(const char *name) {
  return db_transaction.db.graph.edge_type_store.find_or_create(name);
}

bool DbAccessor::type_contains(const char *name) {
  return db_transaction.db.graph.edge_type_store.contains(name);
}

// PROPERTY METHODS
VertexPropertyFamily &
DbAccessor::vertex_property_family_get(const std::string &name) {
  return db_transaction.db.graph.vertices.property_family_find_or_create(
      name);
}

EdgePropertyFamily &
DbAccessor::edge_property_family_get(const std::string &name) {
  return db_transaction.db.graph.edges.property_family_find_or_create(name);
}

// PROPERTY HELPER METHODS
VertexPropertyFamily::PropertyType::PropertyFamilyKey
DbAccessor::vertex_property_key(const std::string &name, Type type) {
  return vertex_property_family_get(name).get(type).family_key();
}

EdgePropertyFamily::PropertyType::PropertyFamilyKey
DbAccessor::edge_property_key(const std::string &name, Type type) {
  return edge_property_family_get(name).get(type).family_key();
}

bool DbAccessor::update_indexes() {
  return db_transaction.update_indexes();
}

// TRANSACTION METHODS
bool DbAccessor::commit() {
  if (db_transaction.update_indexes()) {
    db_transaction.trans.commit();
    return true;
  } else {
    // Index update wasn't successfull so whe are aborting transaction.
    db_transaction.trans.abort();
    return false;
  }
}

void DbAccessor::abort() { db_transaction.trans.abort(); }
