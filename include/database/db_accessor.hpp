#pragma once

#include "database/graph_db.hpp"
#include "database/db_transaction.hpp"
#include "utils/border.hpp"
#include "utils/iterator/iterator.hpp"
#include "utils/option.hpp"
#include "storage/model/typed_value_store.hpp"

namespace tx {
  class Transaction;
}

class Label;

class EdgeType;

using EdgePropertyFamily = PropertyFamily<TypeGroupEdge>;
using VertexPropertyFamily = PropertyFamily<TypeGroupVertex>;

/*
* DbAccessor
* -Guarantees that access to Vertex and Edge is possible only through
* VertexAccessor and EdgeAccessor.
* -Guarantees that changing Vertex and Edge is possible only using
* VertexAccessor returned by vertex_insert() method and
* EdgeAccessor returned by edge_insert() method.
* -Offers CRUD for Vertex and Edge except iterating over all edges.
*
* VertexAccessor
* By default Vertex::accessor is empty. Caller has to call fill() method
* to fetch valid data and check it's return value. fill() method returns
* true if there is valid data for current transaction false otherwise.
* Only exception to this rule is vertex_insert() method in DbAccessor
* which returns by default filled VertexAccessor.
*
* EdgeAccessor
* By default Edge::accessor is empty. Caller has to call fill() method
* to
* fetch valid data and check it's return value. fill() method returns
* true
* if there is valid data for current transaction false otherwise.
* Only exception to this rule is edge_insert() method in DbAccessor
* which
* returns by default filled EdgeAccessor.
*/
class DbAccessor {

public:
  DbAccessor(Db &db);

  DbAccessor(Db &db, tx::Transaction &t);

  DbAccessor(const DbAccessor &other) = delete;

  DbAccessor(DbAccessor &&other) = delete;

  //*******************VERTEX METHODS
  // Returns iterator of VertexAccessor for all vertices.
  // TODO: Implement class specaily for this return
  // NOTE: This implementation must be here to be able to infere return type.
  auto vertex_access() {
    return iter::make_map(
        iter::make_iter(this->db_transaction.db.graph.vertices.access()),
        [&](auto e) -> auto {
          return VertexAccessor(&(e->second), db_transaction);
        });
  }

  // Optionaly return vertex with given internal Id.
  Option<const VertexAccessor> vertex_find(const Id &id);

  // Creates new Vertex and returns filled VertexAccessor.
  VertexAccessor vertex_insert();

  // ******************* EDGE METHODS
  // Returns iterator of EdgeAccessor for all edges.
  // TODO: Implement class specaily for this return
  // NOTE: This implementation must be here to be able to infere return type.
  auto edge_access() {
    return iter::make_map(
        iter::make_iter(this->db_transaction.db.graph.edges.access()),
        [&](auto e) -> auto {
          return EdgeAccessor(&(e->second), db_transaction);
        });
  }

  // Optionally return Edge with given internal Id.
  Option<const EdgeAccessor> edge_find(const Id &id);

  // Creates new Edge and returns filled EdgeAccessor.
  // Slighlty faster than const version.
  EdgeAccessor edge_insert(VertexAccessor &from, VertexAccessor &to);

  // Creates new Edge and returns filled EdgeAccessor.
  EdgeAccessor edge_insert(VertexAccessor const &from,
                           VertexAccessor const &to);

  // ******************* LABEL METHODS
  // Finds or crated label with given name.
  const Label &label_find_or_create(const char *name);

  // True if label with name exists.
  bool label_contains(const char *name);

  // ******************** TYPE METHODS
  // Finds or creates edge_type with given name.
  const EdgeType &type_find_or_create(const char *name);

  // True if edge_type with given name exists.
  bool type_contains(const char *name);

  // ******************** PROPERTY METHODS

  VertexPropertyFamily &vertex_property_family_get(const std::string &name);

  EdgePropertyFamily &edge_property_family_get(const std::string &name);

  // ******************** PROPERTY HELPER METHODS
  VertexPropertyFamily::PropertyType::PropertyFamilyKey
  vertex_property_key(const std::string &name, Type type);

  EdgePropertyFamily::PropertyType::PropertyFamilyKey
  edge_property_key(const std::string &name, Type type);

  template<class T>
  VertexPropertyFamily::PropertyType::PropertyTypeKey <T>
  vertex_property_key(const std::string &name) {
    return vertex_property_family_get(name)
        .get(T::type)
        .template type_key<T>();
  }

  template<class T>
  EdgePropertyFamily::PropertyType::PropertyTypeKey <T>
  edge_property_key(const std::string &name) {
    return edge_property_family_get(name)
        .get(T::type)
        .template type_key<T>();
  }

  bool update_indexes();
  // ******************** TRANSACTION METHODS

  // True if commit was successful, or false if transaction was aborted.
  bool commit();

  // Aborts transaction.
  void abort();

private:
  // TODO: make this friend generic for all indexes.
  template<class T, class K>
  friend
  class NonUniqueUnorderedIndex;

  template<class T, class K>
  friend
  class UniqueOrderedIndex;

  DbTransaction db_transaction;
};
