// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "_mgp.hpp"
#include "mg_exceptions.hpp"
#include "mg_procedure.h"

namespace mgp {

class TextSearchException : public std::exception {
 public:
  explicit TextSearchException(std::string message) : message_(std::move(message)) {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

class IndexException : public std::exception {
 public:
  explicit IndexException(std::string message) : message_(std::move(message)) {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

class ValueException : public std::exception {
 public:
  explicit ValueException(std::string message) : message_(std::move(message)) {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

class NotFoundException : public std::exception {
 public:
  explicit NotFoundException(std::string message) : message_(std::move(message)) {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

class MustAbortException : public std::exception {
 public:
  explicit MustAbortException(std::string message) : message_(std::move(message)) {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

class TerminatedMustAbortException : public MustAbortException {
 public:
  explicit TerminatedMustAbortException() : MustAbortException("Query was asked to terminate directly.") {}
};

class ShutdownMustAbortException : public MustAbortException {
 public:
  explicit ShutdownMustAbortException() : MustAbortException("Query was asked to because of server shutdown.") {}
};

class TimeoutMustAbortException : public MustAbortException {
 public:
  explicit TimeoutMustAbortException() : MustAbortException("Query was asked to because of timeout was hit.") {}
};

// Forward declarations
class Nodes;
using GraphNodes = Nodes;
class GraphRelationships;
class Relationships;
class Node;
class Relationship;
struct MapItem;
class Duration;
class Value;

struct StealType {};
inline constexpr StealType steal{};

class MemoryDispatcher final {
 public:
  MemoryDispatcher() = default;
  ~MemoryDispatcher() = default;
  MemoryDispatcher(const MemoryDispatcher &) = delete;
  MemoryDispatcher(MemoryDispatcher &&) = delete;
  MemoryDispatcher &operator=(const MemoryDispatcher &) = delete;
  MemoryDispatcher &operator=(MemoryDispatcher &&) = delete;

  mgp_memory *GetMemoryResource() noexcept {
    const auto this_id = std::this_thread::get_id();
    std::shared_lock lock(mut_);
    return map_[this_id];
  }

  void Register(mgp_memory *mem) noexcept {
    const auto this_id = std::this_thread::get_id();
    std::unique_lock lock(mut_);
    map_[this_id] = mem;
  }

  void UnRegister() noexcept {
    const auto this_id = std::this_thread::get_id();
    std::unique_lock lock(mut_);
    map_.erase(this_id);
  }

  bool IsThisThreadRegistered() noexcept {
    const auto this_id = std::this_thread::get_id();
    std::shared_lock lock(mut_);
    return map_.contains(this_id);
  }

 private:
  std::unordered_map<std::thread::id, mgp_memory *> map_;
  std::shared_mutex mut_;
};

// The use of this object, with the help of MemoryDispatcherGuard
// should be the prefered way to pass the memory pointer to this
// header. The use of the 'mgp_memory *memory' pointer is deprecated
// and will be removed in upcoming releases.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
inline MemoryDispatcher mrd{};

// TODO - Once we deprecate this we should remove this
// and make sure nothing relies on it anymore. This alone
// can not guarantee threadsafe use of query procedures.
inline mgp_memory *memory{nullptr};

class MemoryDispatcherGuard final {
 public:
  explicit MemoryDispatcherGuard(mgp_memory *mem) { mrd.Register(mem); };

  MemoryDispatcherGuard(const MemoryDispatcherGuard &) = delete;
  MemoryDispatcherGuard(MemoryDispatcherGuard &&) = delete;
  MemoryDispatcherGuard &operator=(const MemoryDispatcherGuard &) = delete;
  MemoryDispatcherGuard &operator=(MemoryDispatcherGuard &&) = delete;

  ~MemoryDispatcherGuard() { mrd.UnRegister(); }
};

// Currently we want to preserve both ways(using mgp::memory and
// MemoryDispatcherGuard) of setting the correct memory resource
// from the shared object files. This forwarding function is a
// helper function for that purpose. Once we get rid of the
// 'mgp_memory *memory' pointer this function will not be needed
// anymore and the calls to the memory resource should rely on
// the mapping instead.
template <typename Func, typename... Args>
inline decltype(auto) MemHandlerCallback(Func &&func, Args &&...args) {
  if (!mrd.IsThisThreadRegistered()) {
    return std::forward<Func>(func)(std::forward<Args>(args)..., memory);
  }
  return std::forward<Func>(func)(std::forward<Args>(args)..., mrd.GetMemoryResource());
}

/* #region Graph (Id, Graph, Nodes, GraphRelationships, Relationships & Labels) */

/// Wrapper for int64_t IDs to prevent dangerous implicit conversions.
class Id {
 public:
  Id() = default;

  /// Construct Id from uint64_t
  static Id FromUint(uint64_t id);
  /// Construct Id from int64_t
  static Id FromInt(int64_t id);

  int64_t AsInt() const;
  uint64_t AsUint() const;

  bool operator==(const Id &other) const;
  bool operator!=(const Id &other) const;

  bool operator<(const Id &other) const;

 private:
  explicit Id(int64_t id);

  int64_t id_;
};

enum class AbortReason : uint8_t {
  NO_ABORT = 0,

  // transaction has been requested to terminate, ie. "TERMINATE TRANSACTIONS ..."
  TERMINATED = 1,

  // server is gracefully shutting down
  SHUTDOWN = 2,

  // the transaction timeout has been reached. Either via "--query-execution-timeout-sec", or a per-transaction timeout
  TIMEOUT = 3,
};

/// @brief Wrapper class for @ref mgp_graph.
class Graph {
 private:
  friend class Node;
  friend class Relationship;

 public:
  explicit Graph(mgp_graph *graph);

  /// @brief Returns the graph order (number of nodes).
  int64_t Order() const;
  /// @brief Returns the graph size (number of relationships).
  int64_t Size() const;

  /// @brief Returns an iterable structure of the graph’s nodes.
  GraphNodes Nodes() const;
  /// @brief Returns an iterable structure of the graph’s relationships.
  GraphRelationships Relationships() const;

  /// @brief Returns the graph node with the given ID.
  Node GetNodeById(Id node_id) const;

  /// @brief Returns whether the graph contains a node with the given ID.
  bool ContainsNode(Id node_id) const;
  /// @brief Returns whether the graph contains the given node.
  bool ContainsNode(const Node &node) const;
  /// @brief Returns whether the graph contains a relationship with the given ID.
  bool ContainsRelationship(Id relationship_id) const;
  /// @brief Returns whether the graph contains the given relationship.
  bool ContainsRelationship(const Relationship &relationship) const;

  /// @brief Returns whether the graph is mutable.
  bool IsMutable() const;
  /// @brief Returns whether the graph is in a transactional storage mode.
  bool IsTransactional() const;
  /// @brief Creates a node and adds it to the graph.
  Node CreateNode();
  /// @brief Deletes a node from the graph.
  void DeleteNode(const Node &node);
  /// @brief Deletes a node and all its incident edges from the graph.
  void DetachDeleteNode(const Node &node);
  /// @brief Creates a relationship of type `type` between nodes `from` and `to` and adds it to the graph.
  Relationship CreateRelationship(const Node &from, const Node &to, std::string_view type);
  /// @brief Changes a relationship from node.
  void SetFrom(Relationship &relationship, const Node &new_from);
  /// @brief Changes a relationship to node.
  void SetTo(Relationship &relationship, const Node &new_to);
  /// @brief Changes the relationship type.
  void ChangeType(Relationship &relationship, std::string_view new_type);
  /// @brief Deletes a relationship from the graph.
  void DeleteRelationship(const Relationship &relationship);

  /// @brief Checks if process must abort
  /// @return AbortReason the reason to abort, if no need to abort then AbortReason::NO_ABORT is returned
  AbortReason MustAbort() const;

  /// @brief Checks if process must abort
  /// @throws MustAbortException If process must abort for any reason
  /// @note For the reason why the process must abort consider using MustAbort method instead
  void CheckMustAbort() const;

 private:
  mgp_graph *graph_;
};

/// @brief View of graph nodes; wrapper class for @ref mgp_vertices_iterator.
class Nodes {
 public:
  explicit Nodes(mgp_vertices_iterator *nodes_iterator);

  class Iterator {
   public:
    friend class Nodes;

    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = const Node;
    using pointer = value_type *;
    using reference = value_type &;

    explicit Iterator(mgp_vertices_iterator *nodes_iterator);

    Iterator(const Iterator &other) noexcept;
    Iterator &operator=(const Iterator &other) = delete;

    ~Iterator();

    Iterator &operator++();

    Iterator operator++(int);

    bool operator==(Iterator other) const;
    bool operator!=(Iterator other) const;

    Node operator*() const;

   private:
    mgp_vertices_iterator *nodes_iterator_ = nullptr;
    size_t index_ = 0;
  };

  Iterator begin() const;
  Iterator end() const;

  Iterator cbegin() const;
  Iterator cend() const;

 private:
  mgp_vertices_iterator *nodes_iterator_ = nullptr;
};

/// @brief View of graph relationships.
// NB: Necessary because of the MGP API not having a method that returns a mgp_edges_iterator over all graph
// relationships.
class GraphRelationships {
 public:
  explicit GraphRelationships(mgp_graph *graph);

  class Iterator {
   public:
    friend class GraphRelationships;

    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = const Relationship;
    using pointer = value_type *;
    using reference = value_type &;

    explicit Iterator(mgp_vertices_iterator *nodes_iterator);

    Iterator(const Iterator &other) noexcept;
    Iterator &operator=(const Iterator &other) = delete;

    ~Iterator();

    Iterator &operator++();
    Iterator operator++(int);

    bool operator==(Iterator other) const;
    bool operator!=(Iterator other) const;

    Relationship operator*() const;

   private:
    mgp_vertices_iterator *nodes_iterator_ = nullptr;
    mgp_edges_iterator *out_relationships_iterator_ = nullptr;
    size_t index_ = 0;
  };

  Iterator begin() const;
  Iterator end() const;

  Iterator cbegin() const;
  Iterator cend() const;

 private:
  mgp_graph *graph_;
};

/// @brief Wrapper class for @ref mgp_edges_iterator.
class Relationships {
 public:
  explicit Relationships(mgp_edges_iterator *relationships_iterator);

  class Iterator {
   public:
    friend class Relationships;

    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = const Relationship;
    using pointer = value_type *;
    using reference = value_type &;

    explicit Iterator(mgp_edges_iterator *relationships_iterator);

    Iterator(const Iterator &other) noexcept;
    Iterator &operator=(const Iterator &other) = delete;

    ~Iterator();

    Iterator &operator++();
    Iterator operator++(int);

    bool operator==(Iterator other) const;
    bool operator!=(Iterator other) const;

    Relationship operator*() const;

   private:
    mgp_edges_iterator *relationships_iterator_ = nullptr;
    size_t index_ = 0;
  };

  Iterator begin() const;
  Iterator end() const;

  Iterator cbegin() const;
  Iterator cend() const;

 private:
  mgp_edges_iterator *relationships_iterator_ = nullptr;
};

/// @brief View of node labels.
class Labels {
 public:
  explicit Labels(mgp_vertex *node_ptr);

  Labels(const Labels &other) noexcept;
  Labels(Labels &&other) noexcept;

  Labels &operator=(const Labels &other) noexcept;
  Labels &operator=(Labels &&other) noexcept;

  ~Labels();

  /// @brief Returns the number of the labels, i.e. the size of their list.
  size_t Size() const;

  /// @brief Return the node’s label at position `index`.
  std::string_view operator[](size_t index) const;

  class Iterator {
   private:
    friend class Labels;

   public:
    using value_type = Labels;
    using difference_type = std::ptrdiff_t;
    using pointer = const Labels *;
    using reference = const Labels &;
    using iterator_category = std::forward_iterator_tag;

    bool operator==(const Iterator &other) const;

    bool operator!=(const Iterator &other) const;

    Iterator &operator++();

    std::string_view operator*() const;

   private:
    Iterator(const Labels *iterable, size_t index);

    const Labels *iterable_;
    size_t index_;
  };

  Iterator begin();
  Iterator end();

  Iterator cbegin();
  Iterator cend();

 private:
  mgp_vertex *node_ptr_;
};

/* #endregion */

/* #region Types */

/* #region Containers (List, Map) */

/// @brief Wrapper class for @ref mgp_list.
class List {
 private:
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

 public:
  /// @brief Creates a List from the copy of the given @ref mgp_list.
  explicit List(mgp_list *ptr);
  /// @brief Creates a List from the copy of the given @ref mgp_list.
  explicit List(const mgp_list *const_ptr);

  /// @brief Creates an empty List.
  explicit List();

  /// @brief Creates a List with the given `capacity`.
  explicit List(size_t capacity);

  /// @brief Creates a List from the given vector.
  explicit List(const std::vector<Value> &values);
  /// @brief Creates a List from the given vector.
  explicit List(std::vector<Value> &&values);

  /// @brief Creates a List from the given initializer_list.
  explicit List(std::initializer_list<Value> list);

  List(const List &other) noexcept;
  List(List &&other) noexcept;

  List &operator=(const List &other) noexcept;
  List &operator=(List &&other) noexcept;

  ~List();

  /// @brief Returns wheter the list contains any deleted values.
  bool ContainsDeleted() const;

  /// @brief Returns the size of the list.
  size_t Size() const;
  /// @brief Returns whether the list is empty.
  bool Empty() const;

  /// @brief Returns the value at the given `index`.
  Value operator[](size_t index) const;

  ///@brief Same as above, but non const value
  Value operator[](size_t index);

  class Iterator {
   private:
    friend class List;

   public:
    using value_type = List;
    using difference_type = std::ptrdiff_t;
    using pointer = const List *;
    using reference = const List &;
    using iterator_category = std::forward_iterator_tag;

    bool operator==(const Iterator &other) const;

    bool operator!=(const Iterator &other) const;

    Iterator &operator++();

    Value operator*() const;

   private:
    Iterator(const List *iterable, size_t index);

    const List *iterable_;
    size_t index_;
  };

  Iterator begin() const;
  Iterator end() const;

  Iterator cbegin() const;
  Iterator cend() const;

  /// @brief Appends the given `value` to the list. The `value` is copied.
  void Append(const Value &value);
  /// @brief Appends the given `value` to the list.
  /// @note Takes the ownership of `value` by moving it. The behavior of accessing `value` after performing this
  /// operation is undefined.
  void Append(Value &&value);

  /// @brief Extends the list and appends the given `value` to it. The `value` is copied.
  void AppendExtend(const Value &value);
  /// @brief Extends the list and appends the given `value` to it.
  /// @note Takes the ownership of `value` by moving it. The behavior of accessing `value` after performing this
  /// operation is undefined.
  void AppendExtend(Value &&value);

  // Value Pop();  // not implemented (requires mgp_list_pop in the MGP API):

  /// @exception std::runtime_error List contains value of unknown type.
  bool operator==(const List &other) const;
  /// @exception std::runtime_error List contains value of unknown type.
  bool operator!=(const List &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_list *ptr_;
};

/// @brief Wrapper class for @ref mgp_map.
class Map {
 private:
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

 public:
  /// @brief Creates a Map from the copy of the given @ref mgp_map.
  explicit Map(mgp_map *ptr);

  /// @brief Creates a Map from the copy of the given @ref mgp_map.
  explicit Map(const mgp_map *const_ptr);

  /// @brief Creates an empty Map.
  explicit Map();

  /// @brief Creates a Map from the given vector.
  explicit Map(const std::map<std::string_view, Value> &items);

  /// @brief Creates a Map from the given vector.
  explicit Map(std::map<std::string_view, Value> &&items);

  /// @brief Creates a Map from the given initializer_list (map items correspond to initializer list pairs).
  Map(std::initializer_list<std::pair<std::string_view, Value>> items);

  Map(const Map &other) noexcept;
  Map(Map &&other) noexcept;

  Map &operator=(const Map &other) noexcept;
  Map &operator=(Map &&other) noexcept;

  ~Map();

  /// @brief Returns wheter the map contains any deleted values.
  bool ContainsDeleted() const;

  /// @brief Returns the size of the map.
  size_t Size() const;

  /// @brief Returns whether the map is empty.
  bool Empty() const;

  /// @brief Returns the value at the given `key`.
  Value operator[](std::string_view key) const;

  /// @brief Returns the value at the given `key`.
  Value At(std::string_view key) const;

  /// @brief Returns true if the given `key` exists.
  bool KeyExists(std::string_view key) const;

  class Iterator {
   public:
    friend class Map;

    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = const MapItem;
    using pointer = value_type *;
    using reference = value_type &;

    explicit Iterator(mgp_map_items_iterator *map_items_iterator);

    Iterator(const Iterator &other) noexcept;
    Iterator &operator=(const Iterator &other) = delete;

    ~Iterator();

    Iterator &operator++();
    Iterator operator++(int);

    bool operator==(Iterator other) const;
    bool operator!=(Iterator other) const;

    MapItem operator*() const;

   private:
    mgp_map_items_iterator *map_items_iterator_ = nullptr;
  };

  Iterator begin() const;
  Iterator end() const;

  Iterator cbegin() const;
  Iterator cend() const;

  /// @brief Inserts the given `key`-`value` pair into the map. The `value` is copied.
  void Insert(std::string_view key, const Value &value);

  /// @brief Inserts the given `key`-`value` pair into the map.
  /// @note Takes the ownership of `value` by moving it. The behavior of accessing `value` after performing this
  /// operation is undefined.
  void Insert(std::string_view key, Value &&value);

  /// @brief Updates the `key`-`value` pair in the map. If the key doesn't exist, the value gets inserted. The `value`
  /// is copied.
  void Update(std::string_view key, const Value &value);

  /// @brief Updates the `key`-`value` pair in the map. If the key doesn't exist, the value gets inserted. The `value`
  /// is copied.
  /// @note Takes the ownership of `value` by moving it. The behavior of accessing `value` after performing this
  /// operation is undefined.
  void Update(std::string_view key, Value &&value);

  /// @brief Erases the element associated with the key from the map, if it doesn't exist does nothing.
  void Erase(std::string_view key);

  // void Clear();  // not implemented (requires mgp_map_clear in the MGP API)

  /// @exception std::runtime_error Map contains value of unknown type.
  bool operator==(const Map &other) const;

  /// @exception std::runtime_error Map contains value of unknown type.
  bool operator!=(const Map &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_map *ptr_;
};
/* #endregion */

/* #region Graph elements (Node, Relationship & Path) */
/// @brief Wrapper class for @ref mgp_vertex.
class Node {
 public:
  friend class Graph;
  friend class Path;
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

  /// @brief Creates a Node from the copy of the given @ref mgp_vertex.
  explicit Node(mgp_vertex *ptr);

  /// @brief Creates a Node from the copy of the given @ref mgp_vertex.
  explicit Node(const mgp_vertex *const_ptr);

  Node(const Node &other) noexcept;
  Node(Node &&other) noexcept;

  Node &operator=(const Node &other) noexcept;
  Node &operator=(Node &&other) noexcept;

  ~Node();

  /// @brief Returns wheter the node has been deleted.
  bool IsDeleted() const;

  /// @brief Returns the node’s ID.
  mgp::Id Id() const;

  /// @brief Returns an iterable & indexable structure of the node’s labels.
  mgp::Labels Labels() const;

  /// @brief Returns whether the node has the given `label`.
  bool HasLabel(std::string_view label) const;

  /// @brief Returns an std::map of the node’s properties.
  std::unordered_map<std::string, Value> Properties() const;

  /// @brief Sets the chosen property to the given value.
  void SetProperty(std::string property, Value value);

  /// @brief Sets the chosen properties to the given values.
  void SetProperties(std::unordered_map<std::string_view, Value> properties);

  /// @brief Removes the chosen property.
  void RemoveProperty(std::string property);

  /// @brief Retrieves the value of the chosen property.
  Value GetProperty(const std::string &property) const;

  /// @brief Returns an iterable structure of the node’s inbound relationships.
  Relationships InRelationships() const;

  /// @brief Returns an iterable structure of the node’s outbound relationships.
  Relationships OutRelationships() const;

  /// @brief Adds a label to the node.
  void AddLabel(std::string_view label);

  /// @brief Removes a label from the node.
  void RemoveLabel(std::string_view label);

  bool operator<(const Node &other) const;

  /// @exception std::runtime_error Node properties contain value(s) of unknown type.
  bool operator==(const Node &other) const;

  /// @exception std::runtime_error Node properties contain value(s) of unknown type.
  bool operator!=(const Node &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

  /// @brief returns the in degree of a node
  inline size_t InDegree() const;

  /// @brief returns the out degree of a node
  inline size_t OutDegree() const;

 private:
  mgp_vertex *ptr_;
};

/// @brief Wrapper class for @ref mgp_edge.
class Relationship {
 private:
  friend class Graph;
  friend class Path;
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

 public:
  /// @brief Creates a Relationship from the copy of the given @ref mgp_edge.
  explicit Relationship(mgp_edge *ptr);
  /// @brief Creates a Relationship from the copy of the given @ref mgp_edge.
  explicit Relationship(const mgp_edge *const_ptr);

  Relationship(const Relationship &other) noexcept;
  Relationship(Relationship &&other) noexcept;

  Relationship &operator=(const Relationship &other) noexcept;
  Relationship &operator=(Relationship &&other) noexcept;

  ~Relationship();

  /// @brief Returns wheter the relationship has been deleted.
  bool IsDeleted() const;

  /// @brief Returns the relationship’s ID.
  mgp::Id Id() const;

  /// @brief Returns the relationship’s type.
  std::string_view Type() const;

  /// @brief Returns an std::map of the relationship’s properties.
  std::unordered_map<std::string, Value> Properties() const;

  /// @brief Sets the chosen property to the given value.
  void SetProperty(std::string property, Value value);

  /// @brief Sets the chosen properties to the given values.
  void SetProperties(std::unordered_map<std::string_view, Value> properties);

  /// @brief Removes the chosen property.
  void RemoveProperty(std::string property);

  /// @brief Retrieves the value of the chosen property.
  Value GetProperty(const std::string &property) const;

  /// @brief Returns the relationship’s source node.
  Node From() const;
  /// @brief Returns the relationship’s destination node.
  Node To() const;

  bool operator<(const Relationship &other) const;

  /// @exception std::runtime_error Relationship properties contain value(s) of unknown type.
  bool operator==(const Relationship &other) const;
  /// @exception std::runtime_error Relationship properties contain value(s) of unknown type.
  bool operator!=(const Relationship &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_edge *ptr_;
};

/// @brief Wrapper class for @ref mgp_path.
class Path {
 private:
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

 public:
  /// @brief Creates a Path from the copy of the given @ref mgp_path.
  explicit Path(mgp_path *ptr);
  /// @brief Creates a Path from the copy of the given @ref mgp_path.
  explicit Path(const mgp_path *const_ptr);

  /// @brief Creates a Path starting with the given `start_node`.
  explicit Path(const Node &start_node);

  Path(const Path &other) noexcept;
  Path(Path &&other) noexcept;

  Path &operator=(const Path &other) noexcept;
  Path &operator=(Path &&other) noexcept;

  ~Path();

  /// @brief Returns wheter the path contains any deleted values.
  bool ContainsDeleted() const;

  /// Returns the path length (number of relationships).
  size_t Length() const;

  /// @brief Returns the node at the given `index`.
  /// @pre The `index` must be less than or equal to length of the path.
  Node GetNodeAt(size_t index) const;

  /// @brief Returns the relationship at the given `index`.
  /// @pre The `index` must be less than length of the path.
  Relationship GetRelationshipAt(size_t index) const;

  /// @brief Adds a relationship continuing from the last node on the path.
  void Expand(const Relationship &relationship);
  /// @brief Removes the last node and the last relationship from the path.
  void Pop();

  /// @exception std::runtime_error Path contains element(s) with unknown value.
  bool operator==(const Path &other) const;
  /// @exception std::runtime_error Path contains element(s) with unknown value.
  bool operator!=(const Path &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_path *ptr_;
};
/* #endregion */

/* #region Temporal types (Date, LocalTime, LocalDateTime, Duration) */

/// @brief Wrapper class for @ref mgp_date.
class Date {
 private:
  friend class Duration;
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

 public:
  /// @brief Creates a Date object from the copy of the given @ref mgp_date.
  explicit Date(mgp_date *ptr);
  /// @brief Creates a Date object from the copy of the given @ref mgp_date.
  explicit Date(const mgp_date *const_ptr);

  /// @brief Creates a Date object from the given string representing a date in the ISO 8601 format (`YYYY-MM-DD`,
  /// `YYYYMMDD`, or `YYYY-MM`).
  explicit Date(std::string_view string);

  /// @brief Creates a Date object with the given `year`, `month`, and `day` properties.
  Date(int year, int month, int day);

  Date(const Date &other) noexcept;
  Date(Date &&other) noexcept;

  Date &operator=(const Date &other) noexcept;
  Date &operator=(Date &&other) noexcept;

  ~Date();

  /// @brief Returns the current Date.
  static Date Now();

  /// @brief Returns the date’s year property.
  int Year() const;
  /// @brief Returns the date’s month property.
  int Month() const;
  /// @brief Returns the date’s day property.
  int Day() const;

  /// @brief Returns the date’s timestamp (microseconds from the Unix epoch).
  int64_t Timestamp() const;

  bool operator==(const Date &other) const;
  Date operator+(const Duration &dur) const;
  Date operator-(const Duration &dur) const;
  Duration operator-(const Date &other) const;

  bool operator<(const Date &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_date *ptr_;
};

/// @brief Wrapper class for @ref mgp_local_time.
class LocalTime {
 private:
  friend class Duration;
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

 public:
  /// @brief Creates a LocalTime object from the copy of the given @ref mgp_local_time.
  explicit LocalTime(mgp_local_time *ptr);
  /// @brief Creates a LocalTime object from the copy of the given @ref mgp_local_time.
  explicit LocalTime(const mgp_local_time *const_ptr);

  /// @brief Creates a LocalTime object from the given string representing a date in the ISO 8601 format ([T]hh:mm:ss,
  /// `[T]hh:mm`, `[T]hhmmss`, `[T]hhmm`, or `[T]hh`).
  explicit LocalTime(std::string_view string);

  /// @brief Creates a LocalTime object with the given `hour`, `minute`, `second`, `millisecond`, and `microsecond`
  /// properties.
  LocalTime(int hour, int minute, int second, int millisecond, int microsecond);

  LocalTime(const LocalTime &other) noexcept;
  LocalTime(LocalTime &&other) noexcept;

  LocalTime &operator=(const LocalTime &other) noexcept;
  LocalTime &operator=(LocalTime &&other) noexcept;

  ~LocalTime();

  /// @brief Returns the current LocalTime.
  static LocalTime Now();

  /// @brief Returns the object’s `hour` property.
  int Hour() const;
  /// @brief Returns the object’s `minute` property.
  int Minute() const;
  /// @brief Returns the object’s `second` property.
  int Second() const;
  /// @brief Returns the object’s `millisecond` property.
  int Millisecond() const;
  /// @brief Returns the object’s `microsecond` property.
  int Microsecond() const;

  /// @brief Returns the object’s timestamp (microseconds from the Unix epoch).
  int64_t Timestamp() const;

  bool operator==(const LocalTime &other) const;
  LocalTime operator+(const Duration &dur) const;
  LocalTime operator-(const Duration &dur) const;
  Duration operator-(const LocalTime &other) const;

  bool operator<(const LocalTime &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_local_time *ptr_;
};

/// @brief Wrapper class for @ref mgp_local_date_time.
class LocalDateTime {
 private:
  friend class Duration;
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

 public:
  /// @brief Creates a LocalDateTime object from the copy of the given @ref mgp_local_date_time.
  explicit LocalDateTime(mgp_local_date_time *ptr);
  /// @brief Creates a LocalDateTime object from the copy of the given @ref mgp_local_date_time.
  explicit LocalDateTime(const mgp_local_date_time *const_ptr);

  /// @brief Creates a LocalDateTime object from the given string representing a date in the ISO 8601 format
  /// (`YYYY-MM-DDThh:mm:ss`, `YYYY-MM-DDThh:mm`, `YYYYMMDDThhmmss`, `YYYYMMDDThhmm`, or `YYYYMMDDThh`).
  explicit LocalDateTime(std::string_view string);

  /// @brief Creates a LocalDateTime object with the given `year`, `month`, `day`, `hour`, `minute`, `second`,
  /// `millisecond`, and `microsecond` properties.
  LocalDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond, int microsecond);

  LocalDateTime(const LocalDateTime &other) noexcept;
  LocalDateTime(LocalDateTime &&other) noexcept;

  LocalDateTime &operator=(const LocalDateTime &other) noexcept;
  LocalDateTime &operator=(LocalDateTime &&other) noexcept;

  ~LocalDateTime();

  /// @brief Returns the current LocalDateTime.
  static LocalDateTime Now();

  /// @brief Returns the object’s `year` property.
  int Year() const;
  /// @brief Returns the object’s `month` property.
  int Month() const;
  /// @brief Returns the object’s `day` property.
  int Day() const;
  /// @brief Returns the object’s `hour` property.
  int Hour() const;
  /// @brief Returns the object’s `minute` property.
  int Minute() const;
  /// @brief Returns the object’s `second` property.
  int Second() const;
  /// @brief Returns the object’s `millisecond` property.
  int Millisecond() const;
  /// @brief Returns the object’s `microsecond` property.
  int Microsecond() const;

  /// @brief Returns the object’s timestamp (microseconds from the Unix epoch).
  int64_t Timestamp() const;

  bool operator==(const LocalDateTime &other) const;
  LocalDateTime operator+(const Duration &dur) const;
  LocalDateTime operator-(const Duration &dur) const;
  Duration operator-(const LocalDateTime &other) const;

  bool operator<(const LocalDateTime &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_local_date_time *ptr_;
};

/// @brief Wrapper class for @ref mgp_duration.
class Duration {
 private:
  friend class Date;
  friend class LocalTime;
  friend class LocalDateTime;
  friend class Value;
  friend class Record;
  friend class Result;
  friend class Parameter;

 public:
  /// @brief Creates a Duration from the copy of the given @ref mgp_duration.
  explicit Duration(mgp_duration *ptr);
  /// @brief Creates a Duration from the copy of the given @ref mgp_duration.
  explicit Duration(const mgp_duration *const_ptr);

  /// @brief Creates a Duration object from the given string in the following format: `P[nD]T[nH][nM][nS]`, where (1)
  /// `n` stands for a number, (2) capital letters are used as a separator, (3) each field in `[]` is optional, and (4)
  /// only the last field may be a non-integer.
  explicit Duration(std::string_view string);

  /// @brief Creates a Duration object from the given number of microseconds.
  explicit Duration(int64_t microseconds);

  /// @brief Creates a Duration object with the given `day`, `hour`, `minute`, `second`, `millisecond`, and
  /// `microsecond` properties.
  Duration(double day, double hour, double minute, double second, double millisecond, double microsecond);

  Duration(const Duration &other) noexcept;
  Duration(Duration &&other) noexcept;

  Duration &operator=(const Duration &other) noexcept;
  Duration &operator=(Duration &&other) noexcept;

  ~Duration();

  /// @brief Returns the duration as microseconds.
  int64_t Microseconds() const;

  bool operator==(const Duration &other) const;
  Duration operator+(const Duration &other) const;
  Duration operator-(const Duration &other) const;
  Duration operator-() const;

  bool operator<(const Duration &other) const;

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_duration *ptr_;
};
/* #endregion */

/* #endregion */

/* #region Value */
enum class Type : uint8_t {
  Null,
  Any,
  Bool,
  Int,
  Double,
  String,
  List,
  Map,
  Node,
  Relationship,
  Path,
  Date,
  LocalTime,
  LocalDateTime,
  Duration
};

/// @brief Wrapper class for @ref mgp_value.
class Value {
 public:
  friend class List;
  friend class Map;
  friend class Date;
  friend class LocalTime;
  friend class LocalDateTime;
  friend class Duration;
  friend class Record;
  friend class Result;

  explicit Value(mgp_value *ptr);

  explicit Value(StealType /*steal*/, mgp_value *ptr);

  // Null constructor:
  explicit Value();

  // Primitive type constructors:
  explicit Value(bool value);
  explicit Value(int64_t value);
  explicit Value(double value);

  // String constructors:
  explicit Value(const char *value);
  explicit Value(std::string_view value);
  // Container constructors:

  /// @brief Constructs a List value from the copy of the given `list`.
  explicit Value(const List &list);
  /// @note The behavior of accessing `list` after performing this operation is undefined.
  explicit Value(List &&list);

  /// @brief Constructs a Map value from the copy of the given `map`.
  explicit Value(const Map &map);
  /// @brief Constructs a Map value and takes ownership of the given `map`.
  /// @note The behavior of accessing `map` after performing this operation is undefined.
  explicit Value(Map &&map);

  // Graph element type constructors:

  /// @brief Constructs a Node value from the copy of the given `node`.
  explicit Value(const Node &node);
  /// @brief Constructs a Node value and takes ownership of the given `node`.
  /// @note The behavior of accessing `node` after performing this operation is undefined.
  explicit Value(Node &&node);

  /// @brief Constructs a Relationship value from the copy of the given `node`.
  explicit Value(const Relationship &relationship);
  /// @brief Constructs a Relationship value and takes ownership of the given `relationship`.
  /// @note The behavior of accessing `relationship` after performing this operation is undefined.
  explicit Value(Relationship &&relationship);

  /// @brief Constructs a Path value from the copy of the given `path`.
  explicit Value(const Path &path);
  /// @brief Constructs a Path value and takes ownership of the given `path`.
  /// @note The behavior of accessing `path` after performing this operation is undefined.
  explicit Value(Path &&path);

  // Temporal type constructors:

  /// @brief Constructs a Date value from the copy of the given `date`.
  explicit Value(const Date &date);
  /// @brief Constructs a Date value and takes ownership of the given `path`.
  /// @note The behavior of accessing `date` after performing this operation is undefined.
  explicit Value(Date &&date);

  /// @brief Constructs a LocalTime value from the copy of the given `local_time`.
  explicit Value(const LocalTime &local_time);
  /// @brief Constructs a LocalTime value and takes ownership of the given `local_time`.
  /// @note The behavior of accessing `local_time` after performing this operation is undefined.
  explicit Value(LocalTime &&local_time);

  /// @brief Constructs a LocalDateTime value from the copy of the given `local_date_time`.
  explicit Value(const LocalDateTime &local_date_time);
  /// @brief Constructs a LocalDateTime value and takes ownership of the given `local_date_time`.
  /// @note The behavior of accessing `local_date_time` after performing this operation is undefined.
  explicit Value(LocalDateTime &&local_date_time);

  /// @brief Constructs a Duration value from the copy of the given `duration`.
  explicit Value(const Duration &duration);
  /// @brief Constructs a Duration value and takes ownership of the given `duration`.
  /// @note The behavior of accessing `duration` after performing this operation is undefined.
  explicit Value(Duration &&duration);

  Value(const Value &other) noexcept;
  Value(Value &&other) noexcept;

  Value &operator=(const Value &other) noexcept;
  Value &operator=(Value &&other) noexcept;

  ~Value();

  /// @brief Returns the pointer to the stored value.
  mgp_value *ptr() const;

  /// @brief Returns the type of the value.
  /// @exception std::runtime_error The value type is unknown.
  mgp::Type Type() const;

  /// @pre Value type needs to be Type::Bool.
  bool ValueBool() const;
  bool ValueBool();
  /// @pre Value type needs to be Type::Int.
  int64_t ValueInt() const;
  int64_t ValueInt();
  /// @pre Value type needs to be Type::Double.
  double ValueDouble() const;
  double ValueDouble();
  /// @pre Value type needs to be Type::Numeric.
  double ValueNumeric() const;
  double ValueNumeric();
  /// @pre Value type needs to be Type::String.
  std::string_view ValueString() const;
  std::string_view ValueString();
  /// @pre Value type needs to be Type::List.
  List ValueList() const;
  List ValueList();
  /// @pre Value type needs to be Type::Map.
  Map ValueMap() const;
  Map ValueMap();
  /// @pre Value type needs to be Type::Node.
  Node ValueNode() const;
  Node ValueNode();
  /// @pre Value type needs to be Type::Relationship.
  Relationship ValueRelationship() const;
  Relationship ValueRelationship();
  /// @pre Value type needs to be Type::Path.
  Path ValuePath() const;
  Path ValuePath();
  /// @pre Value type needs to be Type::Date.
  Date ValueDate() const;
  Date ValueDate();
  /// @pre Value type needs to be Type::LocalTime.
  LocalTime ValueLocalTime() const;
  LocalTime ValueLocalTime();
  /// @pre Value type needs to be Type::LocalDateTime.
  LocalDateTime ValueLocalDateTime() const;
  LocalDateTime ValueLocalDateTime();
  /// @pre Value type needs to be Type::Duration.
  Duration ValueDuration() const;
  Duration ValueDuration();

  /// @brief Returns whether the value is null.
  bool IsNull() const;
  /// @brief Returns whether the value is boolean.
  bool IsBool() const;
  /// @brief Returns whether the value is an integer.
  bool IsInt() const;
  /// @brief Returns whether the value is a floating-point number.
  bool IsDouble() const;
  /// @brief Returns whether the value is numeric.
  bool IsNumeric() const;
  /// @brief Returns whether the value is a string.
  bool IsString() const;
  /// @brief Returns whether the value is a @ref List.
  bool IsList() const;
  /// @brief Returns whether the value is a @ref Map.
  bool IsMap() const;
  /// @brief Returns whether the value is a @ref Node.
  bool IsNode() const;
  /// @brief Returns whether the value is a @ref Relationship.
  bool IsRelationship() const;
  /// @brief Returns whether the value is a @ref Path.
  bool IsPath() const;
  /// @brief Returns whether the value is a @ref Date object.
  bool IsDate() const;
  /// @brief Returns whether the value is a @ref LocalTime object.
  bool IsLocalTime() const;
  /// @brief Returns whether the value is a @ref LocalDateTime object.
  bool IsLocalDateTime() const;
  /// @brief Returns whether the value is a @ref Duration object.
  bool IsDuration() const;

  /// @exception std::runtime_error Unknown value type.
  bool operator==(const Value &other) const;
  /// @exception std::runtime_error Unknown value type.
  bool operator!=(const Value &other) const;

  bool operator<(const Value &other) const;

  friend std::ostream &operator<<(std::ostream &os, const mgp::Value &value);

  /// @brief returns the string representation
  std::string ToString() const;

 private:
  mgp_value *ptr_;
};

/// @brief Key-value pair representing @ref Map items.
struct MapItem {
  const std::string_view key;
  const Value value;

  bool operator==(MapItem &other) const;
  bool operator!=(MapItem &other) const;

  bool operator<(const MapItem &other) const;
};

/* #endregion */

/* #region Results */

/// @brief Procedure result class
class Record {
 public:
  explicit Record(mgp_result_record *record);

  /// @brief Inserts a boolean value under field `field_name`.
  void Insert(const char *field_name, bool value);
  /// @brief Inserts an integer value under field `field_name`.
  void Insert(const char *field_name, std::int64_t value);
  /// @brief Inserts a floating-point value under field `field_name`.
  void Insert(const char *field_name, double value);
  /// @brief Inserts a string value under field `field_name`.
  void Insert(const char *field_name, std::string_view value);
  /// @brief Inserts a string value under field `field_name`.
  void Insert(const char *field_name, const char *value);
  /// @brief Inserts a @ref List value under field `field_name`.
  void Insert(const char *field_name, const List &list);
  /// @brief Inserts a @ref Map value under field `field_name`.
  void Insert(const char *field_name, const Map &map);
  /// @brief Inserts a @ref Node value under field `field_name`.
  void Insert(const char *field_name, const Node &node);
  /// @brief Inserts a @ref Relationship value under field `field_name`.
  void Insert(const char *field_name, const Relationship &relationship);
  /// @brief Inserts a @ref Path value under field `field_name`.
  void Insert(const char *field_name, const Path &path);
  /// @brief Inserts a @ref Date value under field `field_name`.
  void Insert(const char *field_name, const Date &date);
  /// @brief Inserts a @ref LocalTime value under field `field_name`.
  void Insert(const char *field_name, const LocalTime &local_time);
  /// @brief Inserts a @ref LocalDateTime value under field `field_name`.
  void Insert(const char *field_name, const LocalDateTime &local_date_time);
  /// @brief Inserts a @ref Duration value under field `field_name`.
  void Insert(const char *field_name, const Duration &duration);
  /// @brief Inserts a @ref Value value under field `field_name`, and then call appropriate insert.
  void Insert(const char *field_name, const Value &value);

 private:
  mgp_result_record *record_;
};

/// @brief Factory class for @ref Record
class RecordFactory {
 public:
  explicit RecordFactory(mgp_result *result);

  Record NewRecord() const;

  void SetErrorMessage(std::string_view error_msg) const;

  void SetErrorMessage(const char *error_msg) const;

 private:
  mgp_result *result_;
};

/// @brief Function result class
class Result {
 public:
  explicit Result(mgp_func_result *result);

  /// @brief Sets a boolean value to be returned.
  inline void SetValue(bool value);
  /// @brief Sets an integer value to be returned.
  inline void SetValue(std::int64_t value);
  /// @brief Sets a floating-point value to be returned.
  inline void SetValue(double value);
  /// @brief Sets a string value to be returned.
  inline void SetValue(std::string_view value);
  /// @brief Sets a string value to be returned.
  inline void SetValue(const char *value);
  /// @brief Sets a @ref List value to be returned.
  inline void SetValue(const List &list);
  /// @brief Sets a @ref Map value to be returned.
  inline void SetValue(const Map &map);
  /// @brief Sets a @ref Node value to be returned.
  inline void SetValue(const Node &node);
  /// @brief Sets a @ref Relationship value to be returned.
  inline void SetValue(const Relationship &relationship);
  /// @brief Sets a @ref Path value to be returned.
  inline void SetValue(const Path &path);
  /// @brief Sets a @ref Date value to be returned.
  inline void SetValue(const Date &date);
  /// @brief Sets a @ref LocalTime value to be returned.
  inline void SetValue(const LocalTime &local_time);
  /// @brief Sets a @ref LocalDateTime value to be returned.
  inline void SetValue(const LocalDateTime &local_date_time);
  /// @brief Sets a @ref Duration value to be returned.
  inline void SetValue(const Duration &duration);

  void SetErrorMessage(std::string_view error_msg) const;

  void SetErrorMessage(const char *error_msg) const;

 private:
  mgp_func_result *result_;
};

/* #endregion */

/* #region Module */

/// @brief Represents a procedure’s parameter. Parameters are defined by their name, type, and (if optional) default
/// value.
class Parameter {
 public:
  std::string_view name;
  Type type_;
  Type list_item_type_;

  bool optional = false;
  Value default_value;

  /// @brief Creates a non-optional parameter with the given `name` and `type`.
  Parameter(std::string_view name, Type type);

  /// @brief Creates an optional boolean parameter with the given `name` and `default_value`.
  Parameter(std::string_view name, Type type, bool default_value);

  /// @brief Creates an optional integer parameter with the given `name` and `default_value`.
  Parameter(std::string_view name, Type type, int64_t default_value);

  /// @brief Creates an optional floating-point parameter with the given `name` and `default_value`.
  Parameter(std::string_view name, Type type, double default_value);

  /// @brief Creates an optional string parameter with the given `name` and `default_value`.
  Parameter(std::string_view name, Type type, std::string_view default_value);

  /// @brief Creates an optional string parameter with the given `name` and `default_value`.
  Parameter(std::string_view name, Type type, const char *default_value);

  /// @brief Creates an optional parameter with the given `name` and `default_value`.
  Parameter(std::string_view name, Type type, Value default_value);

  /// @brief Creates a non-optional ListParameter with the given `name` and `item_type`.
  Parameter(std::string_view name, std::pair<Type, Type> list_type);

  /// @brief Creates an optional List parameter with the given `name`, `item_type`, and `default_value`.
  Parameter(std::string_view name, std::pair<Type, Type> list_type, Value default_value);

  mgp_type *GetMGPType() const;
};

/// @brief Represents a procedure’s return value. The values are defined by their name and type.
class Return {
 public:
  std::string_view name;
  Type type_;
  Type list_item_type_;

  /// @brief Creates a return value with the given `name` and `type`.
  Return(std::string_view name, Type type);

  Return(std::string_view name, std::pair<Type, Type> list_type);

  mgp_type *GetMGPType() const;
};

enum class ProcedureType : uint8_t {
  Read,
  Write,
};

/// @brief Adds a procedure to the query module.
/// @param callback - procedure callback
/// @param name - procedure name
/// @param proc_type - procedure type (read/write)
/// @param parameters - procedure parameters
/// @param returns - procedure return values
/// @param module - the query module that the procedure is added to
/// @param memory - access to memory
inline void AddProcedure(mgp_proc_cb callback, std::string_view name, ProcedureType proc_type,
                         std::vector<Parameter> parameters, std::vector<Return> returns, mgp_module *module,
                         mgp_memory *memory);

/// @brief Adds a batch procedure to the query module.
/// @param callback - procedure callback
/// @param initializer - procedure initializer
/// @param cleanup - procedure cleanup
/// @param name - procedure name
/// @param proc_type - procedure type (read/write)
/// @param parameters - procedure parameters
/// @param returns - procedure return values
/// @param module - the query module that the procedure is added to
/// @param memory - access to memory
inline void AddBatchProcedure(mgp_proc_cb callback, mgp_proc_initializer initializer, mgp_proc_cleanup cleanup,
                              std::string_view name, ProcedureType proc_type, std::vector<Parameter> parameters,
                              std::vector<Return> returns, mgp_module *module, mgp_memory *memory);

/// @brief Adds a function to the query module.
/// @param callback - function callback
/// @param name - function name
/// @param parameters - function parameters
/// @param module - the query module that the function is added to
/// @param memory - access to memory
inline void AddFunction(mgp_func_cb callback, std::string_view name, std::vector<Parameter> parameters,
                        mgp_module *module, mgp_memory *memory);

/* #endregion */

namespace util {
inline uint64_t Fnv(const std::string_view s) {
  // fnv1a is recommended so use it as the default implementation.
  uint64_t hash = 14695981039346656037UL;

  for (const auto &ch : s) {
    hash = (hash ^ (uint64_t)ch) * 1099511628211UL;
  }

  return hash;
}

/**
 * Does FNV-like hashing on a collection. Not truly FNV
 * because it operates on 8-bit elements, while this
 * implementation uses size_t elements (collection item
 * hash).
 *
 * https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
 *
 *
 * @tparam TIterable A collection type that has begin() and end().
 * @tparam TElement Type of element in the collection.
 * @tparam THash Hash type (has operator() that accepts a 'const TEelement &'
 *  and returns size_t. Defaults to std::hash<TElement>.
 * @param iterable A collection of elements.
 * @param element_hash Function for hashing a single element.
 * @return The hash of the whole collection.
 */
template <typename TIterable, typename TElement, typename THash = std::hash<TElement>>
struct FnvCollection {
  size_t operator()(const TIterable &iterable) const {
    uint64_t hash = 14695981039346656037u;
    THash element_hash;
    for (const TElement &element : iterable) {
      hash *= fnv_prime;
      hash ^= element_hash(element);
    }
    return hash;
  }

 private:
  static const uint64_t fnv_prime = 1099511628211u;
};

/**
 * Like FNV hashing for a collection, just specialized for two elements to avoid
 * iteration overhead.
 */
template <typename TA, typename TB, typename TAHash = std::hash<TA>, typename TBHash = std::hash<TB>>
struct HashCombine {
  size_t operator()(const TA &a, const TB &b) const {
    static constexpr size_t fnv_prime = 1099511628211UL;
    static constexpr size_t fnv_offset = 14695981039346656037UL;
    size_t ret = fnv_offset;
    ret ^= TAHash()(a);
    ret *= fnv_prime;
    ret ^= TBHash()(b);
    return ret;
  }
};

// uint to int conversion in C++ is a bit tricky. Take a look here
// https://stackoverflow.com/questions/14623266/why-cant-i-reinterpret-cast-uint-to-int
// for more details.
template <typename TDest, typename TSrc>
TDest MemcpyCast(TSrc src) {
  TDest dest;
  static_assert(sizeof(dest) == sizeof(src), "MemcpyCast expects source and destination to be of the same size");
  static_assert(std::is_arithmetic_v<TSrc>, "MemcpyCast expects source to be an arithmetic type");
  static_assert(std::is_arithmetic_v<TDest>, "MemcpyCast expects destination to be an arithmetic type");
  std::memcpy(&dest, &src, sizeof(src));
  return dest;
}

/// @brief Returns whether two MGP API values are equal.
inline bool ValuesEqual(mgp_value *value1, mgp_value *value2);

/// @brief Returns whether two MGP API lists are equal.
inline bool ListsEqual(mgp_list *list1, mgp_list *list2) {
  if (list1 == list2) {
    return true;
  }
  if (mgp::list_size(list1) != mgp::list_size(list2)) {
    return false;
  }
  const size_t len = mgp::list_size(list1);
  for (size_t i = 0; i < len; ++i) {
    if (!util::ValuesEqual(mgp::list_at(list1, i), mgp::list_at(list2, i))) {
      return false;
    }
  }
  return true;
}

/// @brief Returns whether two MGP API maps are equal.
inline bool MapsEqual(mgp_map *map1, mgp_map *map2) {
  if (map1 == map2) {
    return true;
  }
  if (mgp::map_size(map1) != mgp::map_size(map2)) {
    return false;
  }
  auto *items_it = mgp::MemHandlerCallback(map_iter_items, map1);
  for (auto *item = mgp::map_items_iterator_get(items_it); item; item = mgp::map_items_iterator_next(items_it)) {
    if (mgp::map_item_key(item) == mgp::map_item_key(item)) {
      return false;
    }
    if (!util::ValuesEqual(mgp::map_item_value(item), mgp::map_item_value(item))) {
      return false;
    }
  }
  mgp::map_items_iterator_destroy(items_it);
  return true;
}

/// @brief Returns whether two MGP API nodes are equal.
inline bool NodesEqual(mgp_vertex *node1, mgp_vertex *node2) {
  // With query modules, two nodes are identical if their IDs are equal
  if (node1 == node2) {
    return true;
  }
  if (mgp::vertex_get_id(node1).as_int != mgp::vertex_get_id(node2).as_int) {
    return false;
  }
  return true;
}

/// @brief Returns whether two MGP API relationships are equal.
inline bool RelationshipsEqual(mgp_edge *relationship1, mgp_edge *relationship2) {
  // With query modules, two relationships are identical if their IDs are equal
  if (relationship1 == relationship2) {
    return true;
  }
  if (mgp::edge_get_id(relationship1).as_int != mgp::edge_get_id(relationship2).as_int) {
    return false;
  }
  return true;
}

/// @brief Returns whether two MGP API paths are equal.
inline bool PathsEqual(mgp_path *path1, mgp_path *path2) {
  // With query modules, two paths are identical if their elements are pairwise identical
  if (path1 == path2) {
    return true;
  }
  if (mgp::path_size(path1) != mgp::path_size(path2)) {
    return false;
  }
  const auto path_size = mgp::path_size(path1);
  for (size_t i = 0; i < path_size; ++i) {
    if (!util::NodesEqual(mgp::path_vertex_at(path1, i), mgp::path_vertex_at(path2, i))) {
      return false;
    }
    if (!util::RelationshipsEqual(mgp::path_edge_at(path1, i), mgp::path_edge_at(path2, i))) {
      return false;
    }
  }
  return util::NodesEqual(mgp::path_vertex_at(path1, path_size), mgp::path_vertex_at(path2, path_size));
}

/// @brief Returns whether two MGP API date objects are equal.
inline bool DatesEqual(mgp_date *date1, mgp_date *date2) { return mgp::date_equal(date1, date2); }

/// @brief Returns whether two MGP API local time objects are equal.
inline bool LocalTimesEqual(mgp_local_time *local_time1, mgp_local_time *local_time2) {
  return mgp::local_time_equal(local_time1, local_time2);
}

/// @brief Returns whether two MGP API local datetime objects are equal.
inline bool LocalDateTimesEqual(mgp_local_date_time *local_date_time1, mgp_local_date_time *local_date_time2) {
  return mgp::local_date_time_equal(local_date_time1, local_date_time2);
}

/// @brief Returns whether two MGP API duration objects are equal.
inline bool DurationsEqual(mgp_duration *duration1, mgp_duration *duration2) {
  return mgp::duration_equal(duration1, duration2);
}

/// @brief Returns whether two MGP API values are equal.
inline bool ValuesEqual(mgp_value *value1, mgp_value *value2) {
  if (value1 == value2) {
    return true;
  }
  // Make int and double comparable, (ex. this is true -> 1.0 == 1)
  if (mgp::value_is_numeric(value1) && mgp::value_is_numeric(value2)) {
    return mgp::value_get_numeric(value1) == mgp::value_get_numeric(value2);
  }
  if (mgp::value_get_type(value1) != mgp::value_get_type(value2)) {
    return false;
  }
  switch (mgp::value_get_type(value1)) {
    case MGP_VALUE_TYPE_NULL:
      return true;
    case MGP_VALUE_TYPE_BOOL:
      return mgp::value_get_bool(value1) == mgp::value_get_bool(value2);
    case MGP_VALUE_TYPE_INT:
      return mgp::value_get_int(value1) == mgp::value_get_int(value2);
    case MGP_VALUE_TYPE_DOUBLE:
      return mgp::value_get_double(value1) == mgp::value_get_double(value2);
    case MGP_VALUE_TYPE_STRING:
      return std::string_view(mgp::value_get_string(value1)) == std::string_view(mgp::value_get_string(value2));
    case MGP_VALUE_TYPE_LIST:
      return util::ListsEqual(mgp::value_get_list(value1), mgp::value_get_list(value2));
    case MGP_VALUE_TYPE_MAP:
      return util::MapsEqual(mgp::value_get_map(value1), mgp::value_get_map(value2));
    case MGP_VALUE_TYPE_VERTEX:
      return util::NodesEqual(mgp::value_get_vertex(value1), mgp::value_get_vertex(value2));
    case MGP_VALUE_TYPE_EDGE:
      return util::RelationshipsEqual(mgp::value_get_edge(value1), mgp::value_get_edge(value2));
    case MGP_VALUE_TYPE_PATH:
      return util::PathsEqual(mgp::value_get_path(value1), mgp::value_get_path(value2));
    case MGP_VALUE_TYPE_DATE:
      return util::DatesEqual(mgp::value_get_date(value1), mgp::value_get_date(value2));
    case MGP_VALUE_TYPE_LOCAL_TIME:
      return util::LocalTimesEqual(mgp::value_get_local_time(value1), mgp::value_get_local_time(value2));
    case MGP_VALUE_TYPE_LOCAL_DATE_TIME:
      return util::LocalDateTimesEqual(mgp::value_get_local_date_time(value1), mgp::value_get_local_date_time(value2));
    case MGP_VALUE_TYPE_DURATION:
      return util::DurationsEqual(mgp::value_get_duration(value1), mgp::value_get_duration(value2));
  }
  throw ValueException("Invalid value; does not match any Memgraph type.");
}

/// @brief Converts C++ API types to their MGP API equivalents.
inline mgp_type *ToMGPType(Type type) {
  switch (type) {
    case Type::Any:
      return mgp::type_any();
    case Type::Bool:
      return mgp::type_bool();
    case Type::Int:
      return mgp::type_int();
    case Type::Double:
      return mgp::type_float();
    case Type::String:
      return mgp::type_string();
    case Type::List:
      return mgp::type_list(mgp::type_any());
    case Type::Map:
      return mgp::type_map();
    case Type::Node:
      return mgp::type_node();
    case Type::Relationship:
      return mgp::type_relationship();
    case Type::Path:
      return mgp::type_path();
    case Type::Date:
      return mgp::type_date();
    case Type::LocalTime:
      return mgp::type_local_time();
    case Type::LocalDateTime:
      return mgp::type_local_date_time();
    case Type::Duration:
      return mgp::type_duration();
    default:
      break;
  }
  throw ValueException("Unknown type error!");
}

/// @brief Converts MGP API types to their C++ API equivalents.
inline Type ToAPIType(mgp_value_type type) {
  switch (type) {
    case MGP_VALUE_TYPE_NULL:
      return Type::Null;
    case MGP_VALUE_TYPE_BOOL:
      return Type::Bool;
    case MGP_VALUE_TYPE_INT:
      return Type::Int;
    case MGP_VALUE_TYPE_DOUBLE:
      return Type::Double;
    case MGP_VALUE_TYPE_STRING:
      return Type::String;
    case MGP_VALUE_TYPE_LIST:
      return Type::List;
    case MGP_VALUE_TYPE_MAP:
      return Type::Map;
    case MGP_VALUE_TYPE_VERTEX:
      return Type::Node;
    case MGP_VALUE_TYPE_EDGE:
      return Type::Relationship;
    case MGP_VALUE_TYPE_PATH:
      return Type::Path;
    case MGP_VALUE_TYPE_DATE:
      return Type::Date;
    case MGP_VALUE_TYPE_LOCAL_TIME:
      return Type::LocalTime;
    case MGP_VALUE_TYPE_LOCAL_DATE_TIME:
      return Type::LocalDateTime;
    case MGP_VALUE_TYPE_DURATION:
      return Type::Duration;
    default:
      break;
  }
  throw ValueException("Unknown type error!");
}
}  // namespace util

/* #region Graph (Id, Graph, Nodes, GraphRelationships, Relationships, Properties & Labels) */

// Id:

inline Id Id::FromUint(uint64_t id) { return Id(util::MemcpyCast<int64_t>(id)); }

inline Id Id::FromInt(int64_t id) { return Id(id); }

inline int64_t Id::AsInt() const { return id_; }

inline uint64_t Id::AsUint() const { return util::MemcpyCast<uint64_t>(id_); }

inline bool Id::operator==(const Id &other) const { return id_ == other.id_; }

inline bool Id::operator!=(const Id &other) const { return !(*this == other); }

inline bool Id::operator<(const Id &other) const { return id_ < other.id_; }

inline Id::Id(int64_t id) : id_(id) {}

// Graph:

inline Graph::Graph(mgp_graph *graph) : graph_(graph) {}

inline AbortReason Graph::MustAbort() const {
  const auto reason = must_abort(graph_);
  switch (reason) {
    case 1:
      return AbortReason::TERMINATED;
    case 2:
      return AbortReason::SHUTDOWN;
    case 3:
      return AbortReason::TIMEOUT;
    default:
      break;
  }
  return AbortReason::NO_ABORT;
}

inline void Graph::CheckMustAbort() const {
  switch (MustAbort()) {
    case AbortReason::TERMINATED:
      throw TerminatedMustAbortException();
    case AbortReason::SHUTDOWN:
      throw ShutdownMustAbortException();
    case AbortReason::TIMEOUT:
      throw TimeoutMustAbortException();
    case AbortReason::NO_ABORT:
      break;
  }
}

inline int64_t Graph::Order() const {
  int64_t i = 0;
  for (const auto _ : Nodes()) {
    i++;
  }
  return i;
}

inline int64_t Graph::Size() const {
  int64_t i = 0;
  for (const auto _ : Relationships()) {
    i++;
  }
  return i;
}

inline GraphNodes Graph::Nodes() const {
  auto *nodes_it = mgp::MemHandlerCallback(graph_iter_vertices, graph_);
  if (nodes_it == nullptr) {
    throw mg_exception::NotEnoughMemoryException();
  }
  return GraphNodes(nodes_it);
}

inline GraphRelationships Graph::Relationships() const { return GraphRelationships(graph_); }

inline Node Graph::GetNodeById(const Id node_id) const {
  auto *mgp_node = mgp::MemHandlerCallback(graph_get_vertex_by_id, graph_, mgp_vertex_id{.as_int = node_id.AsInt()});
  if (mgp_node == nullptr) {
    mgp::vertex_destroy(mgp_node);
    throw NotFoundException("Node with ID " + std::to_string(node_id.AsUint()) + " not found!");
  }
  auto node = Node(mgp_node);
  mgp::vertex_destroy(mgp_node);
  return node;
}

inline bool Graph::ContainsNode(const Id node_id) const {
  auto *mgp_node = mgp::MemHandlerCallback(graph_get_vertex_by_id, graph_, mgp_vertex_id{.as_int = node_id.AsInt()});
  if (mgp_node == nullptr) {
    return false;
  }

  mgp::vertex_destroy(mgp_node);
  return true;
}

inline bool Graph::ContainsNode(const Node &node) const { return ContainsNode(node.Id()); }

inline bool Graph::ContainsRelationship(const Id relationship_id) const {
  for (const auto &graph_relationship : Relationships()) {
    if (graph_relationship.Id() == relationship_id) {
      return true;
    }
  }
  return false;
}

inline bool Graph::ContainsRelationship(const Relationship &relationship) const {
  for (const auto &graph_relationship : Relationships()) {
    if (relationship == graph_relationship) {
      return true;
    }
  }
  return false;
}

inline bool Graph::IsMutable() const { return mgp::graph_is_mutable(graph_); }

inline bool Graph::IsTransactional() const { return mgp::graph_is_transactional(graph_); }

inline Node Graph::CreateNode() {
  auto *vertex = mgp::MemHandlerCallback(graph_create_vertex, graph_);
  auto node = Node(vertex);

  mgp::vertex_destroy(vertex);

  return node;
}

inline void Graph::DeleteNode(const Node &node) { mgp::graph_delete_vertex(graph_, node.ptr_); }

inline void Graph::DetachDeleteNode(const Node &node) { mgp::graph_detach_delete_vertex(graph_, node.ptr_); };

inline Relationship Graph::CreateRelationship(const Node &from, const Node &to, const std::string_view type) {
  auto *edge =
      mgp::MemHandlerCallback(graph_create_edge, graph_, from.ptr_, to.ptr_, mgp_edge_type{.name = type.data()});
  auto relationship = Relationship(edge);

  mgp::edge_destroy(edge);

  return relationship;
}

inline void Graph::SetFrom(Relationship &relationship, const Node &new_from) {
  mgp_edge *edge = mgp::MemHandlerCallback(mgp::graph_edge_set_from, graph_, relationship.ptr_, new_from.ptr_);
  relationship = Relationship(edge);
  mgp::edge_destroy(edge);
}

inline void Graph::SetTo(Relationship &relationship, const Node &new_to) {
  mgp_edge *edge = mgp::MemHandlerCallback(mgp::graph_edge_set_to, graph_, relationship.ptr_, new_to.ptr_);
  relationship = Relationship(edge);
  mgp::edge_destroy(edge);
}

inline void Graph::ChangeType(Relationship &relationship, std::string_view new_type) {
  mgp_edge *edge = mgp::MemHandlerCallback(mgp::graph_edge_change_type, graph_, relationship.ptr_,
                                           mgp_edge_type{.name = new_type.data()});
  relationship = Relationship(edge);
  mgp::edge_destroy(edge);
}

inline void Graph::DeleteRelationship(const Relationship &relationship) {
  mgp::graph_delete_edge(graph_, relationship.ptr_);
}

// Nodes:

inline Nodes::Nodes(mgp_vertices_iterator *nodes_iterator) : nodes_iterator_(nodes_iterator) {}

inline Nodes::Iterator::Iterator(mgp_vertices_iterator *nodes_iterator) : nodes_iterator_(nodes_iterator) {
  if (nodes_iterator_ == nullptr) {
    return;
  }

  if (mgp::vertices_iterator_get(nodes_iterator_) == nullptr) {
    mgp::vertices_iterator_destroy(nodes_iterator_);
    nodes_iterator_ = nullptr;
  }
}

inline Nodes::Iterator::Iterator(const Iterator &other) noexcept : Iterator(other.nodes_iterator_) {}

inline Nodes::Iterator::~Iterator() {
  if (nodes_iterator_ != nullptr) {
    mgp::vertices_iterator_destroy(nodes_iterator_);
  }
}

inline Nodes::Iterator &Nodes::Iterator::operator++() {
  if (nodes_iterator_ != nullptr) {
    auto *next = mgp::vertices_iterator_next(nodes_iterator_);

    if (next == nullptr) {
      mgp::vertices_iterator_destroy(nodes_iterator_);
      nodes_iterator_ = nullptr;
      return *this;
    }
    index_++;
  }
  return *this;
}

inline Nodes::Iterator Nodes::Iterator::operator++(int) {
  auto retval = *this;
  ++*this;
  return retval;
}

inline bool Nodes::Iterator::operator==(Iterator other) const {
  if (nodes_iterator_ == nullptr && other.nodes_iterator_ == nullptr) {
    return true;
  }
  if (nodes_iterator_ == nullptr || other.nodes_iterator_ == nullptr) {
    return false;
  }
  return mgp::vertex_equal(mgp::vertices_iterator_get(nodes_iterator_),
                           mgp::vertices_iterator_get(other.nodes_iterator_)) &&
         index_ == other.index_;
}

inline bool Nodes::Iterator::operator!=(Iterator other) const { return !(*this == other); }

inline Node Nodes::Iterator::operator*() const {
  if (nodes_iterator_ == nullptr) {
    return Node((const mgp_vertex *)nullptr);
  }

  return Node(mgp::vertices_iterator_get(nodes_iterator_));
}

inline Nodes::Iterator Nodes::begin() const { return Iterator(nodes_iterator_); }

inline Nodes::Iterator Nodes::end() const { return Iterator(nullptr); }

inline Nodes::Iterator Nodes::cbegin() const { return Iterator(nodes_iterator_); }

inline Nodes::Iterator Nodes::cend() const { return Iterator(nullptr); }

// GraphRelationships:

inline GraphRelationships::GraphRelationships(mgp_graph *graph) : graph_(graph) {}

inline GraphRelationships::Iterator::Iterator(mgp_vertices_iterator *nodes_iterator) : nodes_iterator_(nodes_iterator) {
  // Positions the iterator over the first existing relationship

  if (nodes_iterator_ == nullptr) {
    return;
  }

  // Go through each graph node’s adjacent nodes
  for (auto *node = mgp::vertices_iterator_get(nodes_iterator_); node;
       node = mgp::vertices_iterator_next(nodes_iterator_)) {
    // Check if node exists
    if (node == nullptr) {
      mgp::vertices_iterator_destroy(nodes_iterator_);
      nodes_iterator_ = nullptr;
      return;
    }

    // Check if node has out-relationships
    out_relationships_iterator_ = mgp::MemHandlerCallback(vertex_iter_out_edges, node);
    auto *relationship = mgp::edges_iterator_get(out_relationships_iterator_);
    if (relationship != nullptr) {
      return;
    }

    mgp::edges_iterator_destroy(out_relationships_iterator_);
    out_relationships_iterator_ = nullptr;
  }
}

inline GraphRelationships::Iterator::Iterator(const Iterator &other) noexcept : Iterator(other.nodes_iterator_) {}

inline GraphRelationships::Iterator::~Iterator() {
  if (nodes_iterator_ != nullptr) {
    mgp::vertices_iterator_destroy(nodes_iterator_);
  }
  if (out_relationships_iterator_ != nullptr) {
    mgp::edges_iterator_destroy(out_relationships_iterator_);
  }
}

inline GraphRelationships::Iterator &GraphRelationships::Iterator::operator++() {
  // Moves the iterator onto the next existing relationship

  // 1. Check if the current node has remaining relationships to iterate over

  if (out_relationships_iterator_ != nullptr) {
    auto *next = mgp::edges_iterator_next(out_relationships_iterator_);

    if (next != nullptr) {
      return *this;
    }

    mgp::edges_iterator_destroy(out_relationships_iterator_);
    out_relationships_iterator_ = nullptr;
  }

  // 2. Move onto the next nodes

  if (nodes_iterator_ != nullptr) {
    for (auto *node = mgp::vertices_iterator_next(nodes_iterator_); node;
         node = mgp::vertices_iterator_next(nodes_iterator_)) {
      // Check if node exists - if it doesn’t, we’ve reached the end of the iterator
      if (node == nullptr) {
        mgp::vertices_iterator_destroy(nodes_iterator_);
        nodes_iterator_ = nullptr;
        return *this;
      }

      // Check if node has out-relationships
      out_relationships_iterator_ = mgp::MemHandlerCallback(vertex_iter_out_edges, node);
      auto *relationship = mgp::edges_iterator_get(out_relationships_iterator_);
      if (relationship != nullptr) {
        return *this;
      }

      mgp::edges_iterator_destroy(out_relationships_iterator_);
      out_relationships_iterator_ = nullptr;
    }
  }
  mgp::vertices_iterator_destroy(nodes_iterator_);
  nodes_iterator_ = nullptr;
  return *this;
}

inline GraphRelationships::Iterator GraphRelationships::Iterator::operator++(int) {
  auto retval = *this;
  ++*this;
  return retval;
}

inline bool GraphRelationships::Iterator::operator==(Iterator other) const {
  if (out_relationships_iterator_ == nullptr && other.out_relationships_iterator_ == nullptr) {
    return true;
  }
  if (out_relationships_iterator_ == nullptr || other.out_relationships_iterator_ == nullptr) {
    return false;
  }
  return mgp::edge_equal(mgp::edges_iterator_get(out_relationships_iterator_),
                         mgp::edges_iterator_get(other.out_relationships_iterator_)) &&
         index_ == other.index_;
}

inline bool GraphRelationships::Iterator::operator!=(Iterator other) const { return !(*this == other); }

inline Relationship GraphRelationships::Iterator::operator*() const {
  if (out_relationships_iterator_ != nullptr) {
    return Relationship(mgp::edges_iterator_get(out_relationships_iterator_));
  }

  return Relationship((mgp_edge *)nullptr);
}

inline GraphRelationships::Iterator GraphRelationships::begin() const {
  return Iterator(mgp::MemHandlerCallback(graph_iter_vertices, graph_));
}

inline GraphRelationships::Iterator GraphRelationships::end() const { return Iterator(nullptr); }

inline GraphRelationships::Iterator GraphRelationships::cbegin() const {
  return Iterator(mgp::MemHandlerCallback(graph_iter_vertices, graph_));
}

inline GraphRelationships::Iterator GraphRelationships::cend() const { return Iterator(nullptr); }

// Relationships:

inline Relationships::Relationships(mgp_edges_iterator *relationships_iterator)
    : relationships_iterator_(relationships_iterator) {}

inline Relationships::Iterator::Iterator(mgp_edges_iterator *relationships_iterator)
    : relationships_iterator_(relationships_iterator) {
  if (relationships_iterator_ == nullptr) {
    return;
  }
  if (mgp::edges_iterator_get(relationships_iterator_) == nullptr) {
    mgp::edges_iterator_destroy(relationships_iterator_);
    relationships_iterator_ = nullptr;
  }
}

inline Relationships::Iterator::Iterator(const Iterator &other) noexcept : Iterator(other.relationships_iterator_) {}

inline Relationships::Iterator::~Iterator() {
  if (relationships_iterator_ != nullptr) {
    mgp::edges_iterator_destroy(relationships_iterator_);
  }
}

inline Relationships::Iterator &Relationships::Iterator::operator++() {
  if (relationships_iterator_ != nullptr) {
    auto *next = mgp::edges_iterator_next(relationships_iterator_);

    if (next == nullptr) {
      mgp::edges_iterator_destroy(relationships_iterator_);
      relationships_iterator_ = nullptr;
      return *this;
    }
    index_++;
  }
  return *this;
}

inline Relationships::Iterator Relationships::Iterator::operator++(int) {
  auto retval = *this;
  ++*this;
  return retval;
}

inline bool Relationships::Iterator::operator==(Iterator other) const {
  if (relationships_iterator_ == nullptr && other.relationships_iterator_ == nullptr) {
    return true;
  }
  if (relationships_iterator_ == nullptr || other.relationships_iterator_ == nullptr) {
    return false;
  }
  return mgp::edge_equal(mgp::edges_iterator_get(relationships_iterator_),
                         mgp::edges_iterator_get(other.relationships_iterator_)) &&
         index_ == other.index_;
}

inline bool Relationships::Iterator::operator!=(Iterator other) const { return !(*this == other); }

inline Relationship Relationships::Iterator::operator*() const {
  if (relationships_iterator_ == nullptr) {
    return Relationship((mgp_edge *)nullptr);
  }

  auto relationship = Relationship(mgp::edges_iterator_get(relationships_iterator_));
  return relationship;
}

inline Relationships::Iterator Relationships::begin() const { return Iterator(relationships_iterator_); }

inline Relationships::Iterator Relationships::end() const { return Iterator(nullptr); }

inline Relationships::Iterator Relationships::cbegin() const { return Iterator(relationships_iterator_); }

inline Relationships::Iterator Relationships::cend() const { return Iterator(nullptr); }

// Labels:

inline Labels::Labels(mgp_vertex *node_ptr) : node_ptr_(mgp::MemHandlerCallback(vertex_copy, node_ptr)) {}

inline Labels::Labels(const Labels &other) noexcept : Labels(other.node_ptr_) {}

inline Labels::Labels(Labels &&other) noexcept : node_ptr_(other.node_ptr_) { other.node_ptr_ = nullptr; }

inline Labels &Labels::operator=(const Labels &other) noexcept {
  if (this != &other) {
    mgp::vertex_destroy(node_ptr_);

    node_ptr_ = mgp::MemHandlerCallback(vertex_copy, other.node_ptr_);
  }
  return *this;
}

inline Labels &Labels::operator=(Labels &&other) noexcept {
  if (this != &other) {
    mgp::vertex_destroy(node_ptr_);

    node_ptr_ = other.node_ptr_;
    other.node_ptr_ = nullptr;
  }
  return *this;
}

inline Labels::~Labels() {
  if (node_ptr_ != nullptr) {
    mgp::vertex_destroy(node_ptr_);
  }
}

inline bool Labels::Iterator::operator==(const Iterator &other) const {
  return iterable_ == other.iterable_ && index_ == other.index_;
}

inline bool Labels::Iterator::operator!=(const Iterator &other) const { return !(*this == other); }

inline Labels::Iterator &Labels::Iterator::operator++() {
  index_++;
  return *this;
}

inline std::string_view Labels::Iterator::operator*() const { return (*iterable_)[index_]; }

inline Labels::Iterator::Iterator(const Labels *iterable, size_t index) : iterable_(iterable), index_(index) {}

inline size_t Labels::Size() const { return mgp::vertex_labels_count(node_ptr_); }

inline std::string_view Labels::operator[](size_t index) const { return mgp::vertex_label_at(node_ptr_, index).name; }

inline Labels::Iterator Labels::begin() { return Iterator(this, 0); }

inline Labels::Iterator Labels::end() { return Iterator(this, Size()); }

inline Labels::Iterator Labels::cbegin() { return Iterator(this, 0); }

inline Labels::Iterator Labels::cend() { return Iterator(this, Size()); }

/* #endregion */

/* #region Types */

/* #region Containers (List, Map) */

// List:

inline List::List(mgp_list *ptr) : ptr_(mgp::MemHandlerCallback(list_copy, ptr)) {}

inline List::List(const mgp_list *const_ptr)
    : ptr_(mgp::MemHandlerCallback(list_copy, const_cast<mgp_list *>(const_ptr))) {}

inline List::List() : ptr_(mgp::MemHandlerCallback(list_make_empty, 0)) {}

inline List::List(size_t capacity) : ptr_(mgp::MemHandlerCallback(list_make_empty, capacity)) {}

inline List::List(const std::vector<Value> &values) : ptr_(mgp::MemHandlerCallback(list_make_empty, values.size())) {
  for (const auto &value : values) {
    AppendExtend(value);
  }
}

inline List::List(std::vector<Value> &&values) : ptr_(mgp::MemHandlerCallback(list_make_empty, values.size())) {
  for (auto &value : values) {
    Append(std::move(value));
  }
}

inline List::List(const std::initializer_list<Value> values)
    : ptr_(mgp::MemHandlerCallback(list_make_empty, values.size())) {
  for (const auto &value : values) {
    AppendExtend(value);
  }
}

inline List::List(const List &other) noexcept : List(other.ptr_) {}

inline List::List(List &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }

inline List &List::operator=(const List &other) noexcept {
  if (this != &other) {
    mgp::list_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(list_copy, other.ptr_);
  }
  return *this;
}

inline List &List::operator=(List &&other) noexcept {
  if (this != &other) {
    mgp::list_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline List::~List() {
  if (ptr_ != nullptr) {
    mgp::list_destroy(ptr_);
  }
}

inline bool List::ContainsDeleted() const { return mgp::list_contains_deleted(ptr_); }

inline size_t List::Size() const { return mgp::list_size(ptr_); }

inline bool List::Empty() const { return Size() == 0; }

inline Value List::operator[](size_t index) const { return Value(mgp::list_at(ptr_, index)); }

inline Value List::operator[](size_t index) { return Value(mgp::list_at(ptr_, index)); }

inline bool List::Iterator::operator==(const Iterator &other) const {
  return iterable_ == other.iterable_ && index_ == other.index_;
}

inline bool List::Iterator::operator!=(const Iterator &other) const { return !(*this == other); }

inline List::Iterator &List::Iterator::operator++() {
  index_++;
  return *this;
}

inline Value List::Iterator::operator*() const { return (*iterable_)[index_]; }

inline List::Iterator::Iterator(const List *iterable, size_t index) : iterable_(iterable), index_(index) {}

inline List::Iterator List::begin() const { return Iterator(this, 0); }

inline List::Iterator List::end() const { return Iterator(this, Size()); }

inline List::Iterator List::cbegin() const { return Iterator(this, 0); }

inline List::Iterator List::cend() const { return Iterator(this, Size()); }

inline void List::Append(const Value &value) { mgp::list_append(ptr_, value.ptr_); }

inline void List::Append(Value &&value) {
  mgp::list_append(ptr_, value.ptr_);
  value.ptr_ = nullptr;
}

inline void List::AppendExtend(const Value &value) { mgp::list_append_extend(ptr_, value.ptr_); }

inline void List::AppendExtend(Value &&value) { mgp::list_append_extend(ptr_, value.ptr_); }

inline bool List::operator==(const List &other) const { return util::ListsEqual(ptr_, other.ptr_); }

inline bool List::operator!=(const List &other) const { return !(*this == other); }

inline std::string List::ToString() const {
  const size_t size = Size();
  if (size == 0) {
    return "[]";
  }
  std::string return_str{"["};
  size_t i = 0;
  const mgp::List &list = (*this);
  while (i < size - 1) {
    return_str.append(list[i].ToString() + ", ");
    i++;
  }
  return_str.append(list[i].ToString() + "]");
  return return_str;
}

// MapItem:

inline bool MapItem::operator==(MapItem &other) const { return key == other.key && value == other.value; }

inline bool MapItem::operator!=(MapItem &other) const { return !(*this == other); }

inline bool MapItem::operator<(const MapItem &other) const { return key < other.key; }

// Map:

inline Map::Map(mgp_map *ptr) : ptr_(mgp::MemHandlerCallback(map_copy, ptr)) {}

inline Map::Map(const mgp_map *const_ptr) : ptr_(mgp::MemHandlerCallback(map_copy, const_cast<mgp_map *>(const_ptr))) {}

inline Map::Map() : ptr_(mgp::MemHandlerCallback(map_make_empty)) {}

inline Map::Map(const std::map<std::string_view, Value> &items) : ptr_(mgp::MemHandlerCallback(map_make_empty)) {
  for (const auto &[key, value] : items) {
    Insert(key, value);
  }
}

inline Map::Map(std::map<std::string_view, Value> &&items) : ptr_(mgp::MemHandlerCallback(map_make_empty)) {
  for (auto &[key, value] : items) {
    Insert(key, value);
  }
}

inline Map::Map(const std::initializer_list<std::pair<std::string_view, Value>> items)
    : ptr_(mgp::MemHandlerCallback(map_make_empty)) {
  for (const auto &[key, value] : items) {
    Insert(key, value);
  }
}

inline Map::Map(const Map &other) noexcept : Map(other.ptr_) {}

inline Map::Map(Map &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }

inline Map &Map::operator=(const Map &other) noexcept {
  if (this != &other) {
    mgp::map_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(map_copy, other.ptr_);
  }
  return *this;
}

inline Map &Map::operator=(Map &&other) noexcept {
  if (this != &other) {
    mgp::map_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline Map::~Map() {
  if (ptr_ != nullptr) {
    mgp::map_destroy(ptr_);
  }
}

inline bool Map::ContainsDeleted() const { return mgp::map_contains_deleted(ptr_); }

inline size_t Map::Size() const { return mgp::map_size(ptr_); }

inline bool Map::Empty() const { return Size() == 0; }

inline Value Map::operator[](std::string_view key) const { return Value(mgp::map_at(ptr_, key.data())); }

inline Value Map::At(std::string_view key) const {
  auto *ptr = mgp::map_at(ptr_, key.data());
  if (ptr) {
    return Value(ptr);
  }

  return Value();
}

inline bool Map::KeyExists(std::string_view key) const { return mgp::key_exists(ptr_, key.data()); }

inline Map::Iterator::Iterator(mgp_map_items_iterator *map_items_iterator) : map_items_iterator_(map_items_iterator) {
  if (map_items_iterator_ == nullptr) return;
  if (mgp::map_items_iterator_get(map_items_iterator_) == nullptr) {
    mgp::map_items_iterator_destroy(map_items_iterator_);
    map_items_iterator_ = nullptr;
  }
}

inline Map::Iterator::Iterator(const Iterator &other) noexcept : Iterator(other.map_items_iterator_) {}

inline Map::Iterator::~Iterator() {
  if (map_items_iterator_ != nullptr) {
    mgp::map_items_iterator_destroy(map_items_iterator_);
  }
}

inline Map::Iterator &Map::Iterator::operator++() {
  if (map_items_iterator_ != nullptr) {
    auto *next = mgp::map_items_iterator_next(map_items_iterator_);

    if (next == nullptr) {
      mgp::map_items_iterator_destroy(map_items_iterator_);
      map_items_iterator_ = nullptr;
      return *this;
    }
  }
  return *this;
}

inline Map::Iterator Map::Iterator::operator++(int) {
  auto retval = *this;
  ++*this;
  return retval;
}

inline bool Map::Iterator::operator==(Iterator other) const {
  if (map_items_iterator_ == nullptr && other.map_items_iterator_ == nullptr) {
    return true;
  }
  if (map_items_iterator_ == nullptr || other.map_items_iterator_ == nullptr) {
    return false;
  }
  return mgp::map_items_iterator_get(map_items_iterator_) == mgp::map_items_iterator_get(other.map_items_iterator_);
}

inline bool Map::Iterator::operator!=(Iterator other) const { return !(*this == other); }

inline MapItem Map::Iterator::operator*() const {
  if (map_items_iterator_ == nullptr) {
    throw ValueException("Empty map item!");
  }

  auto *raw_map_item = mgp::map_items_iterator_get(map_items_iterator_);

  const auto *map_key = mgp::map_item_key(raw_map_item);
  auto map_value = Value(mgp::map_item_value(raw_map_item));

  return MapItem{.key = map_key, .value = map_value};
}

inline Map::Iterator Map::begin() const { return Iterator(mgp::MemHandlerCallback(map_iter_items, ptr_)); }

inline Map::Iterator Map::end() const { return Iterator(nullptr); }

inline Map::Iterator Map::cbegin() const { return Iterator(mgp::MemHandlerCallback(map_iter_items, ptr_)); }

inline Map::Iterator Map::cend() const { return Iterator(nullptr); }

inline void Map::Insert(std::string_view key, const Value &value) { mgp::map_insert(ptr_, key.data(), value.ptr_); }

inline void Map::Insert(std::string_view key, Value &&value) {
  mgp::map_insert(ptr_, key.data(), value.ptr_);
  value.~Value();
  value.ptr_ = nullptr;
}

inline void Map::Update(std::string_view key, const Value &value) { mgp::map_update(ptr_, key.data(), value.ptr_); }

inline void Map::Update(std::string_view key, Value &&value) {
  mgp::map_update(ptr_, key.data(), value.ptr_);
  value.~Value();
  value.ptr_ = nullptr;
}

inline void Map::Erase(std::string_view key) { mgp::map_erase(ptr_, key.data()); }

inline bool Map::operator==(const Map &other) const { return util::MapsEqual(ptr_, other.ptr_); }

inline bool Map::operator!=(const Map &other) const { return !(*this == other); }

inline std::string Map::ToString() const {
  const size_t map_size = Size();
  if (map_size == 0) {
    return "{}";
  }
  std::string return_string{"{"};
  size_t i = 0;
  for (const auto &[key, value] : *this) {
    if (i == map_size - 1) {
      return_string.append(std::string(key) + ": " + value.ToString() + "}");
      break;
    }
    return_string.append(std::string(key) + ": " + value.ToString() + ", ");
    ++i;
  }
  return return_string;
}

/* #endregion */

/* #region Graph elements (Node, Relationship & Path) */

// Node:

inline Node::Node(mgp_vertex *ptr) : ptr_(MemHandlerCallback(vertex_copy, ptr)) {}

inline Node::Node(const mgp_vertex *const_ptr)
    : ptr_(mgp::MemHandlerCallback(vertex_copy, const_cast<mgp_vertex *>(const_ptr))) {}

inline Node::Node(const Node &other) noexcept : Node(other.ptr_) {}

inline Node::Node(Node &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }

inline Node &Node::operator=(const Node &other) noexcept {
  if (this != &other) {
    mgp::vertex_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(vertex_copy, other.ptr_);
  }
  return *this;
}

inline Node &Node::operator=(Node &&other) noexcept {
  if (this != &other) {
    mgp::vertex_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline Node::~Node() {
  if (ptr_ != nullptr) {
    mgp::vertex_destroy(ptr_);
  }
}

inline bool Node::IsDeleted() const { return mgp::vertex_is_deleted(ptr_); }

inline mgp::Id Node::Id() const { return Id::FromInt(mgp::vertex_get_id(ptr_).as_int); }

inline mgp::Labels Node::Labels() const { return mgp::Labels(ptr_); }

inline bool Node::HasLabel(std::string_view label) const {
  for (const auto node_label : Labels()) {
    if (label == node_label) {
      return true;
    }
  }
  return false;
}

inline Relationships Node::InRelationships() const {
  auto *relationship_iterator = mgp::MemHandlerCallback(vertex_iter_in_edges, ptr_);
  if (relationship_iterator == nullptr) {
    throw mg_exception::NotEnoughMemoryException();
  }
  return Relationships(relationship_iterator);
}

inline Relationships Node::OutRelationships() const {
  auto *relationship_iterator = mgp::MemHandlerCallback(vertex_iter_out_edges, ptr_);
  if (relationship_iterator == nullptr) {
    throw mg_exception::NotEnoughMemoryException();
  }
  return Relationships(relationship_iterator);
}

inline void Node::AddLabel(const std::string_view label) {
  mgp::vertex_add_label(this->ptr_, mgp_label{.name = label.data()});
}

inline void Node::RemoveLabel(const std::string_view label) {
  mgp::vertex_remove_label(this->ptr_, mgp_label{.name = label.data()});
}

inline std::unordered_map<std::string, Value> Node::Properties() const {
  mgp_properties_iterator *properties_iterator = mgp::MemHandlerCallback(vertex_iter_properties, ptr_);
  std::unordered_map<std::string, Value> property_map;
  for (auto *property = mgp::properties_iterator_get(properties_iterator); property;
       property = mgp::properties_iterator_next(properties_iterator)) {
    property_map.emplace(std::string(property->name), Value(property->value));
  }
  mgp::properties_iterator_destroy(properties_iterator);
  return property_map;
}

inline void Node::SetProperty(std::string property, Value value) {
  mgp::vertex_set_property(ptr_, property.data(), value.ptr());
}

inline void Node::SetProperties(std::unordered_map<std::string_view, Value> properties) {
  mgp_map *map = mgp::MemHandlerCallback(map_make_empty);

  for (auto const &[k, v] : properties) {
    mgp::map_insert(map, k.data(), v.ptr());
  }

  mgp::vertex_set_properties(ptr_, map);
  mgp::map_destroy(map);
}

inline void Node::RemoveProperty(std::string property) { SetProperty(property, Value()); }

inline Value Node::GetProperty(const std::string &property) const {
  mgp_value *vertex_prop = mgp::MemHandlerCallback(vertex_get_property, ptr_, property.data());
  return Value(steal, vertex_prop);
}

inline bool Node::operator<(const Node &other) const { return Id() < other.Id(); }

inline bool Node::operator==(const Node &other) const { return util::NodesEqual(ptr_, other.ptr_); }

inline bool Node::operator!=(const Node &other) const { return !(*this == other); }

// this functions is used both in relationship and node ToString
inline std::string PropertiesToString(const std::map<std::string, Value> &property_map) {
  std::string properties;
  const auto map_size = property_map.size();
  size_t i = 0;
  for (const auto &[key, value] : property_map) {
    if (i == map_size - 1) {
      properties.append(std::string(key) + ": " + value.ToString());
      break;
    }
    properties.append(std::string(key) + ": " + value.ToString() + ", ");
    ++i;
  }
  return properties;
}

inline std::string Node::ToString() const {
  std::string labels{", "};
  for (auto label : Labels()) {
    labels.append(":" + std::string(label));
  }
  if (labels == ", ") {
    labels = "";  // dont use labels if they dont exist
  }
  std::unordered_map<std::string, Value> properties_map{Properties()};
  std::map<std::string, Value> properties_map_sorted{};

  for (const auto &[k, v] : properties_map) {
    properties_map_sorted.emplace(k, v);
  }
  std::string properties{PropertiesToString(properties_map_sorted)};

  return "(id: " + std::to_string(Id().AsInt()) + labels + ", properties: {" + properties + "})";
}

inline size_t Node::InDegree() const { return mgp::vertex_get_in_degree(ptr_); }

inline size_t Node::OutDegree() const { return mgp::vertex_get_out_degree(ptr_); }

// Relationship:

inline Relationship::Relationship(mgp_edge *ptr) : ptr_(mgp::MemHandlerCallback(edge_copy, ptr)) {}

inline Relationship::Relationship(const mgp_edge *const_ptr)
    : ptr_(mgp::MemHandlerCallback(edge_copy, const_cast<mgp_edge *>(const_ptr))) {}

inline Relationship::Relationship(const Relationship &other) noexcept : Relationship(other.ptr_) {}

inline Relationship::Relationship(Relationship &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }

inline Relationship &Relationship::operator=(const Relationship &other) noexcept {
  if (this != &other) {
    mgp::edge_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(edge_copy, other.ptr_);
  }
  return *this;
}

inline Relationship &Relationship::operator=(Relationship &&other) noexcept {
  if (this != &other) {
    mgp::edge_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline Relationship::~Relationship() {
  if (ptr_ != nullptr) {
    mgp::edge_destroy(ptr_);
  }
}

inline bool Relationship::IsDeleted() const { return mgp::edge_is_deleted(ptr_); }

inline mgp::Id Relationship::Id() const { return Id::FromInt(mgp::edge_get_id(ptr_).as_int); }

inline std::string_view Relationship::Type() const { return mgp::edge_get_type(ptr_).name; }

inline std::unordered_map<std::string, Value> Relationship::Properties() const {
  mgp_properties_iterator *properties_iterator = mgp::MemHandlerCallback(edge_iter_properties, ptr_);
  std::unordered_map<std::string, Value> property_map;
  for (mgp_property *property = mgp::properties_iterator_get(properties_iterator); property;
       property = mgp::properties_iterator_next(properties_iterator)) {
    property_map.emplace(property->name, Value(property->value));
  }
  mgp::properties_iterator_destroy(properties_iterator);
  return property_map;
}

inline void Relationship::SetProperty(std::string property, Value value) {
  mgp::edge_set_property(ptr_, property.data(), value.ptr());
}

inline void Relationship::SetProperties(std::unordered_map<std::string_view, Value> properties) {
  mgp_map *map = mgp::MemHandlerCallback(map_make_empty);

  for (auto const &[k, v] : properties) {
    mgp::map_insert(map, k.data(), v.ptr());
  }

  mgp::edge_set_properties(ptr_, map);
  mgp::map_destroy(map);
}

inline void Relationship::RemoveProperty(std::string property) { SetProperty(property, Value()); }

inline Value Relationship::GetProperty(const std::string &property) const {
  mgp_value *edge_prop = mgp::MemHandlerCallback(edge_get_property, ptr_, property.data());
  return Value(steal, edge_prop);
}

inline Node Relationship::From() const { return Node(mgp::edge_get_from(ptr_)); }

inline Node Relationship::To() const { return Node(mgp::edge_get_to(ptr_)); }

inline bool Relationship::operator<(const Relationship &other) const { return Id() < other.Id(); }

inline bool Relationship::operator==(const Relationship &other) const {
  return util::RelationshipsEqual(ptr_, other.ptr_);
}

inline bool Relationship::operator!=(const Relationship &other) const { return !(*this == other); }

inline std::string Relationship::ToString() const {
  const auto from = From();
  const auto to = To();

  const std::string type{Type()};
  std::unordered_map<std::string, Value> properties_map{Properties()};
  std::map<std::string, Value> properties_map_sorted{};

  for (const auto &[k, v] : properties_map) {
    properties_map_sorted.emplace(k, v);
  }
  std::string properties{PropertiesToString(properties_map_sorted)};

  const std::string relationship{"[type: " + type + ", id: " + std::to_string(Id().AsInt()) + ", properties: {" +
                                 properties + "}]"};

  return from.ToString() + "-" + relationship + "->" + to.ToString();
}
// Path:

inline Path::Path(mgp_path *ptr) : ptr_(mgp::MemHandlerCallback(path_copy, ptr)) {}

inline Path::Path(const mgp_path *const_ptr)
    : ptr_(mgp::MemHandlerCallback(path_copy, const_cast<mgp_path *>(const_ptr))) {}

inline Path::Path(const Node &start_node) : ptr_(mgp::MemHandlerCallback(path_make_with_start, start_node.ptr_)) {}

inline Path::Path(const Path &other) noexcept : Path(other.ptr_) {}

inline Path::Path(Path &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }

inline Path &Path::operator=(const Path &other) noexcept {
  if (this != &other) {
    mgp::path_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(path_copy, other.ptr_);
  }
  return *this;
}

inline Path &Path::operator=(Path &&other) noexcept {
  if (this != &other) {
    mgp::path_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline Path::~Path() {
  if (ptr_ != nullptr) {
    mgp::path_destroy(ptr_);
  }
}

inline bool Path::ContainsDeleted() const { return mgp::path_contains_deleted(ptr_); }

inline size_t Path::Length() const { return mgp::path_size(ptr_); }

inline Node Path::GetNodeAt(size_t index) const {
  auto *node_ptr = mgp::path_vertex_at(ptr_, index);
  if (node_ptr == nullptr) {
    throw IndexException("Index value out of bounds.");
  }
  return Node(node_ptr);
}

inline Relationship Path::GetRelationshipAt(size_t index) const {
  auto *relationship_ptr = mgp::path_edge_at(ptr_, index);
  if (relationship_ptr == nullptr) {
    throw IndexException("Index value out of bounds.");
  }
  return Relationship(relationship_ptr);
}

inline void Path::Expand(const Relationship &relationship) { mgp::path_expand(ptr_, relationship.ptr_); }

inline void Path::Pop() { mgp::path_pop(ptr_); }

inline bool Path::operator==(const Path &other) const { return util::PathsEqual(ptr_, other.ptr_); }

inline bool Path::operator!=(const Path &other) const { return !(*this == other); }

inline std::string Path::ToString() const {
  const auto length = Length();
  size_t i = 0;
  std::string return_string;
  for (i = 0; i < length; i++) {
    const auto node = GetNodeAt(i);
    return_string.append(node.ToString() + "-");

    const Relationship rel = GetRelationshipAt(i);
    std::unordered_map<std::string, Value> properties_map{rel.Properties()};
    std::map<std::string, Value> properties_map_sorted{};

    for (const auto &[k, v] : properties_map) {
      properties_map_sorted.emplace(k, v);
    }
    std::string properties{PropertiesToString(properties_map_sorted)};

    return_string.append("[type: " + std::string(rel.Type()) + ", id: " + std::to_string(rel.Id().AsInt()) +
                         ", properties: {" + properties + "}]->");
  }

  const auto node = GetNodeAt(i);
  return_string.append(node.ToString());
  return return_string;
}

/* #endregion */

/* #region Temporal types (Date, LocalTime, LocalDateTime, Duration) */

// Date:

inline Date::Date(mgp_date *ptr) : ptr_(mgp::MemHandlerCallback(date_copy, ptr)) {}

inline Date::Date(const mgp_date *const_ptr)
    : ptr_(mgp::MemHandlerCallback(date_copy, const_cast<mgp_date *>(const_ptr))) {}

inline Date::Date(std::string_view string) : ptr_(mgp::MemHandlerCallback(date_from_string, string.data())) {}

inline Date::Date(int year, int month, int day) {
  mgp_date_parameters params{.year = year, .month = month, .day = day};
  ptr_ = mgp::MemHandlerCallback(date_from_parameters, &params);
}

inline Date::Date(const Date &other) noexcept : Date(other.ptr_) {}

inline Date::Date(Date &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }

inline Date &Date::operator=(const Date &other) noexcept {
  if (this != &other) {
    mgp::date_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(date_copy, other.ptr_);
  }
  return *this;
}

inline Date &Date::operator=(Date &&other) noexcept {
  if (this != &other) {
    mgp::date_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline Date::~Date() {
  if (ptr_ != nullptr) {
    mgp::date_destroy(ptr_);
  }
}

inline Date Date::Now() {
  auto *mgp_date = mgp::MemHandlerCallback(date_now);
  auto date = Date(mgp_date);
  mgp::date_destroy(mgp_date);

  return date;
}

inline int Date::Year() const { return mgp::date_get_year(ptr_); }

inline int Date::Month() const { return mgp::date_get_month(ptr_); }

inline int Date::Day() const { return mgp::date_get_day(ptr_); }

inline int64_t Date::Timestamp() const { return mgp::date_timestamp(ptr_); }

inline bool Date::operator==(const Date &other) const { return util::DatesEqual(ptr_, other.ptr_); }

inline Date Date::operator+(const Duration &dur) const {
  auto *mgp_sum = mgp::MemHandlerCallback(date_add_duration, ptr_, dur.ptr_);
  auto sum = Date(mgp_sum);
  mgp::date_destroy(mgp_sum);

  return sum;
}

inline Date Date::operator-(const Duration &dur) const {
  auto *mgp_difference = mgp::MemHandlerCallback(date_add_duration, ptr_, dur.ptr_);
  auto difference = Date(mgp_difference);
  mgp::date_destroy(mgp_difference);

  return difference;
}

inline Duration Date::operator-(const Date &other) const {
  auto *mgp_difference = mgp::MemHandlerCallback(date_diff, ptr_, other.ptr_);
  auto difference = Duration(mgp_difference);
  mgp::duration_destroy(mgp_difference);

  return difference;
}

inline bool Date::operator<(const Date &other) const {
  auto *difference = mgp::MemHandlerCallback(date_diff, ptr_, other.ptr_);
  auto is_less = (mgp::duration_get_microseconds(difference) < 0);
  mgp::duration_destroy(difference);

  return is_less;
}

inline std::string Date::ToString() const {
  return std::to_string(Year()) + "-" + std::to_string(Month()) + "-" + std::to_string(Day());
}

// LocalTime:

inline LocalTime::LocalTime(mgp_local_time *ptr) : ptr_(mgp::MemHandlerCallback(local_time_copy, ptr)) {}

inline LocalTime::LocalTime(const mgp_local_time *const_ptr)
    : ptr_(mgp::MemHandlerCallback(local_time_copy, const_cast<mgp_local_time *>(const_ptr))) {}

inline LocalTime::LocalTime(std::string_view string)
    : ptr_(mgp::MemHandlerCallback(local_time_from_string, string.data())) {}

inline LocalTime::LocalTime(int hour, int minute, int second, int millisecond, int microsecond) {
  mgp_local_time_parameters params{
      .hour = hour, .minute = minute, .second = second, .millisecond = millisecond, .microsecond = microsecond};
  ptr_ = mgp::MemHandlerCallback(local_time_from_parameters, &params);
}

inline LocalTime::LocalTime(const LocalTime &other) noexcept : LocalTime(other.ptr_) {}

inline LocalTime::LocalTime(LocalTime &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; };

inline LocalTime &LocalTime::operator=(const LocalTime &other) noexcept {
  if (this != &other) {
    mgp::local_time_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(local_time_copy, other.ptr_);
  }
  return *this;
}

inline LocalTime &LocalTime::operator=(LocalTime &&other) noexcept {
  if (this != &other) {
    mgp::local_time_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline LocalTime::~LocalTime() {
  if (ptr_ != nullptr) {
    mgp::local_time_destroy(ptr_);
  }
}

inline LocalTime LocalTime::Now() {
  auto *mgp_local_time = mgp::MemHandlerCallback(local_time_now);
  auto local_time = LocalTime(mgp_local_time);
  mgp::local_time_destroy(mgp_local_time);

  return local_time;
}

inline int LocalTime::Hour() const { return mgp::local_time_get_hour(ptr_); }

inline int LocalTime::Minute() const { return mgp::local_time_get_minute(ptr_); }

inline int LocalTime::Second() const { return mgp::local_time_get_second(ptr_); }

inline int LocalTime::Millisecond() const { return mgp::local_time_get_millisecond(ptr_); }

inline int LocalTime::Microsecond() const { return mgp::local_time_get_microsecond(ptr_); }

inline int64_t LocalTime::Timestamp() const { return mgp::local_time_timestamp(ptr_); }

inline bool LocalTime::operator==(const LocalTime &other) const { return util::LocalTimesEqual(ptr_, other.ptr_); }

inline LocalTime LocalTime::operator+(const Duration &dur) const {
  auto *mgp_sum = mgp::MemHandlerCallback(local_time_add_duration, ptr_, dur.ptr_);
  auto sum = LocalTime(mgp_sum);
  mgp::local_time_destroy(mgp_sum);

  return sum;
}

inline LocalTime LocalTime::operator-(const Duration &dur) const {
  auto *mgp_difference = mgp::MemHandlerCallback(local_time_sub_duration, ptr_, dur.ptr_);
  auto difference = LocalTime(mgp_difference);
  mgp::local_time_destroy(mgp_difference);

  return difference;
}

inline Duration LocalTime::operator-(const LocalTime &other) const {
  auto *mgp_difference = mgp::MemHandlerCallback(local_time_diff, ptr_, other.ptr_);
  auto difference = Duration(mgp_difference);
  mgp::duration_destroy(mgp_difference);

  return difference;
}

inline bool LocalTime::operator<(const LocalTime &other) const {
  auto *difference = mgp::MemHandlerCallback(local_time_diff, ptr_, other.ptr_);
  auto is_less = (mgp::duration_get_microseconds(difference) < 0);
  mgp::duration_destroy(difference);

  return is_less;
}

inline std::string LocalTime::ToString() const {
  return std::to_string(Hour()) + ":" + std::to_string(Minute()) + ":" + std::to_string(Second()) + "," +
         std::to_string(Millisecond()) + std::to_string(Microsecond());
}

// LocalDateTime:

inline LocalDateTime::LocalDateTime(mgp_local_date_time *ptr)
    : ptr_(mgp::MemHandlerCallback(local_date_time_copy, ptr)) {}

inline LocalDateTime::LocalDateTime(const mgp_local_date_time *const_ptr)
    : ptr_(mgp::MemHandlerCallback(local_date_time_copy, const_cast<mgp_local_date_time *>(const_ptr))) {}

inline LocalDateTime::LocalDateTime(std::string_view string)
    : ptr_(mgp::MemHandlerCallback(local_date_time_from_string, string.data())) {}

inline LocalDateTime::LocalDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond,
                                    int microsecond) {
  struct mgp_date_parameters date_params {
    .year = year, .month = month, .day = day
  };
  struct mgp_local_time_parameters local_time_params {
    .hour = hour, .minute = minute, .second = second, .millisecond = millisecond, .microsecond = microsecond
  };
  mgp_local_date_time_parameters params{.date_parameters = &date_params, .local_time_parameters = &local_time_params};
  ptr_ = mgp::MemHandlerCallback(local_date_time_from_parameters, &params);
}

inline LocalDateTime::LocalDateTime(const LocalDateTime &other) noexcept : LocalDateTime(other.ptr_) {}

inline LocalDateTime::LocalDateTime(LocalDateTime &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; };

inline LocalDateTime &LocalDateTime::operator=(const LocalDateTime &other) noexcept {
  if (this != &other) {
    mgp::local_date_time_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(local_date_time_copy, other.ptr_);
  }
  return *this;
}

inline LocalDateTime &LocalDateTime::operator=(LocalDateTime &&other) noexcept {
  if (this != &other) {
    mgp::local_date_time_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline LocalDateTime::~LocalDateTime() {
  if (ptr_ != nullptr) {
    mgp::local_date_time_destroy(ptr_);
  }
}

inline LocalDateTime LocalDateTime::Now() {
  auto *mgp_local_date_time = mgp::MemHandlerCallback(local_date_time_now);
  auto local_date_time = LocalDateTime(mgp_local_date_time);
  mgp::local_date_time_destroy(mgp_local_date_time);

  return local_date_time;
}

inline int LocalDateTime::Year() const { return mgp::local_date_time_get_year(ptr_); }

inline int LocalDateTime::Month() const { return mgp::local_date_time_get_month(ptr_); }

inline int LocalDateTime::Day() const { return mgp::local_date_time_get_day(ptr_); }

inline int LocalDateTime::Hour() const { return mgp::local_date_time_get_hour(ptr_); }

inline int LocalDateTime::Minute() const { return mgp::local_date_time_get_minute(ptr_); }

inline int LocalDateTime::Second() const { return mgp::local_date_time_get_second(ptr_); }

inline int LocalDateTime::Millisecond() const { return mgp::local_date_time_get_millisecond(ptr_); }

inline int LocalDateTime::Microsecond() const { return mgp::local_date_time_get_microsecond(ptr_); }

inline int64_t LocalDateTime::Timestamp() const { return mgp::local_date_time_timestamp(ptr_); }

inline bool LocalDateTime::operator==(const LocalDateTime &other) const {
  return util::LocalDateTimesEqual(ptr_, other.ptr_);
}

inline LocalDateTime LocalDateTime::operator+(const Duration &dur) const {
  auto *mgp_sum = mgp::MemHandlerCallback(local_date_time_add_duration, ptr_, dur.ptr_);
  auto sum = LocalDateTime(mgp_sum);
  mgp::local_date_time_destroy(mgp_sum);

  return sum;
}

inline LocalDateTime LocalDateTime::operator-(const Duration &dur) const {
  auto *mgp_difference = mgp::MemHandlerCallback(local_date_time_sub_duration, ptr_, dur.ptr_);
  auto difference = LocalDateTime(mgp_difference);
  mgp::local_date_time_destroy(mgp_difference);

  return difference;
}

inline Duration LocalDateTime::operator-(const LocalDateTime &other) const {
  auto *mgp_difference = mgp::MemHandlerCallback(local_date_time_diff, ptr_, other.ptr_);
  auto difference = Duration(mgp_difference);
  mgp::duration_destroy(mgp_difference);

  return difference;
}

inline bool LocalDateTime::operator<(const LocalDateTime &other) const {
  auto *difference = mgp::MemHandlerCallback(local_date_time_diff, ptr_, other.ptr_);
  auto is_less = (mgp::duration_get_microseconds(difference) < 0);
  mgp::duration_destroy(difference);

  return is_less;
}

inline std::string LocalDateTime::ToString() const {
  return std::to_string(Year()) + "-" + std::to_string(Month()) + "-" + std::to_string(Day()) + "T" +
         std::to_string(Hour()) + ":" + std::to_string(Minute()) + ":" + std::to_string(Second()) + "," +
         std::to_string(Millisecond()) + std::to_string(Microsecond());
}

// Duration:

inline Duration::Duration(mgp_duration *ptr) : ptr_(mgp::MemHandlerCallback(duration_copy, ptr)) {}

inline Duration::Duration(const mgp_duration *const_ptr)
    : ptr_(mgp::MemHandlerCallback(duration_copy, const_cast<mgp_duration *>(const_ptr))) {}

inline Duration::Duration(std::string_view string)
    : ptr_(mgp::MemHandlerCallback(duration_from_string, string.data())) {}

inline Duration::Duration(int64_t microseconds)
    : ptr_(mgp::MemHandlerCallback(duration_from_microseconds, microseconds)) {}

inline Duration::Duration(double day, double hour, double minute, double second, double millisecond,
                          double microsecond) {
  mgp_duration_parameters params{.day = day,
                                 .hour = hour,
                                 .minute = minute,
                                 .second = second,
                                 .millisecond = millisecond,
                                 .microsecond = microsecond};
  ptr_ = mgp::MemHandlerCallback(duration_from_parameters, &params);
}

inline Duration::Duration(const Duration &other) noexcept : Duration(other.ptr_) {}

inline Duration::Duration(Duration &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; };

inline Duration &Duration::operator=(const Duration &other) noexcept {
  if (this != &other) {
    mgp::duration_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(duration_copy, other.ptr_);
  }
  return *this;
}

inline Duration &Duration::operator=(Duration &&other) noexcept {
  if (this != &other) {
    mgp::duration_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline Duration::~Duration() {
  if (ptr_ != nullptr) {
    mgp::duration_destroy(ptr_);
  }
}

inline int64_t Duration::Microseconds() const { return mgp::duration_get_microseconds(ptr_); }

inline bool Duration::operator==(const Duration &other) const { return util::DurationsEqual(ptr_, other.ptr_); }

inline Duration Duration::operator+(const Duration &other) const {
  auto *mgp_sum = mgp::MemHandlerCallback(duration_add, ptr_, other.ptr_);
  auto sum = Duration(mgp_sum);
  mgp::duration_destroy(mgp_sum);

  return sum;
}

inline Duration Duration::operator-(const Duration &other) const {
  auto *mgp_difference = mgp::MemHandlerCallback(duration_sub, ptr_, other.ptr_);
  auto difference = Duration(mgp_difference);
  mgp::duration_destroy(mgp_difference);

  return difference;
}

inline Duration Duration::operator-() const {
  auto *mgp_neg = mgp::MemHandlerCallback(duration_neg, ptr_);
  auto neg = Duration(mgp_neg);
  mgp::duration_destroy(mgp_neg);

  return neg;
}

inline bool Duration::operator<(const Duration &other) const {
  auto *difference = mgp::MemHandlerCallback(duration_sub, ptr_, other.ptr_);
  auto is_less = (mgp::duration_get_microseconds(difference) < 0);
  mgp::duration_destroy(difference);

  return is_less;
}

inline std::string Duration::ToString() const { return std::to_string(Microseconds()) + "ms"; }

/* #endregion */

/* #endregion */

/* #region Value */

inline Value::Value(mgp_value *ptr) : ptr_(mgp::MemHandlerCallback(value_copy, ptr)) {}
inline Value::Value(StealType /*steal*/, mgp_value *ptr) : ptr_{ptr} {}

inline Value::Value() : ptr_(mgp::MemHandlerCallback(value_make_null)) {}

inline Value::Value(const bool value) : ptr_(mgp::MemHandlerCallback(value_make_bool, value)) {}

inline Value::Value(const int64_t value) : ptr_(mgp::MemHandlerCallback(value_make_int, value)) {}

inline Value::Value(const double value) : ptr_(mgp::MemHandlerCallback(value_make_double, value)) {}

inline Value::Value(const char *value) : ptr_(mgp::MemHandlerCallback(value_make_string, value)) {}

inline Value::Value(const std::string_view value) : ptr_(mgp::MemHandlerCallback(value_make_string, value.data())) {}

inline Value::Value(const List &list) : ptr_(mgp::value_make_list(mgp::MemHandlerCallback(list_copy, list.ptr_))) {}

inline Value::Value(List &&list) {
  ptr_ = mgp::value_make_list(list.ptr_);
  list.ptr_ = nullptr;
}

inline Value::Value(const Map &map) : ptr_(mgp::value_make_map(mgp::MemHandlerCallback(map_copy, map.ptr_))) {}

inline Value::Value(Map &&map) {
  ptr_ = mgp::value_make_map(map.ptr_);
  map.ptr_ = nullptr;
}

inline Value::Value(const Node &node) : ptr_(mgp::value_make_vertex(mgp::MemHandlerCallback(vertex_copy, node.ptr_))) {}

inline Value::Value(Node &&node) {
  ptr_ = mgp::value_make_vertex(const_cast<mgp_vertex *>(node.ptr_));
  node.ptr_ = nullptr;
}

inline Value::Value(const Relationship &relationship)
    : ptr_(mgp::value_make_edge(mgp::MemHandlerCallback(edge_copy, relationship.ptr_))) {}

inline Value::Value(Relationship &&relationship) {
  ptr_ = mgp::value_make_edge(const_cast<mgp_edge *>(relationship.ptr_));
  relationship.ptr_ = nullptr;
}

inline Value::Value(const Path &path) : ptr_(mgp::value_make_path(mgp::MemHandlerCallback(path_copy, path.ptr_))) {}

inline Value::Value(Path &&path) {
  ptr_ = mgp::value_make_path(path.ptr_);
  path.ptr_ = nullptr;
}

inline Value::Value(const Date &date) : ptr_(mgp::value_make_date(mgp::MemHandlerCallback(date_copy, date.ptr_))) {}

inline Value::Value(Date &&date) {
  ptr_ = mgp::value_make_date(date.ptr_);
  date.ptr_ = nullptr;
}

inline Value::Value(const LocalTime &local_time)
    : ptr_(mgp::value_make_local_time(mgp::MemHandlerCallback(local_time_copy, local_time.ptr_))) {}

inline Value::Value(LocalTime &&local_time) {
  ptr_ = mgp::value_make_local_time(local_time.ptr_);
  local_time.ptr_ = nullptr;
}

inline Value::Value(const LocalDateTime &local_date_time)
    : ptr_(mgp::value_make_local_date_time(mgp::MemHandlerCallback(local_date_time_copy, local_date_time.ptr_))) {}

inline Value::Value(LocalDateTime &&local_date_time) {
  ptr_ = mgp::value_make_local_date_time(local_date_time.ptr_);
  local_date_time.ptr_ = nullptr;
}

inline Value::Value(const Duration &duration)
    : ptr_(mgp::value_make_duration(mgp::MemHandlerCallback(duration_copy, duration.ptr_))) {}

inline Value::Value(Duration &&duration) {
  ptr_ = mgp::value_make_duration(duration.ptr_);
  duration.ptr_ = nullptr;
}

inline Value::Value(const Value &other) noexcept : Value(other.ptr_) {}

inline Value::Value(Value &&other) noexcept : ptr_(other.ptr_) { other.ptr_ = nullptr; }

inline Value &Value::operator=(const Value &other) noexcept {
  if (this != &other) {
    mgp::value_destroy(ptr_);

    ptr_ = mgp::MemHandlerCallback(value_copy, other.ptr_);
  }
  return *this;
}

inline Value &Value::operator=(Value &&other) noexcept {
  if (this != &other) {
    mgp::value_destroy(ptr_);

    ptr_ = other.ptr_;
    other.ptr_ = nullptr;
  }
  return *this;
}

inline Value::~Value() {
  if (ptr_ != nullptr) {
    mgp::value_destroy(ptr_);
  }
}

inline mgp_value *Value::ptr() const { return ptr_; }

inline mgp::Type Value::Type() const { return util::ToAPIType(mgp::value_get_type(ptr_)); }

inline bool Value::ValueBool() const {
  if (Type() != Type::Bool) {
    throw ValueException("Type of value is wrong: expected Bool.");
  }
  return mgp::value_get_bool(ptr_);
}
inline bool Value::ValueBool() {
  if (Type() != Type::Bool) {
    throw ValueException("Type of value is wrong: expected Bool.");
  }
  return mgp::value_get_bool(ptr_);
}

inline std::int64_t Value::ValueInt() const {
  if (Type() != Type::Int) {
    throw ValueException("Type of value is wrong: expected Int.");
  }
  return mgp::value_get_int(ptr_);
}
inline std::int64_t Value::ValueInt() {
  if (Type() != Type::Int) {
    throw ValueException("Type of value is wrong: expected Int.");
  }
  return mgp::value_get_int(ptr_);
}

inline double Value::ValueDouble() const {
  if (Type() != Type::Double) {
    throw ValueException("Type of value is wrong: expected Double.");
  }
  return mgp::value_get_double(ptr_);
}
inline double Value::ValueDouble() {
  if (Type() != Type::Double) {
    throw ValueException("Type of value is wrong: expected Double.");
  }
  return mgp::value_get_double(ptr_);
}

inline double Value::ValueNumeric() const {
  if (Type() != Type::Int && Type() != Type::Double) {
    throw ValueException("Type of value is wrong: expected Int or Double.");
  }
  if (Type() == Type::Int) {
    return static_cast<double>(mgp::value_get_int(ptr_));
  }
  return mgp::value_get_double(ptr_);
}
inline double Value::ValueNumeric() {
  if (Type() != Type::Int && Type() != Type::Double) {
    throw ValueException("Type of value is wrong: expected Int or Double.");
  }
  if (Type() == Type::Int) {
    return static_cast<double>(mgp::value_get_int(ptr_));
  }
  return mgp::value_get_double(ptr_);
}

inline std::string_view Value::ValueString() const {
  if (Type() != Type::String) {
    throw ValueException("Type of value is wrong: expected String.");
  }
  return mgp::value_get_string(ptr_);
}
inline std::string_view Value::ValueString() {
  if (Type() != Type::String) {
    throw ValueException("Type of value is wrong: expected String.");
  }
  return mgp::value_get_string(ptr_);
}

inline List Value::ValueList() const {
  if (Type() != Type::List) {
    throw ValueException("Type of value is wrong: expected List.");
  }
  return List(mgp::value_get_list(ptr_));
}
inline List Value::ValueList() {
  if (Type() != Type::List) {
    throw ValueException("Type of value is wrong: expected List.");
  }
  return List(mgp::value_get_list(ptr_));
}

inline Map Value::ValueMap() const {
  if (Type() != Type::Map) {
    throw ValueException("Type of value is wrong: expected Map.");
  }
  return Map(mgp::value_get_map(ptr_));
}
inline Map Value::ValueMap() {
  if (Type() != Type::Map) {
    throw ValueException("Type of value is wrong: expected Map.");
  }
  return Map(mgp::value_get_map(ptr_));
}

inline Node Value::ValueNode() const {
  if (Type() != Type::Node) {
    throw ValueException("Type of value is wrong: expected Node.");
  }
  return Node(mgp::value_get_vertex(ptr_));
}
inline Node Value::ValueNode() {
  if (Type() != Type::Node) {
    throw ValueException("Type of value is wrong: expected Node.");
  }
  return Node(mgp::value_get_vertex(ptr_));
}

inline Relationship Value::ValueRelationship() const {
  if (Type() != Type::Relationship) {
    throw ValueException("Type of value is wrong: expected Relationship.");
  }
  return Relationship(mgp::value_get_edge(ptr_));
}
inline Relationship Value::ValueRelationship() {
  if (Type() != Type::Relationship) {
    throw ValueException("Type of value is wrong: expected Relationship.");
  }
  return Relationship(mgp::value_get_edge(ptr_));
}

inline Path Value::ValuePath() const {
  if (Type() != Type::Path) {
    throw ValueException("Type of value is wrong: expected Path.");
  }
  return Path(mgp::value_get_path(ptr_));
}
inline Path Value::ValuePath() {
  if (Type() != Type::Path) {
    throw ValueException("Type of value is wrong: expected Path.");
  }
  return Path(mgp::value_get_path(ptr_));
}

inline Date Value::ValueDate() const {
  if (Type() != Type::Date) {
    throw ValueException("Type of value is wrong: expected Date.");
  }
  return Date(mgp::value_get_date(ptr_));
}
inline Date Value::ValueDate() {
  if (Type() != Type::Date) {
    throw ValueException("Type of value is wrong: expected Date.");
  }
  return Date(mgp::value_get_date(ptr_));
}

inline LocalTime Value::ValueLocalTime() const {
  if (Type() != Type::LocalTime) {
    throw ValueException("Type of value is wrong: expected LocalTime.");
  }
  return LocalTime(mgp::value_get_local_time(ptr_));
}
inline LocalTime Value::ValueLocalTime() {
  if (Type() != Type::LocalTime) {
    throw ValueException("Type of value is wrong: expected LocalTime.");
  }
  return LocalTime(mgp::value_get_local_time(ptr_));
}

inline LocalDateTime Value::ValueLocalDateTime() const {
  if (Type() != Type::LocalDateTime) {
    throw ValueException("Type of value is wrong: expected LocalDateTime.");
  }
  return LocalDateTime(mgp::value_get_local_date_time(ptr_));
}
inline LocalDateTime Value::ValueLocalDateTime() {
  if (Type() != Type::LocalDateTime) {
    throw ValueException("Type of value is wrong: expected LocalDateTime.");
  }
  return LocalDateTime(mgp::value_get_local_date_time(ptr_));
}

inline Duration Value::ValueDuration() const {
  if (Type() != Type::Duration) {
    throw ValueException("Type of value is wrong: expected Duration.");
  }
  return Duration(mgp::value_get_duration(ptr_));
}
inline Duration Value::ValueDuration() {
  if (Type() != Type::Duration) {
    throw ValueException("Type of value is wrong: expected Duration.");
  }
  return Duration(mgp::value_get_duration(ptr_));
}

inline bool Value::IsNull() const { return mgp::value_is_null(ptr_); }

inline bool Value::IsBool() const { return mgp::value_is_bool(ptr_); }

inline bool Value::IsInt() const { return mgp::value_is_int(ptr_); }

inline bool Value::IsDouble() const { return mgp::value_is_double(ptr_); }

inline bool Value::IsNumeric() const { return IsInt() || IsDouble(); }

inline bool Value::IsString() const { return mgp::value_is_string(ptr_); }

inline bool Value::IsList() const { return mgp::value_is_list(ptr_); }

inline bool Value::IsMap() const { return mgp::value_is_map(ptr_); }

inline bool Value::IsNode() const { return mgp::value_is_vertex(ptr_); }

inline bool Value::IsRelationship() const { return mgp::value_is_edge(ptr_); }

inline bool Value::IsPath() const { return mgp::value_is_path(ptr_); }

inline bool Value::IsDate() const { return mgp::value_is_date(ptr_); }

inline bool Value::IsLocalTime() const { return mgp::value_is_local_time(ptr_); }

inline bool Value::IsLocalDateTime() const { return mgp::value_is_local_date_time(ptr_); }

inline bool Value::IsDuration() const { return mgp::value_is_duration(ptr_); }

inline bool Value::operator==(const Value &other) const { return util::ValuesEqual(ptr_, other.ptr_); }

inline bool Value::operator!=(const Value &other) const { return !(*this == other); }

inline bool Value::operator<(const Value &other) const {
  const mgp::Type &type = Type();
  if (type != other.Type() && !(IsNumeric() && other.IsNumeric())) {
    throw ValueException("Values have to be of the same type");
  }

  switch (type) {
    case Type::Null:
      throw ValueException("Cannot compare Null types");
    case Type::Bool:
      return ValueBool() < other.ValueBool();
    case Type::Int:
      return ValueNumeric() < other.ValueNumeric();
    case Type::Double:
      return ValueNumeric() < other.ValueNumeric();
    case Type::String:
      return ValueString() < other.ValueString();
    case Type::Node:
      return ValueNode() < other.ValueNode();
    case Type::Relationship:
      return ValueRelationship() < other.ValueRelationship();
    case Type::Date:
      return ValueDate() < other.ValueDate();
    case Type::LocalTime:
      return ValueLocalTime() < other.ValueLocalTime();
    case Type::LocalDateTime:
      return ValueLocalDateTime() < other.ValueLocalDateTime();
    case Type::Duration:
      return ValueDuration() < other.ValueDuration();
    case Type::Path:
    case Type::List:
    case Type::Map:
      throw ValueException("Operator < is not defined for this Path, List or Map data type");
    default:
      throw ValueException("Undefined behaviour");
  }
}

inline std::ostream &operator<<(std::ostream &os, const mgp::Value &value) {
  switch (value.Type()) {
    case mgp::Type::Null:
      return os << "null";
    case mgp::Type::Any:
      return os << "any";
    case mgp::Type::Bool:
      return os << (value.ValueBool() ? "true" : "false");
    case mgp::Type::Int:
      return os << std::to_string(value.ValueInt());
    case mgp::Type::Double:
      return os << std::to_string(value.ValueDouble());
    case mgp::Type::String:
      return os << std::string(value.ValueString());
    case mgp::Type::List:
      throw mgp::ValueException("Printing mgp::List type currently not supported.");
    case mgp::Type::Map:
      throw mgp::ValueException("Printing mgp::Map type currently not supported.");
    case mgp::Type::Node:
      return os << "Node[" + std::to_string(value.ValueNode().Id().AsInt()) + "]";
    case mgp::Type::Relationship:
      return os << "Relationship[" + std::to_string(value.ValueRelationship().Id().AsInt()) + "]";
    case mgp::Type::Path:
      throw mgp::ValueException("Printing mgp::Path type currently not supported.");
    case mgp::Type::Date: {
      const auto date{value.ValueDate()};
      return os << std::to_string(date.Year()) + "-" + std::to_string(date.Month()) + "-" + std::to_string(date.Day());
    }
    case mgp::Type::LocalTime: {
      const auto localTime{value.ValueLocalTime()};
      return os << std::to_string(localTime.Hour()) + ":" + std::to_string(localTime.Minute()) + ":" +
                       std::to_string(localTime.Second()) + "," + std::to_string(localTime.Millisecond()) +
                       std::to_string(localTime.Microsecond());
    }
    case mgp::Type::LocalDateTime: {
      const auto localDateTime = value.ValueLocalDateTime();
      return os << std::to_string(localDateTime.Year()) + "-" + std::to_string(localDateTime.Month()) + "-" +
                       std::to_string(localDateTime.Day()) + "T" + std::to_string(localDateTime.Hour()) + ":" +
                       std::to_string(localDateTime.Minute()) + ":" + std::to_string(localDateTime.Second()) + "," +
                       std::to_string(localDateTime.Millisecond()) + std::to_string(localDateTime.Microsecond());
    }
    case mgp::Type::Duration:
      return os << std::to_string(value.ValueDuration().Microseconds()) + "ms";
    default:
      throw mgp::ValueException("Unknown value type");
  }
}

inline std::ostream &operator<<(std::ostream &os, const mgp::Type &type) {
  switch (type) {
    case mgp::Type::Null:
      return os << "null";
    case mgp::Type::Bool:
      return os << "bool";
    case mgp::Type::Int:
      return os << "int";
    case mgp::Type::Double:
      return os << "double";
    case mgp::Type::String:
      return os << "string";
    case mgp::Type::List:
      return os << "list";
    case mgp::Type::Map:
      return os << "map";
    case mgp::Type::Node:
      return os << "vertex";
    case mgp::Type::Relationship:
      return os << "edge";
    case mgp::Type::Path:
      return os << "path";
    case mgp::Type::Date:
      return os << "date";
    case mgp::Type::LocalTime:
      return os << "local_time";
    case mgp::Type::LocalDateTime:
      return os << "local_date_time";
    case mgp::Type::Duration:
      return os << "duration";
    default:
      throw ValueException("Unknown type");
  }
}

inline std::string Value::ToString() const {
  const mgp::Type &type = Type();
  switch (type) {
    case Type::Null:
      return "";
    case Type::Bool:
      return ValueBool() ? "true" : "false";
    case Type::Int:
      return std::to_string(ValueInt());
    case Type::Double:
      return std::to_string(ValueDouble());
    case Type::String:
      return std::string(ValueString());
    case Type::Node:
      return ValueNode().ToString();
    case Type::Relationship:
      return ValueRelationship().ToString();
    case Type::Date:
      return ValueDate().ToString();
    case Type::LocalTime:
      return ValueLocalTime().ToString();
    case Type::LocalDateTime:
      return ValueLocalDateTime().ToString();
    case Type::Duration:
      return ValueDuration().ToString();
    case Type::List:
      return ValueList().ToString();
    case Type::Map:
      return ValueMap().ToString();
    case Type::Path:
      return ValuePath().ToString();
    default:
      throw ValueException("Undefined behaviour");
  }
}

/* #endregion */

/* #region Record */
// Record:

inline Record::Record(mgp_result_record *record) : record_(record) {}

inline void Record::Insert(const char *field_name, bool value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_bool, value);
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, std::int64_t value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_int, value);
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, double value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_double, value);
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, std::string_view value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_string, value.data());
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const char *value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_string, value);
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const List &list) {
  auto *mgp_val = mgp::value_make_list(mgp::MemHandlerCallback(list_copy, list.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const Map &map) {
  auto *mgp_val = mgp::value_make_map(mgp::MemHandlerCallback(map_copy, map.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const Node &node) {
  auto *mgp_val = mgp::value_make_vertex(mgp::MemHandlerCallback(vertex_copy, node.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const Relationship &relationship) {
  auto *mgp_val = mgp::value_make_edge(mgp::MemHandlerCallback(edge_copy, relationship.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const Path &path) {
  auto *mgp_val = mgp::value_make_path(mgp::MemHandlerCallback(path_copy, path.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const Date &date) {
  auto *mgp_val = mgp::value_make_date(mgp::MemHandlerCallback(date_copy, date.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const LocalTime &local_time) {
  auto *mgp_val = mgp::value_make_local_time(mgp::MemHandlerCallback(local_time_copy, local_time.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const LocalDateTime &local_date_time) {
  auto *mgp_val = mgp::value_make_local_date_time(mgp::MemHandlerCallback(local_date_time_copy, local_date_time.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const Duration &duration) {
  auto *mgp_val = mgp::value_make_duration(mgp::MemHandlerCallback(duration_copy, duration.ptr_));
  { mgp::result_record_insert(record_, field_name, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Record::Insert(const char *field_name, const Value &value) {
  switch (value.Type()) {
    case Type::Bool:
      return Insert(field_name, value.ValueBool());
    case Type::Int:
      return Insert(field_name, value.ValueInt());
    case Type::Double:
      return Insert(field_name, value.ValueDouble());
    case Type::String:
      return Insert(field_name, value.ValueString());
    case Type::List:
      return Insert(field_name, value.ValueList());
    case Type::Map:
      return Insert(field_name, value.ValueMap());
    case Type::Node:
      return Insert(field_name, value.ValueNode());
    case Type::Relationship:
      return Insert(field_name, value.ValueRelationship());
    case Type::Path:
      return Insert(field_name, value.ValuePath());
    case Type::Date:
      return Insert(field_name, value.ValueDate());
    case Type::LocalTime:
      return Insert(field_name, value.ValueLocalTime());
    case Type::LocalDateTime:
      return Insert(field_name, value.ValueLocalDateTime());
    case Type::Duration:
      return Insert(field_name, value.ValueDuration());

    default:
      throw ValueException("No Record.Insert for this datatype");
  }
}

// RecordFactory:

inline RecordFactory::RecordFactory(mgp_result *result) : result_(result) {}

inline Record RecordFactory::NewRecord() const {
  auto *record = mgp::result_new_record(result_);
  if (record == nullptr) {
    throw mg_exception::NotEnoughMemoryException();
  }
  return Record(record);
}

inline void RecordFactory::SetErrorMessage(const std::string_view error_msg) const {
  mgp::result_set_error_msg(result_, error_msg.data());
}

inline void RecordFactory::SetErrorMessage(const char *error_msg) const {
  mgp::result_set_error_msg(result_, error_msg);
}

// Result:

inline Result::Result(mgp_func_result *result) : result_(result) {}

inline void Result::SetValue(bool value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_bool, value);
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(std::int64_t value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_int, value);
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(double value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_double, value);
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(std::string_view value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_string, value.data());
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const char *value) {
  auto *mgp_val = mgp::MemHandlerCallback(value_make_string, value);
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const List &list) {
  auto *mgp_val = mgp::value_make_list(mgp::MemHandlerCallback(list_copy, list.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const Map &map) {
  auto *mgp_val = mgp::value_make_map(mgp::MemHandlerCallback(map_copy, map.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const Node &node) {
  auto *mgp_val = mgp::value_make_vertex(mgp::MemHandlerCallback(vertex_copy, node.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const Relationship &relationship) {
  auto *mgp_val = mgp::value_make_edge(mgp::MemHandlerCallback(edge_copy, relationship.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const Path &path) {
  auto *mgp_val = mgp::value_make_path(mgp::MemHandlerCallback(path_copy, path.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const Date &date) {
  auto *mgp_val = mgp::value_make_date(mgp::MemHandlerCallback(date_copy, date.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const LocalTime &local_time) {
  auto *mgp_val = mgp::value_make_local_time(mgp::MemHandlerCallback(local_time_copy, local_time.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const LocalDateTime &local_date_time) {
  auto *mgp_val = mgp::value_make_local_date_time(mgp::MemHandlerCallback(local_date_time_copy, local_date_time.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetValue(const Duration &duration) {
  auto *mgp_val = mgp::value_make_duration(mgp::MemHandlerCallback(duration_copy, duration.ptr_));
  { mgp::MemHandlerCallback(func_result_set_value, result_, mgp_val); }
  mgp::value_destroy(mgp_val);
}

inline void Result::SetErrorMessage(const std::string_view error_msg) const {
  mgp::MemHandlerCallback(func_result_set_error_msg, result_, error_msg.data());
}

inline void Result::SetErrorMessage(const char *error_msg) const {
  mgp::MemHandlerCallback(func_result_set_error_msg, result_, error_msg);
}

/* #endregion */

/* #region Module */

// Parameter:

inline Parameter::Parameter(std::string_view name, Type type) : name(name), type_(type) {}

inline Parameter::Parameter(std::string_view name, Type type, bool default_value)
    : name(name), type_(type), optional(true), default_value(Value(default_value)) {}

inline Parameter::Parameter(std::string_view name, Type type, int64_t default_value)
    : name(name), type_(type), optional(true), default_value(Value(default_value)) {}

inline Parameter::Parameter(std::string_view name, Type type, double default_value)
    : name(name), type_(type), optional(true), default_value(Value(default_value)) {}

inline Parameter::Parameter(std::string_view name, Type type, std::string_view default_value)
    : name(name), type_(type), optional(true), default_value(Value(default_value)) {}

inline Parameter::Parameter(std::string_view name, Type type, const char *default_value)
    : name(name), type_(type), optional(true), default_value(Value(default_value)) {}

inline Parameter::Parameter(std::string_view name, Type type, Value default_value)
    : name(name), type_(type), optional(true), default_value(std::move(default_value)) {}

inline Parameter::Parameter(std::string_view name, std::pair<Type, Type> list_type)
    : name(name), type_(list_type.first), list_item_type_(list_type.second) {}

inline Parameter::Parameter(std::string_view name, std::pair<Type, Type> list_type, Value default_value)
    : name(name),
      type_(list_type.first),
      list_item_type_(list_type.second),
      optional(true),
      default_value(std::move(default_value)) {}

inline mgp_type *Parameter::GetMGPType() const {
  if (type_ == Type::List) {
    return mgp::type_list(util::ToMGPType(list_item_type_));
  }

  return util::ToMGPType(type_);
}

// Return:

inline Return::Return(std::string_view name, Type type) : name(name), type_(type) {}

inline Return::Return(std::string_view name, std::pair<Type, Type> list_type)
    : name(name), type_(list_type.first), list_item_type_(list_type.second) {}

inline mgp_type *Return::GetMGPType() const {
  if (type_ == Type::List) {
    return mgp::type_list(util::ToMGPType(list_item_type_));
  }

  return util::ToMGPType(type_);
}

// do not enter
namespace detail {
inline void AddParamsReturnsToProc(mgp_proc *proc, std::vector<Parameter> &parameters,
                                   const std::vector<Return> &returns) {
  for (const auto &parameter : parameters) {
    const auto *parameter_name = parameter.name.data();
    if (!parameter.optional) {
      mgp::proc_add_arg(proc, parameter_name, parameter.GetMGPType());
    } else {
      mgp::proc_add_opt_arg(proc, parameter_name, parameter.GetMGPType(), parameter.default_value.ptr());
    }
  }

  for (const auto return_ : returns) {
    const auto *return_name = return_.name.data();
    mgp::proc_add_result(proc, return_name, return_.GetMGPType());
  }
}
}  // namespace detail

inline bool CreateLabelIndex(mgp_graph *memgraph_graph, const std::string_view label) {
  return create_label_index(memgraph_graph, label.data());
}

inline bool DropLabelIndex(mgp_graph *memgraph_graph, const std::string_view label) {
  return drop_label_index(memgraph_graph, label.data());
}

inline List ListAllLabelIndices(mgp_graph *memgraph_graph) {
  auto *label_indices = mgp::MemHandlerCallback(list_all_label_indices, memgraph_graph);
  if (label_indices == nullptr) {
    throw ValueException("Couldn't list all label indices");
  }
  return List(label_indices);
}

inline bool CreateLabelPropertyIndex(mgp_graph *memgraph_graph, const std::string_view label,
                                     const std::string_view property) {
  return create_label_property_index(memgraph_graph, label.data(), property.data());
}

inline bool DropLabelPropertyIndex(mgp_graph *memgraph_graph, const std::string_view label,
                                   const std::string_view property) {
  return drop_label_property_index(memgraph_graph, label.data(), property.data());
}

inline List ListAllLabelPropertyIndices(mgp_graph *memgraph_graph) {
  auto *label_property_indices = mgp::MemHandlerCallback(list_all_label_property_indices, memgraph_graph);
  if (label_property_indices == nullptr) {
    throw ValueException("Couldn't list all label+property indices");
  }
  return List(label_property_indices);
}

namespace {
constexpr std::string_view kErrorMsgKey = "error_msg";
constexpr std::string_view kSearchResultsKey = "search_results";
constexpr std::string_view kAggregationResultsKey = "aggregation_results";
}  // namespace

inline List SearchTextIndex(mgp_graph *memgraph_graph, std::string_view index_name, std::string_view search_query,
                            text_search_mode search_mode) {
  auto results_or_error = Map(mgp::MemHandlerCallback(graph_search_text_index, memgraph_graph, index_name.data(),
                                                      search_query.data(), search_mode));
  if (!results_or_error.KeyExists(kErrorMsgKey) || !results_or_error.KeyExists(kSearchResultsKey)) {
    throw TextSearchException{"Incomplete text index search results!"};
  }
  if (!results_or_error.At(kErrorMsgKey).IsString() || !results_or_error.At(kSearchResultsKey).IsList()) {
    throw TextSearchException{"Text index search results have wrong type!"};
  }

  auto maybe_error = results_or_error[kErrorMsgKey].ValueString();
  if (!maybe_error.empty()) {
    throw TextSearchException(maybe_error.data());
  }

  return results_or_error[kSearchResultsKey].ValueList();
}

inline std::string_view AggregateOverTextIndex(mgp_graph *memgraph_graph, std::string_view index_name,
                                               std::string_view search_query, std::string_view aggregation_query) {
  auto results_or_error =
      Map(mgp::MemHandlerCallback(graph_aggregate_over_text_index, memgraph_graph, index_name.data(),
                                  search_query.data(), aggregation_query.data()));
  if (!results_or_error.KeyExists(kErrorMsgKey) || !results_or_error.KeyExists(kAggregationResultsKey)) {
    throw TextSearchException{"Incomplete text index aggregation results!"};
  }
  if (!results_or_error.At(kErrorMsgKey).IsString() || !results_or_error.At(kAggregationResultsKey).IsString()) {
    throw TextSearchException{"Text index aggregation results have wrong type!"};
  }

  auto maybe_error = results_or_error[kErrorMsgKey].ValueString();
  if (!maybe_error.empty()) {
    throw TextSearchException(maybe_error.data());
  }

  return results_or_error[kAggregationResultsKey].ValueString();
}

inline bool CreateExistenceConstraint(mgp_graph *memgraph_graph, const std::string_view label,
                                      const std::string_view property) {
  return create_existence_constraint(memgraph_graph, label.data(), property.data());
}

inline bool DropExistenceConstraint(mgp_graph *memgraph_graph, const std::string_view label,
                                    const std::string_view property) {
  return drop_existence_constraint(memgraph_graph, label.data(), property.data());
}

inline List ListAllExistenceConstraints(mgp_graph *memgraph_graph) {
  auto *existence_constraints = mgp::MemHandlerCallback(list_all_existence_constraints, memgraph_graph);
  if (existence_constraints == nullptr) {
    throw ValueException("Couldn't list all existence_constraints");
  }
  return List(existence_constraints);
}

inline bool CreateUniqueConstraint(mgp_graph *memgraph_graph, const std::string_view label, mgp_value *properties) {
  return create_unique_constraint(memgraph_graph, label.data(), properties);
}

inline bool DropUniqueConstraint(mgp_graph *memgraph_graph, const std::string_view label, mgp_value *properties) {
  return drop_unique_constraint(memgraph_graph, label.data(), properties);
}

inline List ListAllUniqueConstraints(mgp_graph *memgraph_graph) {
  auto *unique_constraints = mgp::MemHandlerCallback(list_all_unique_constraints, memgraph_graph);
  if (unique_constraints == nullptr) {
    throw ValueException("Couldn't list all unique_constraints");
  }
  return List(unique_constraints);
}

void AddProcedure(mgp_proc_cb callback, std::string_view name, ProcedureType proc_type,
                  std::vector<Parameter> parameters, std::vector<Return> returns, mgp_module *module,
                  mgp_memory * /*memory*/) {
  auto *proc = (proc_type == ProcedureType::Read) ? mgp::module_add_read_procedure(module, name.data(), callback)
                                                  : mgp::module_add_write_procedure(module, name.data(), callback);
  detail::AddParamsReturnsToProc(proc, parameters, returns);
}

void AddBatchProcedure(mgp_proc_cb callback, mgp_proc_initializer initializer, mgp_proc_cleanup cleanup,
                       std::string_view name, ProcedureType proc_type, std::vector<Parameter> parameters,
                       std::vector<Return> returns, mgp_module *module, mgp_memory * /*memory*/) {
  auto *proc = (proc_type == ProcedureType::Read)
                   ? mgp::module_add_batch_read_procedure(module, name.data(), callback, initializer, cleanup)
                   : mgp::module_add_batch_write_procedure(module, name.data(), callback, initializer, cleanup);
  detail::AddParamsReturnsToProc(proc, parameters, returns);
}

void AddFunction(mgp_func_cb callback, std::string_view name, std::vector<Parameter> parameters, mgp_module *module,
                 mgp_memory *memory) {
  auto *func = mgp::module_add_function(module, name.data(), callback);

  for (const auto &parameter : parameters) {
    const auto *parameter_name = parameter.name.data();

    if (!parameter.optional) {
      mgp::func_add_arg(func, parameter_name, parameter.GetMGPType());
    } else {
      mgp::func_add_opt_arg(func, parameter_name, parameter.GetMGPType(), parameter.default_value.ptr());
    }
  }
}

/* #endregion */

}  // namespace mgp

namespace std {
template <>
struct hash<mgp::Id> {
  size_t operator()(const mgp::Id &x) const { return hash<int64_t>()(x.AsInt()); };
};

template <>
struct hash<mgp::Node> {
  size_t operator()(const mgp::Node &x) const { return hash<int64_t>()(x.Id().AsInt()); };
};

template <>
struct hash<mgp::Relationship> {
  size_t operator()(const mgp::Relationship &x) const { return hash<int64_t>()(x.Id().AsInt()); };
};

template <>
struct hash<mgp::Path> {
  size_t operator()(const mgp::Path &x) const {
    // https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
    // See mgp::util::FnvCollection
    constexpr const uint64_t fnv_prime = 1099511628211U;
    uint64_t hash = 14695981039346656037U;

    auto multiply_and_xor = [](uint64_t &hash, size_t element_hash) {
      hash *= fnv_prime;
      hash ^= element_hash;
    };

    for (size_t i = 0; i < x.Length() - 1; ++i) {
      multiply_and_xor(hash, std::hash<mgp::Node>{}(x.GetNodeAt(i)));
      multiply_and_xor(hash, std::hash<mgp::Relationship>{}(x.GetRelationshipAt(i)));
    }
    multiply_and_xor(hash, std::hash<mgp::Node>{}(x.GetNodeAt(x.Length())));
    return hash;
  }
};

template <>
struct hash<mgp::Date> {
  size_t operator()(const mgp::Date &x) const { return hash<int64_t>()(x.Timestamp()); };
};

template <>
struct hash<mgp::LocalTime> {
  size_t operator()(const mgp::LocalTime &x) const { return hash<int64_t>()(x.Timestamp()); };
};

template <>
struct hash<mgp::LocalDateTime> {
  size_t operator()(const mgp::LocalDateTime &x) const { return hash<int64_t>()(x.Timestamp()); };
};

template <>
struct hash<mgp::Duration> {
  size_t operator()(const mgp::Duration &x) const { return hash<int64_t>()(x.Microseconds()); };
};

template <>
struct hash<mgp::MapItem> {
  size_t operator()(const mgp::MapItem &x) const { return hash<std::string_view>()(x.key); };
};

template <>
struct hash<mgp::Map> {
  size_t operator()(const mgp::Map &x) const {
    return mgp::util::FnvCollection<mgp::Map, mgp::MapItem, std::hash<mgp::MapItem>>{}(x);
  }
};

template <>
struct hash<mgp::Value> {
  size_t operator()(const mgp::Value &x) const {
    switch (x.Type()) {
      case mgp::Type::Null:
        return 31;
      case mgp::Type::Any:
        throw mg_exception::InvalidArgumentException();
      case mgp::Type::Bool:
        return std::hash<bool>{}(x.ValueBool());
      case mgp::Type::Int:
        // we cast int to double for hashing purposes
        // to be consistent with equality (2.0 == 2) == true
        return std::hash<double>{}((double)x.ValueInt());
      case mgp::Type::Double:
        return std::hash<double>{}(x.ValueDouble());
      case mgp::Type::String:
        return std::hash<std::string_view>{}(x.ValueString());
      case mgp::Type::List:
        return mgp::util::FnvCollection<mgp::List, mgp::Value, std::hash<mgp::Value>>{}(x.ValueList());
      case mgp::Type::Map:
        return std::hash<mgp::Map>{}(x.ValueMap());
      case mgp::Type::Node:
        return std::hash<mgp::Node>{}(x.ValueNode());
      case mgp::Type::Relationship:
        return std::hash<mgp::Relationship>{}(x.ValueRelationship());
      case mgp::Type::Path:
        return std::hash<mgp::Path>{}(x.ValuePath());
      case mgp::Type::Date:
        return std::hash<mgp::Date>{}(x.ValueDate());
      case mgp::Type::LocalTime:
        return std::hash<mgp::LocalTime>{}(x.ValueLocalTime());
      case mgp::Type::LocalDateTime:
        return std::hash<mgp::LocalDateTime>{}(x.ValueLocalDateTime());
      case mgp::Type::Duration:
        return std::hash<mgp::Duration>{}(x.ValueDuration());
    }
    throw mg_exception::InvalidArgumentException();
  }
};

template <>
struct hash<mgp::List> {
  size_t operator()(const mgp::List &x) {
    return mgp::util::FnvCollection<mgp::List, mgp::Value, std::hash<mgp::Value>>{}(x);
  }
};
}  // namespace std
