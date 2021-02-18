#pragma once

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "utils/cast.hpp"
#include "utils/exceptions.hpp"

namespace communication::bolt {

/** Forward declaration of Value class. */
class Value;

/** Wraps int64_t to prevent dangerous implicit conversions. */
class Id {
 public:
  Id() = default;

  /** Construct Id from uint64_t */
  static Id FromUint(uint64_t id) { return Id(utils::MemcpyCast<int64_t>(id)); }

  /** Construct Id from int64_t */
  static Id FromInt(int64_t id) { return Id(id); }

  int64_t AsInt() const { return id_; }
  uint64_t AsUint() const { return utils::MemcpyCast<uint64_t>(id_); }

 private:
  explicit Id(int64_t id) : id_(id) {}

  int64_t id_;
};

inline bool operator==(const Id &id1, const Id &id2) { return id1.AsInt() == id2.AsInt(); }

inline bool operator!=(const Id &id1, const Id &id2) { return !(id1 == id2); }

/**
 * Structure used when reading a Vertex with the decoder.
 * The decoder writes data into this structure.
 */
struct Vertex {
  Id id;
  std::vector<std::string> labels;
  std::map<std::string, Value> properties;
};

/**
 * Structure used when reading an Edge with the decoder.
 * The decoder writes data into this structure.
 */
struct Edge {
  Id id;
  Id from;
  Id to;
  std::string type;
  std::map<std::string, Value> properties;
};

/**
 * Structure used when reading an UnboundEdge with the decoder.
 * The decoder writes data into this structure.
 */
struct UnboundedEdge {
  Id id;
  std::string type;
  std::map<std::string, Value> properties;
};

/**
 * Structure used when reading a Path with the decoder.
 * The decoder writes data into this structure.
 */
struct Path {
  Path() {}

  Path(const std::vector<Vertex> &vertices, const std::vector<Edge> &edges) {
    // Helper function. Looks for the given element in the collection. If found,
    // puts its index into `indices`. Otherwise emplaces the given element
    // into the collection and puts that index into `indices`. A multiplier is
    // added to switch between positive and negative indices (that define edge
    // direction).
    auto add_element = [this](auto &collection, const auto &element, int multiplier, int offset) {
      auto found =
          std::find_if(collection.begin(), collection.end(), [&](const auto &e) { return e.id == element.id; });
      indices.emplace_back(multiplier * (std::distance(collection.begin(), found) + offset));
      if (found == collection.end()) collection.push_back(element);
    };

    this->vertices.reserve(vertices.size());
    this->edges.reserve(edges.size());
    this->vertices.emplace_back(vertices[0]);
    for (uint i = 0; i < edges.size(); i++) {
      const auto &e = edges[i];
      const auto &v = vertices[i + 1];
      UnboundedEdge unbounded_edge{e.id, e.type, e.properties};
      add_element(this->edges, unbounded_edge, e.to == v.id ? 1 : -1, 1);
      add_element(this->vertices, v, 1, 0);
    }
  }

  /** Unique vertices in the path. */
  std::vector<Vertex> vertices;
  /** Unique edges in the path. */
  std::vector<UnboundedEdge> edges;
  /**
   * Indices that map path positions to vertices/edges.
   * Positive indices for left-to-right directionality and negative for
   * right-to-left.
   */
  std::vector<int64_t> indices;
};

/** Value represents supported values in the Bolt protocol. */
class Value {
 public:
  /** Default constructor, makes Null */
  Value() : type_(Type::Null) {}

  /** Types that can be stored in a Value. */
  enum class Type : unsigned { Null, Bool, Int, Double, String, List, Map, Vertex, Edge, UnboundedEdge, Path };

  // constructors for primitive types
  Value(bool value) : type_(Type::Bool) { bool_v = value; }
  Value(int value) : type_(Type::Int) { int_v = value; }
  Value(int64_t value) : type_(Type::Int) { int_v = value; }
  Value(double value) : type_(Type::Double) { double_v = value; }

  // constructors for non-primitive types
  Value(const std::string &value) : type_(Type::String) { new (&string_v) std::string(value); }
  Value(const char *value) : Value(std::string(value)) {}
  Value(const std::vector<Value> &value) : type_(Type::List) { new (&list_v) std::vector<Value>(value); }
  Value(const std::map<std::string, Value> &value) : type_(Type::Map) {
    new (&map_v) std::map<std::string, Value>(value);
  }
  Value(const Vertex &value) : type_(Type::Vertex) { new (&vertex_v) Vertex(value); }
  Value(const Edge &value) : type_(Type::Edge) { new (&edge_v) Edge(value); }
  Value(const UnboundedEdge &value) : type_(Type::UnboundedEdge) { new (&unbounded_edge_v) UnboundedEdge(value); }
  Value(const Path &value) : type_(Type::Path) { new (&path_v) Path(value); }

  // move constructors for non-primitive values
  Value(std::string &&value) noexcept : type_(Type::String) { new (&string_v) std::string(std::move(value)); }
  Value(std::vector<Value> &&value) noexcept : type_(Type::List) { new (&list_v) std::vector<Value>(std::move(value)); }
  Value(std::map<std::string, Value> &&value) noexcept : type_(Type::Map) {
    new (&map_v) std::map<std::string, Value>(std::move(value));
  }
  Value(Vertex &&value) noexcept : type_(Type::Vertex) { new (&vertex_v) Vertex(std::move(value)); }
  Value(Edge &&value) noexcept : type_(Type::Edge) { new (&edge_v) Edge(std::move(value)); }
  Value(UnboundedEdge &&value) noexcept : type_(Type::UnboundedEdge) {
    new (&unbounded_edge_v) UnboundedEdge(std::move(value));
  }
  Value(Path &&value) noexcept : type_(Type::Path) { new (&path_v) Path(std::move(value)); }

  Value &operator=(const Value &other);
  Value &operator=(Value &&other) noexcept;
  Value(const Value &other);
  Value(Value &&other) noexcept;
  ~Value();

  Type type() const { return type_; }

#define DECL_GETTER_BY_VALUE(type, value_type) \
  value_type &Value##type();                   \
  value_type Value##type() const;

  DECL_GETTER_BY_VALUE(Bool, bool)
  DECL_GETTER_BY_VALUE(Int, int64_t)
  DECL_GETTER_BY_VALUE(Double, double)

#undef DECL_GETTER_BY_VALUE

#define DECL_GETTER_BY_REFERENCE(type, value_type) \
  value_type &Value##type();                       \
  const value_type &Value##type() const;

  DECL_GETTER_BY_REFERENCE(String, std::string)
  DECL_GETTER_BY_REFERENCE(List, std::vector<Value>)
  using map_t = std::map<std::string, Value>;
  DECL_GETTER_BY_REFERENCE(Map, map_t)
  DECL_GETTER_BY_REFERENCE(Vertex, Vertex)
  DECL_GETTER_BY_REFERENCE(Edge, Edge)
  DECL_GETTER_BY_REFERENCE(UnboundedEdge, UnboundedEdge)
  DECL_GETTER_BY_REFERENCE(Path, Path)

#undef DECL_GETTER_BY_REFERNCE

#define TYPE_CHECKER(type) \
  bool Is##type() const { return type_ == Type::type; }

  TYPE_CHECKER(Bool)
  TYPE_CHECKER(Int)
  TYPE_CHECKER(Double)
  TYPE_CHECKER(String)
  TYPE_CHECKER(List)
  TYPE_CHECKER(Map)
  TYPE_CHECKER(Vertex)
  TYPE_CHECKER(Edge)
  TYPE_CHECKER(UnboundedEdge)
  TYPE_CHECKER(Path)

#undef TYPE_CHECKER

  friend std::ostream &operator<<(std::ostream &os, const Value &value);

 private:
  Type type_;

  // storage for the value of the property
  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    std::string string_v;
    std::vector<Value> list_v;
    std::map<std::string, Value> map_v;
    Vertex vertex_v;
    Edge edge_v;
    UnboundedEdge unbounded_edge_v;
    Path path_v;
  };
};

/**
 * An exception raised by the Value system.
 */
class ValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  ValueException() : BasicException("Incompatible template param and type!") {}
};

/**
 * Output operators.
 */
std::ostream &operator<<(std::ostream &os, const Vertex &vertex);
std::ostream &operator<<(std::ostream &os, const Edge &edge);
std::ostream &operator<<(std::ostream &os, const UnboundedEdge &edge);
std::ostream &operator<<(std::ostream &os, const Path &path);
std::ostream &operator<<(std::ostream &os, const Value &value);
std::ostream &operator<<(std::ostream &os, const Value::Type type);
}  // namespace communication::bolt
