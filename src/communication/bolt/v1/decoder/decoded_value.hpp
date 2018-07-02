#pragma once

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "utils/cast.hpp"
#include "utils/exceptions.hpp"

namespace communication::bolt {

/** Forward declaration of DecodedValue class. */
class DecodedValue;

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

inline bool operator==(const Id &id1, const Id &id2) {
  return id1.AsInt() == id2.AsInt();
}

inline bool operator!=(const Id &id1, const Id &id2) { return !(id1 == id2); }

/**
 * Structure used when reading a Vertex with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedVertex {
  Id id;
  std::vector<std::string> labels;
  std::map<std::string, DecodedValue> properties;
};

/**
 * Structure used when reading an Edge with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedEdge {
  Id id;
  Id from;
  Id to;
  std::string type;
  std::map<std::string, DecodedValue> properties;
};

/**
 * Structure used when reading an UnboundEdge with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedUnboundedEdge {
  Id id;
  std::string type;
  std::map<std::string, DecodedValue> properties;
};

/**
 * Structure used when reading a Path with the decoder.
 * The decoder writes data into this structure.
 */
struct DecodedPath {
  DecodedPath() {}

  DecodedPath(const std::vector<DecodedVertex> &vertices,
              const std::vector<DecodedEdge> &edges) {
    // Helper function. Looks for the given element in the collection. If found,
    // puts its index into `indices`. Otherwise emplaces the given element
    // into the collection and puts that index into `indices`. A multiplier is
    // added to switch between positive and negative indices (that define edge
    // direction).
    auto add_element = [this](auto &collection, const auto &element,
                              int multiplier, int offset) {
      auto found =
          std::find_if(collection.begin(), collection.end(),
                       [&](const auto &e) { return e.id == element.id; });
      indices.emplace_back(multiplier *
                           (std::distance(collection.begin(), found) + offset));
      if (found == collection.end()) collection.push_back(element);
    };

    this->vertices.reserve(vertices.size());
    this->edges.reserve(edges.size());
    this->vertices.emplace_back(vertices[0]);
    for (uint i = 0; i < edges.size(); i++) {
      const auto &e = edges[i];
      const auto &v = vertices[i + 1];
      DecodedUnboundedEdge unbounded_edge{e.id, e.type, e.properties};
      add_element(this->edges, unbounded_edge, e.to == v.id ? 1 : -1, 1);
      add_element(this->vertices, v, 1, 0);
    }
  }

  /** Unique vertices in the path. */
  std::vector<DecodedVertex> vertices;
  /** Unique edges in the path. */
  std::vector<DecodedUnboundedEdge> edges;
  /**
   * Indices that map path positions to vertices/edges.
   * Positive indices for left-to-right directionality and negative for
   * right-to-left.
   */
  std::vector<int64_t> indices;
};

/** DecodedValue represents supported values in the Bolt protocol. */
class DecodedValue {
 public:
  /** Default constructor, makes Null */
  DecodedValue() : type_(Type::Null) {}

  /** Types that can be stored in a DecodedValue. */
  enum class Type : unsigned {
    Null,
    Bool,
    Int,
    Double,
    String,
    List,
    Map,
    Vertex,
    Edge,
    UnboundedEdge,
    Path
  };

  // constructors for primitive types
  DecodedValue(bool value) : type_(Type::Bool) { bool_v = value; }
  DecodedValue(int value) : type_(Type::Int) { int_v = value; }
  DecodedValue(int64_t value) : type_(Type::Int) { int_v = value; }
  DecodedValue(double value) : type_(Type::Double) { double_v = value; }

  // constructors for non-primitive types
  DecodedValue(const std::string &value) : type_(Type::String) {
    new (&string_v) std::string(value);
  }
  DecodedValue(const std::vector<DecodedValue> &value) : type_(Type::List) {
    new (&list_v) std::vector<DecodedValue>(value);
  }
  DecodedValue(const std::map<std::string, DecodedValue> &value)
      : type_(Type::Map) {
    new (&map_v) std::map<std::string, DecodedValue>(value);
  }
  DecodedValue(const DecodedVertex &value) : type_(Type::Vertex) {
    new (&vertex_v) DecodedVertex(value);
  }
  DecodedValue(const DecodedEdge &value) : type_(Type::Edge) {
    new (&edge_v) DecodedEdge(value);
  }
  DecodedValue(const DecodedUnboundedEdge &value) : type_(Type::UnboundedEdge) {
    new (&unbounded_edge_v) DecodedUnboundedEdge(value);
  }
  DecodedValue(const DecodedPath &value) : type_(Type::Path) {
    new (&path_v) DecodedPath(value);
  }

  DecodedValue &operator=(const DecodedValue &other);
  DecodedValue(const DecodedValue &other);
  ~DecodedValue();

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
  DECL_GETTER_BY_REFERENCE(List, std::vector<DecodedValue>)
  using map_t = std::map<std::string, DecodedValue>;
  DECL_GETTER_BY_REFERENCE(Map, map_t)
  DECL_GETTER_BY_REFERENCE(Vertex, DecodedVertex)
  DECL_GETTER_BY_REFERENCE(Edge, DecodedEdge)
  DECL_GETTER_BY_REFERENCE(UnboundedEdge, DecodedUnboundedEdge)
  DECL_GETTER_BY_REFERENCE(Path, DecodedPath)

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

  friend std::ostream &operator<<(std::ostream &os, const DecodedValue &value);

 private:
  Type type_;

  // storage for the value of the property
  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    std::string string_v;
    std::vector<DecodedValue> list_v;
    std::map<std::string, DecodedValue> map_v;
    DecodedVertex vertex_v;
    DecodedEdge edge_v;
    DecodedUnboundedEdge unbounded_edge_v;
    DecodedPath path_v;
  };
};

/**
 * An exception raised by the DecodedValue system.
 */
class DecodedValueException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  DecodedValueException()
      : BasicException("Incompatible template param and type!") {}
};

/**
 * Output operators.
 */
std::ostream &operator<<(std::ostream &os, const DecodedVertex &vertex);
std::ostream &operator<<(std::ostream &os, const DecodedEdge &edge);
std::ostream &operator<<(std::ostream &os, const DecodedUnboundedEdge &edge);
std::ostream &operator<<(std::ostream &os, const DecodedPath &path);
std::ostream &operator<<(std::ostream &os, const DecodedValue &value);
std::ostream &operator<<(std::ostream &os, const DecodedValue::Type type);
}  // namespace communication::bolt
