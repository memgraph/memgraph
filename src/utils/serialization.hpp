#pragma once

#include <experimental/optional>

#include "boost/serialization/split_free.hpp"
#include "query/typed_value.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "utils/exceptions.hpp"

namespace boost::serialization {

namespace {

template <size_t idx, class TArchive, class... Elements>
void tuple_serialization_helper(TArchive &ar, std::tuple<Elements...> &tup) {
  if constexpr (idx < sizeof...(Elements)) {
    ar &std::get<idx>(tup);
    tuple_serialization_helper<idx + 1, TArchive, Elements...>(ar, tup);
  }
}

}  // namespace

template <class TArchive, class... Elements>
inline void serialize(TArchive &ar, std::tuple<Elements...> &tup,
                      unsigned int) {
  tuple_serialization_helper<0, TArchive, Elements...>(ar, tup);
}

template <class TArchive, class T>
inline void serialize(TArchive &ar, std::experimental::optional<T> &opt,
                      unsigned int version) {
  split_free(ar, opt, version);
}

template <class TArchive, class T>
void save(TArchive &ar, const std::experimental::optional<T> &opt,
          unsigned int) {
  ar << static_cast<bool>(opt);
  if (opt) {
    ar << *opt;
  }
}

template <class TArchive, class T>
void load(TArchive &ar, std::experimental::optional<T> &opt, unsigned int) {
  bool has_value;
  ar >> has_value;
  if (has_value) {
    T tmp;
    ar >> tmp;
    opt = std::move(tmp);
  } else {
    opt = std::experimental::nullopt;
  }
}

}  // namespace boost::serialization

namespace utils {

/**
 * Saves the given value into the given Boost archive. The optional
 * `save_graph_element` function is called if the given `value` is a
 * [Vertex|Edge|Path]. If that function is not provided, and `value` is one of
 * those, an exception is thrown.
 */
template <class TArchive>
void SaveTypedValue(
    TArchive &ar, const query::TypedValue &value,
    std::function<void(TArchive &ar, const query::TypedValue &value)>
        save_graph_element = nullptr) {
  ar << value.type();
  switch (value.type()) {
    case query::TypedValue::Type::Null:
      return;
    case query::TypedValue::Type::Bool:
      ar << value.Value<bool>();
      return;
    case query::TypedValue::Type::Int:
      ar << value.Value<int64_t>();
      return;
    case query::TypedValue::Type::Double:
      ar << value.Value<double>();
      return;
    case query::TypedValue::Type::String:
      ar << value.Value<std::string>();
      return;
    case query::TypedValue::Type::List: {
      const auto &values = value.Value<std::vector<query::TypedValue>>();
      ar << values.size();
      for (const auto &v : values) {
        SaveTypedValue(ar, v, save_graph_element);
      }
      return;
    }
    case query::TypedValue::Type::Map: {
      const auto &map = value.Value<std::map<std::string, query::TypedValue>>();
      ar << map.size();
      for (const auto &key_value : map) {
        ar << key_value.first;
        SaveTypedValue(ar, key_value.second, save_graph_element);
      }
      return;
    }
    case query::TypedValue::Type::Vertex:
    case query::TypedValue::Type::Edge:
    case query::TypedValue::Type::Path:
      if (save_graph_element) {
        save_graph_element(ar, value);
      } else {
        throw utils::BasicException("Unable to archive TypedValue of type: {}",
                                    value.type());
      }
  }
}

/** Loads a typed value into the given reference from the given archive. The
 * optional `load_graph_element` function is called if a [Vertex|Edge|Path]
 * TypedValue should be unarchived. If that function is not provided, and
 * `value` is one of those, an exception is thrown.
 */
template <class TArchive>
void LoadTypedValue(TArchive &ar, query::TypedValue &value,
                    std::function<void(TArchive &ar, query::TypedValue::Type,
                                       query::TypedValue &)>
                        load_graph_element = nullptr) {
  query::TypedValue::Type type = query::TypedValue::Type::Null;
  ar >> type;
  switch (type) {
    case query::TypedValue::Type::Null:
      return;
    case query::TypedValue::Type::Bool: {
      bool v;
      ar >> v;
      value = v;
      return;
    }
    case query::TypedValue::Type::Int: {
      int64_t v;
      ar >> v;
      value = v;
      return;
    }
    case query::TypedValue::Type::Double: {
      double v;
      ar >> v;
      value = v;
      return;
    }
    case query::TypedValue::Type::String: {
      std::string v;
      ar >> v;
      value = v;
      return;
    }
    case query::TypedValue::Type::List: {
      value = std::vector<query::TypedValue>{};
      auto &list = value.ValueList();
      size_t size;
      ar >> size;
      list.reserve(size);
      for (size_t i = 0; i < size; ++i) {
        list.emplace_back();
        LoadTypedValue(ar, list.back(), load_graph_element);
      }
      return;
    }
    case query::TypedValue::Type::Map: {
      value = std::map<std::string, query::TypedValue>{};
      auto &map = value.ValueMap();
      size_t size;
      ar >> size;
      for (size_t i = 0; i < size; ++i) {
        std::string key;
        ar >> key;
        LoadTypedValue(ar, map[key], load_graph_element);
      }
      return;
    }
    case query::TypedValue::Type::Vertex:
    case query::TypedValue::Type::Edge:
    case query::TypedValue::Type::Path:
      if (load_graph_element) {
        load_graph_element(ar, type, value);
      } else {
        throw utils::BasicException(
            "Unexpected TypedValue type '{}' when loading from archive", type);
      }
  }
}
}  // namespace utils
