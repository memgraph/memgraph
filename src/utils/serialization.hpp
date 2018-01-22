#pragma once

#include <experimental/optional>

#include "boost/serialization/split_free.hpp"
#include "query/typed_value.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "utils/exceptions.hpp"

namespace boost::serialization {

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

}  // boost::serialization

namespace utils {

/** Saves the given value into the given Boost archive. */
template <class TArchive>
void SaveTypedValue(TArchive &ar, const query::TypedValue &value) {
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
        SaveTypedValue(ar, v);
      }
      return;
    }
    case query::TypedValue::Type::Map: {
      const auto &map = value.Value<std::map<std::string, query::TypedValue>>();
      ar << map.size();
      for (const auto &key_value : map) {
        ar << key_value.first;
        SaveTypedValue(ar, key_value.second);
      }
      return;
    }
    case query::TypedValue::Type::Vertex:
    case query::TypedValue::Type::Edge:
    case query::TypedValue::Type::Path:
      throw utils::BasicException("Unable to archive TypedValue of type: {}",
                                  value.type());
  }
}

/** Loads a typed value into the given reference from the given archive. */
template <class TArchive>
void LoadTypedValue(TArchive &ar, query::TypedValue &value) {
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
      std::vector<query::TypedValue> values;
      size_t size;
      ar >> size;
      values.reserve(size);
      for (size_t i = 0; i < size; ++i) {
        query::TypedValue tv;
        LoadTypedValue(ar, tv);
        values.emplace_back(tv);
      }
      value = values;
      return;
    }
    case query::TypedValue::Type::Map: {
      std::map<std::string, query::TypedValue> map;
      size_t size;
      ar >> size;
      for (size_t i = 0; i < size; ++i) {
        std::string key;
        ar >> key;
        query::TypedValue v;
        LoadTypedValue(ar, v);
        map.emplace(key, v);
      }
      value = map;
      return;
    }
    case query::TypedValue::Type::Vertex:
    case query::TypedValue::Type::Edge:
    case query::TypedValue::Type::Path:
      throw utils::BasicException(
          "Unexpected TypedValue type '{}' when loading from archive", type);
  }
}
}  // namespace utils
