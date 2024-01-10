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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "slk/streams.hpp"
#include "utils/cast.hpp"
#include "utils/concepts.hpp"
#include "utils/endian.hpp"
#include "utils/exceptions.hpp"
#include "utils/typeinfo.hpp"

// The namespace name stands for SaveLoadKit. It should be not mistaken for the
// Mercedes car model line.
namespace memgraph::slk {

// Static assert for the assumption made in this library.
static_assert(std::is_same_v<std::uint8_t, char> || std::is_same_v<std::uint8_t, unsigned char>,
              "The slk library requires uint8_t to be implemented as char or "
              "unsigned char.");

/// Exception that will be thrown if an object can't be decoded from the byte
/// stream.
class SlkDecodeException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(SlkDecodeException)
};

// Forward declarations for all recursive `Save` and `Load` functions must be
// here because C++ doesn't know how to resolve the function call if it isn't in
// the global namespace.

template <typename T>
void Save(const std::vector<T> &obj, Builder *builder);
template <typename T>
void Load(std::vector<T> *obj, Reader *reader);

template <typename T, typename Cmp>
void Save(const std::set<T, Cmp> &obj, Builder *builder);
template <typename T, typename Cmp>
void Load(std::set<T, Cmp> *obj, Reader *reader);

template <typename K, typename V>
void Save(const std::map<K, V> &obj, Builder *builder);
template <typename K, typename V>
void Load(std::map<K, V> *obj, Reader *reader);

template <typename K, typename V>
void Save(const std::unordered_map<K, V> &obj, Builder *builder);
template <typename K, typename V>
void Load(std::unordered_map<K, V> *obj, Reader *reader);

template <typename T>
void Save(const std::unique_ptr<T> &obj, Builder *builder);
template <typename T>
void Load(std::unique_ptr<T> *obj, Reader *reader);
template <typename T>
void Load(std::unique_ptr<T> *obj, Reader *reader, const std::function<void(std::unique_ptr<T> *, Reader *)> &load);

template <typename T>
void Save(const std::optional<T> &obj, Builder *builder);
template <typename T>
void Load(std::optional<T> *obj, Reader *reader);

template <typename T>
void Save(const std::shared_ptr<T> &obj, Builder *builder, std::vector<T *> *saved);
template <typename T>
void Save(const std::shared_ptr<T> &obj, Builder *builder, std::vector<T *> *saved,
          const std::function<void(const T &, Builder *builder)> &save);
template <typename T>
void Load(std::shared_ptr<T> *obj, Reader *reader, std::vector<std::shared_ptr<T>> *loaded);
template <typename T>
void Load(std::shared_ptr<T> *obj, Reader *reader, std::vector<std::shared_ptr<T>> *loaded,
          const std::function<void(std::unique_ptr<T> *, Reader *reader)> &load);

// Implementation of serialization for primitive types.

#define MAKE_PRIMITIVE_SAVE(primitive_type)                                                 \
  inline void Save(primitive_type obj, Builder *builder) {                                  \
    primitive_type obj_encoded = utils::HostToLittleEndian(obj);                            \
    builder->Save(reinterpret_cast<const uint8_t *>(&obj_encoded), sizeof(primitive_type)); \
  }

MAKE_PRIMITIVE_SAVE(bool)
MAKE_PRIMITIVE_SAVE(char)
MAKE_PRIMITIVE_SAVE(int8_t)
MAKE_PRIMITIVE_SAVE(uint8_t)
MAKE_PRIMITIVE_SAVE(int16_t)
MAKE_PRIMITIVE_SAVE(uint16_t)
MAKE_PRIMITIVE_SAVE(int32_t)
MAKE_PRIMITIVE_SAVE(uint32_t)
MAKE_PRIMITIVE_SAVE(int64_t)
MAKE_PRIMITIVE_SAVE(uint64_t)

#undef MAKE_PRIMITIVE_SAVE

#define MAKE_PRIMITIVE_LOAD(primitive_type)                                          \
  inline void Load(primitive_type *obj, Reader *reader) {                            \
    primitive_type obj_encoded;                                                      \
    reader->Load(reinterpret_cast<uint8_t *>(&obj_encoded), sizeof(primitive_type)); \
    *obj = utils::LittleEndianToHost(obj_encoded);                                   \
  }

MAKE_PRIMITIVE_LOAD(bool)
MAKE_PRIMITIVE_LOAD(char)
MAKE_PRIMITIVE_LOAD(int8_t)
MAKE_PRIMITIVE_LOAD(uint8_t)
MAKE_PRIMITIVE_LOAD(int16_t)
MAKE_PRIMITIVE_LOAD(uint16_t)
MAKE_PRIMITIVE_LOAD(int32_t)
MAKE_PRIMITIVE_LOAD(uint32_t)
MAKE_PRIMITIVE_LOAD(int64_t)
MAKE_PRIMITIVE_LOAD(uint64_t)

#undef MAKE_PRIMITIVE_LOAD

inline void Save(float obj, Builder *builder) { slk::Save(utils::MemcpyCast<uint32_t>(obj), builder); }

inline void Save(double obj, Builder *builder) { slk::Save(utils::MemcpyCast<uint64_t>(obj), builder); }

inline void Load(float *obj, Reader *reader) {
  uint32_t obj_encoded;
  slk::Load(&obj_encoded, reader);
  *obj = utils::MemcpyCast<float>(obj_encoded);
}

inline void Load(double *obj, Reader *reader) {
  uint64_t obj_encoded;
  slk::Load(&obj_encoded, reader);
  *obj = utils::MemcpyCast<double>(obj_encoded);
}

// Implementation of serialization of complex types.

inline void Save(const std::string &obj, Builder *builder) {
  uint64_t size = obj.size();
  Save(size, builder);
  builder->Save(reinterpret_cast<const uint8_t *>(obj.data()), size);
}

inline void Save(const char *obj, Builder *builder) {
  uint64_t size = strlen(obj);
  Save(size, builder);
  builder->Save(reinterpret_cast<const uint8_t *>(obj), size);
}

inline void Save(const std::string_view obj, Builder *builder) {
  uint64_t size = obj.size();
  Save(size, builder);
  builder->Save(reinterpret_cast<const uint8_t *>(obj.data()), size);
}

inline void Load(std::string *obj, Reader *reader) {
  uint64_t size = 0;
  Load(&size, reader);
  *obj = std::string(size, '\0');
  reader->Load(reinterpret_cast<uint8_t *>(obj->data()), size);
}

template <typename T>
inline void Save(const std::vector<T> &obj, Builder *builder) {
  uint64_t size = obj.size();
  Save(size, builder);
  for (const auto &item : obj) {
    Save(item, builder);
  }
}

template <typename T>
inline void Load(std::vector<T> *obj, Reader *reader) {
  uint64_t size = 0;
  Load(&size, reader);
  obj->resize(size);
  for (uint64_t i = 0; i < size; ++i) {
    Load(&(*obj)[i], reader);
  }
}

template <typename T, typename Cmp>
inline void Save(const std::set<T, Cmp> &obj, Builder *builder) {
  uint64_t size = obj.size();
  Save(size, builder);
  for (const auto &item : obj) {
    Save(item, builder);
  }
}

template <typename T, typename Cmp>
inline void Load(std::set<T, Cmp> *obj, Reader *reader) {
  uint64_t size = 0;
  Load(&size, reader);
  for (uint64_t i = 0; i < size; ++i) {
    T item;
    Load(&item, reader);
    obj->emplace(std::move(item));
  }
}

#define MAKE_MAP_SAVE(map_type)                                   \
  template <typename K, typename V>                               \
  inline void Save(const map_type<K, V> &obj, Builder *builder) { \
    uint64_t size = obj.size();                                   \
    Save(size, builder);                                          \
    for (const auto &item : obj) {                                \
      Save(item.first, builder);                                  \
      Save(item.second, builder);                                 \
    }                                                             \
  }

MAKE_MAP_SAVE(std::map)
MAKE_MAP_SAVE(std::unordered_map)

#undef MAKE_MAP_SAVE

#define MAKE_MAP_LOAD(map_type)                           \
  template <typename K, typename V>                       \
  inline void Load(map_type<K, V> *obj, Reader *reader) { \
    uint64_t size = 0;                                    \
    Load(&size, reader);                                  \
    for (uint64_t i = 0; i < size; ++i) {                 \
      K key;                                              \
      V value;                                            \
      Load(&key, reader);                                 \
      Load(&value, reader);                               \
      obj->emplace(std::move(key), std::move(value));     \
    }                                                     \
  }

MAKE_MAP_LOAD(std::map)
MAKE_MAP_LOAD(std::unordered_map)

#undef MAKE_MAP_LOAD

template <typename T>
inline void Save(const std::unique_ptr<T> &obj, Builder *builder) {
  if (obj.get() == nullptr) {
    bool exists = false;
    Save(exists, builder);
  } else {
    bool exists = true;
    Save(exists, builder);
    Save(*obj.get(), builder);
  }
}

template <typename T>
inline void Load(std::unique_ptr<T> *obj, Reader *reader) {
  // Prevent any loading which may potentially break class hierarchies.
  // Unfortunately, C++14 doesn't have (or I'm not aware of it) a trait for
  // checking whether some type has any derived or base classes.
  static_assert(!std::is_polymorphic_v<T>,
                "Only non polymorphic types can be loaded generically from a "
                "pointer. Pass a custom load function as the 3rd argument.");
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    T item;
    Load(&item, reader);
    *obj = std::make_unique<T>(std::move(item));
  } else {
    *obj = nullptr;
  }
}

template <typename T>
inline void Load(std::unique_ptr<T> *obj, Reader *reader,
                 const std::function<void(std::unique_ptr<T> *, Reader *)> &load) {
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    load(obj, reader);
  } else {
    *obj = nullptr;
  }
}

template <typename T>
inline void Save(const std::optional<T> &obj, Builder *builder) {
  if (obj == std::nullopt) {
    bool exists = false;
    Save(exists, builder);
  } else {
    bool exists = true;
    Save(exists, builder);
    Save(*obj, builder);
  }
}

inline void Save(const utils::TypeId &obj, Builder *builder) {
  Save(static_cast<std::underlying_type_t<utils::TypeId>>(obj), builder);
}

template <typename T>
inline void Load(std::optional<T> *obj, Reader *reader) {
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    T item;
    Load(&item, reader);
    obj->emplace(std::move(item));
  } else {
    *obj = std::nullopt;
  }
}

template <typename A, typename B>
inline void Save(const std::pair<A, B> &obj, Builder *builder) {
  Save(obj.first, builder);
  Save(obj.second, builder);
}

template <typename A, typename B>
inline void Load(std::pair<A, B> *obj, Reader *reader) {
  A first;
  B second;
  Load(&first, reader);
  Load(&second, reader);
  *obj = std::pair<A, B>(std::move(first), std::move(second));
}

// Implementation of three argument serialization for complex types.

template <typename T>
inline void Save(const std::shared_ptr<T> &obj, Builder *builder, std::vector<T *> *saved) {
  Save<T>(obj, builder, saved, [](const auto &elem, auto *builder) { Save(elem, builder); });
}

template <typename T>
inline void Save(const std::shared_ptr<T> &obj, Builder *builder, std::vector<T *> *saved,
                 const std::function<void(const T &, Builder *builder)> &save) {
  if (obj.get() == nullptr) {
    bool exists = false;
    Save(exists, builder);
  } else {
    bool exists = true;
    Save(exists, builder);
    auto pos = std::find(saved->begin(), saved->end(), obj.get());
    if (pos != saved->end()) {
      bool in_place = false;
      Save(in_place, builder);
      uint64_t index = pos - saved->begin();
      Save(index, builder);
    } else {
      bool in_place = true;
      Save(in_place, builder);
      save(*obj, builder);
      saved->push_back(obj.get());
    }
  }
}

template <typename T>
inline void Load(std::shared_ptr<T> *obj, Reader *reader, std::vector<std::shared_ptr<T>> *loaded) {
  // Prevent any loading which may potentially break class hierarchies.
  // Unfortunately, C++14 doesn't have (or I'm not aware of it) a trait for
  // checking whether some type has any derived or base classes.
  static_assert(!std::is_polymorphic_v<T>,
                "Only non polymorphic types can be loaded generically from a "
                "pointer. Pass a custom load function as the 4th argument.");
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    bool in_place = false;
    Load(&in_place, reader);
    if (in_place) {
      T item;
      Load(&item, reader);
      *obj = std::make_shared<T>(std::move(item));
      loaded->push_back(*obj);
    } else {
      uint64_t index = 0;
      Load(&index, reader);
      if (index < loaded->size()) {
        *obj = (*loaded)[index];
      } else {
        throw SlkDecodeException("Couldn't load shared pointer!");
      }
    }
  } else {
    *obj = nullptr;
  }
}

template <typename T>
inline void Load(std::shared_ptr<T> *obj, Reader *reader, std::vector<std::shared_ptr<T>> *loaded,
                 const std::function<void(std::unique_ptr<T> *, Reader *reader)> &load) {
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    bool in_place = false;
    Load(&in_place, reader);
    if (in_place) {
      std::unique_ptr<T> item;
      load(&item, reader);
      *obj = std::move(item);
      loaded->push_back(*obj);
    } else {
      uint64_t index = 0;
      Load(&index, reader);
      if (index < loaded->size()) {
        *obj = (*loaded)[index];
      } else {
        throw SlkDecodeException("Couldn't load shared pointer!");
      }
    }
  } else {
    *obj = nullptr;
  }
}

template <typename T>
inline void Save(const std::vector<T> &obj, Builder *builder,
                 std::function<void(const T &, Builder *)> item_save_function) {
  uint64_t size = obj.size();
  Save(size, builder);
  for (const auto &item : obj) {
    item_save_function(item, builder);
  }
}

template <typename T>
inline void Load(std::vector<T> *obj, Reader *reader, std::function<void(T *, Reader *)> item_load_function) {
  uint64_t size = 0;
  Load(&size, reader);
  obj->resize(size);
  for (uint64_t i = 0; i < size; ++i) {
    item_load_function(&(*obj)[i], reader);
  }
}

template <typename T>
inline void Save(const std::optional<T> &obj, Builder *builder,
                 std::function<void(const T &, Builder *)> item_save_function) {
  if (obj == std::nullopt) {
    bool exists = false;
    Save(exists, builder);
  } else {
    bool exists = true;
    Save(exists, builder);
    item_save_function(*obj, builder);
  }
}

template <typename T>
inline void Load(std::optional<T> *obj, Reader *reader, std::function<void(T *, Reader *)> item_load_function) {
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    T item;
    item_load_function(&item, reader);
    obj->emplace(std::move(item));
  } else {
    *obj = std::nullopt;
  }
}

inline void Load(utils::TypeId *obj, Reader *reader) {
  using enum_type = std::underlying_type_t<utils::TypeId>;
  enum_type obj_encoded;
  slk::Load(&obj_encoded, reader);
  *obj = utils::TypeId(utils::MemcpyCast<enum_type>(obj_encoded));
}

}  // namespace memgraph::slk
