#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <experimental/optional>
#include <experimental/type_traits>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "communication/rpc/streams.hpp"

// The namespace name stands for SaveLoadKit. It should be not mistaken for the
// Mercedes car model line.
namespace slk {

// Static assert for the assumption made in this library.
static_assert(std::experimental::is_same_v<std::uint8_t, char> ||
                  std::experimental::is_same_v<std::uint8_t, unsigned char>,
              "The slk library requires uint8_t to be implemented as char or "
              "unsigned char.");

// Forward declarations for all recursive `Save` and `Load` functions must be
// here because C++ doesn't know how to resolve the function call if it isn't in
// the global namespace.

template <typename T>
void Save(const std::vector<T> &obj, Builder *builder);
template <typename T>
void Load(std::vector<T> *obj, Reader *reader);

template <typename T>
void Save(const std::set<T> &obj, Builder *builder);
template <typename T>
void Load(std::set<T> *obj, Reader *reader);

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
void Save(const std::experimental::optional<T> &obj, Builder *builder);
template <typename T>
void Load(std::experimental::optional<T> *obj, Reader *reader);

template <typename T>
void Save(const std::shared_ptr<T> &obj, Builder *builder,
          std::vector<T *> *saved);
template <typename T>
void Load(std::shared_ptr<T> *obj, Reader *reader,
          std::vector<std::shared_ptr<T>> *loaded);

// Implementation of serialization for primitive types.

#define MAKE_PRIMITIVE_SAVE(primitive_type)                \
  inline void Save(primitive_type obj, Builder *builder) { \
    builder->Save(reinterpret_cast<const uint8_t *>(&obj), \
                  sizeof(primitive_type));                 \
  }

MAKE_PRIMITIVE_SAVE(bool)
MAKE_PRIMITIVE_SAVE(int8_t)
MAKE_PRIMITIVE_SAVE(uint8_t)
MAKE_PRIMITIVE_SAVE(int16_t)
MAKE_PRIMITIVE_SAVE(uint16_t)
MAKE_PRIMITIVE_SAVE(int32_t)
MAKE_PRIMITIVE_SAVE(uint32_t)
MAKE_PRIMITIVE_SAVE(int64_t)
MAKE_PRIMITIVE_SAVE(uint64_t)
MAKE_PRIMITIVE_SAVE(float)
MAKE_PRIMITIVE_SAVE(double)

#undef MAKE_PRIMITIVE_SAVE

#define MAKE_PRIMITIVE_LOAD(primitive_type)                                 \
  inline void Load(primitive_type *obj, Reader *reader) {                   \
    reader->Load(reinterpret_cast<uint8_t *>(obj), sizeof(primitive_type)); \
  }

MAKE_PRIMITIVE_LOAD(bool)
MAKE_PRIMITIVE_LOAD(int8_t)
MAKE_PRIMITIVE_LOAD(uint8_t)
MAKE_PRIMITIVE_LOAD(int16_t)
MAKE_PRIMITIVE_LOAD(uint16_t)
MAKE_PRIMITIVE_LOAD(int32_t)
MAKE_PRIMITIVE_LOAD(uint32_t)
MAKE_PRIMITIVE_LOAD(int64_t)
MAKE_PRIMITIVE_LOAD(uint64_t)
MAKE_PRIMITIVE_LOAD(float)
MAKE_PRIMITIVE_LOAD(double)

#undef MAKE_PRIMITIVE_LOAD

// Implementation of serialization of complex types.

inline void Save(const std::string &obj, Builder *builder) {
  uint64_t size = obj.size();
  builder->Save(reinterpret_cast<const uint8_t *>(&size), sizeof(uint64_t));
  builder->Save(reinterpret_cast<const uint8_t *>(obj.data()), size);
}

inline void Load(std::string *obj, Reader *reader) {
  const int kMaxStackBuffer = 8192;
  uint64_t size = 0;
  reader->Load(reinterpret_cast<uint8_t *>(&size), sizeof(uint64_t));
  if (size < kMaxStackBuffer) {
    // Here we use a temporary buffer on the stack to prevent temporary
    // allocations. Most of strings that are decoded are small so it makes no
    // sense to allocate a temporary buffer every time we decode a string. This
    // way we allocate a temporary buffer only when the string is large. This
    // wouldn't be necessary if we had full C++17 support. In C++17 we could
    // preallocate the `buff[size]` in the destination string `*obj =
    // std::string('\0', size)` and just call `reader->Load(obj->data())`.
    char buff[kMaxStackBuffer];
    reader->Load(reinterpret_cast<uint8_t *>(buff), size);
    *obj = std::string(buff, size);
  } else {
    auto buff = std::unique_ptr<char[]>(new char[size]);
    reader->Load(reinterpret_cast<uint8_t *>(buff.get()), size);
    *obj = std::string(buff.get(), size);
  }
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

template <typename T>
inline void Save(const std::set<T> &obj, Builder *builder) {
  uint64_t size = obj.size();
  Save(size, builder);
  for (const auto &item : obj) {
    Save(item, builder);
  }
}

template <typename T>
inline void Load(std::set<T> *obj, Reader *reader) {
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
inline void Save(const std::experimental::optional<T> &obj, Builder *builder) {
  if (obj == std::experimental::nullopt) {
    bool exists = false;
    Save(exists, builder);
  } else {
    bool exists = true;
    Save(exists, builder);
    Save(*obj, builder);
  }
}

template <typename T>
inline void Load(std::experimental::optional<T> *obj, Reader *reader) {
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    T item;
    Load(&item, reader);
    obj->emplace(std::move(item));
  } else {
    *obj = std::experimental::nullopt;
  }
}

// Implementation of three argument serialization for complex types.

template <typename T>
inline void Save(const std::shared_ptr<T> &obj, Builder *builder,
                 std::vector<T *> *saved) {
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
      Save(*obj.get(), builder);
      saved->push_back(obj.get());
    }
  }
}

template <typename T>
inline void Load(std::shared_ptr<T> *obj, Reader *reader,
                 std::vector<std::shared_ptr<T>> *loaded) {
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
      // TODO: handle if index doesn't exist!
      *obj = (*loaded)[index];
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
inline void Load(std::vector<T> *obj, Reader *reader,
                 std::function<void(T *, Reader *)> item_load_function) {
  uint64_t size = 0;
  Load(&size, reader);
  obj->resize(size);
  for (uint64_t i = 0; i < size; ++i) {
    item_load_function(&(*obj)[i], reader);
  }
}

template <typename T>
inline void Save(const std::experimental::optional<T> &obj, Builder *builder,
                 std::function<void(const T &, Builder *)> item_save_function) {
  if (obj == std::experimental::nullopt) {
    bool exists = false;
    Save(exists, builder);
  } else {
    bool exists = true;
    Save(exists, builder);
    item_save_function(*obj, builder);
  }
}

template <typename T>
inline void Load(std::experimental::optional<T> *obj, Reader *reader,
                 std::function<void(T *, Reader *)> item_load_function) {
  bool exists = false;
  Load(&exists, reader);
  if (exists) {
    T item;
    item_load_function(&item, reader);
    obj->emplace(std::move(item));
  } else {
    *obj = std::experimental::nullopt;
  }
}
}  // namespace slk
