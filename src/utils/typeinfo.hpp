#pragma once

#include <cstdint>

namespace utils {

/// Type information on a C++ type.
///
/// You should embed this structure as a static constant member `kType` and make
/// sure you generate a unique ID for it. Also, if your type has inheritance,
/// you may want to add a `virtual utils::TypeInfo GetType();` method to get the
/// runtime type.
struct TypeInfo {
  /// Unique ID for the type.
  uint64_t id;
  /// Pretty name of the type.
  const char *name;
};

inline bool operator==(const TypeInfo &a, const TypeInfo &b) {
  return a.id == b.id;
}
inline bool operator!=(const TypeInfo &a, const TypeInfo &b) {
  return a.id != b.id;
}
inline bool operator<(const TypeInfo &a, const TypeInfo &b) {
  return a.id < b.id;
}
inline bool operator<=(const TypeInfo &a, const TypeInfo &b) {
  return a.id <= b.id;
}
inline bool operator>(const TypeInfo &a, const TypeInfo &b) {
  return a.id > b.id;
}
inline bool operator>=(const TypeInfo &a, const TypeInfo &b) {
  return a.id >= b.id;
}

}  // namespace utils
