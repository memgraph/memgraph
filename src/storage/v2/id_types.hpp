#pragma once

#include <type_traits>

#include "utils/cast.hpp"

namespace storage {

#define STORAGE_DEFINE_ID_TYPE(name)                                  \
  class name final {                                                  \
   private:                                                           \
    explicit name(uint64_t id) : id_(id) {}                           \
                                                                      \
   public:                                                            \
    static name FromUint(uint64_t id) { return name{id}; }            \
    static name FromInt(int64_t id) {                                 \
      return name{utils::MemcpyCast<uint64_t>(id)};                   \
    }                                                                 \
    uint64_t AsUint() const { return id_; }                           \
    int64_t AsInt() const { return utils::MemcpyCast<int64_t>(id_); } \
                                                                      \
   private:                                                           \
    uint64_t id_;                                                     \
  };                                                                  \
  static_assert(std::is_trivially_copyable<name>::value,              \
                "storage::" #name " must be trivially copyable!");    \
  inline bool operator==(const name &first, const name &second) {     \
    return first.AsUint() == second.AsUint();                         \
  }                                                                   \
  inline bool operator!=(const name &first, const name &second) {     \
    return first.AsUint() != second.AsUint();                         \
  }                                                                   \
  inline bool operator<(const name &first, const name &second) {      \
    return first.AsUint() < second.AsUint();                          \
  }                                                                   \
  inline bool operator>(const name &first, const name &second) {      \
    return first.AsUint() > second.AsUint();                          \
  }                                                                   \
  inline bool operator<=(const name &first, const name &second) {     \
    return first.AsUint() <= second.AsUint();                         \
  }                                                                   \
  inline bool operator>=(const name &first, const name &second) {     \
    return first.AsUint() >= second.AsUint();                         \
  }

STORAGE_DEFINE_ID_TYPE(Gid);
STORAGE_DEFINE_ID_TYPE(LabelId);
STORAGE_DEFINE_ID_TYPE(PropertyId);
STORAGE_DEFINE_ID_TYPE(EdgeTypeId);

#undef STORAGE_DEFINE_ID_TYPE

}  // namespace storage
