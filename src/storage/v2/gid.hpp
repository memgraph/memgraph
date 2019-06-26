#pragma once

#include "utils/cast.hpp"

namespace storage {

class Gid final {
 private:
  explicit Gid(uint64_t gid) : gid_(gid) {}

 public:
  static Gid FromUint(uint64_t gid) { return Gid{gid}; }

  static Gid FromInt(int64_t gid) {
    return Gid{utils::MemcpyCast<uint64_t>(gid)};
  }

  uint64_t AsUint() const { return gid_; }

  int64_t AsInt() const { return utils::MemcpyCast<int64_t>(gid_); }

 private:
  uint64_t gid_;
};

inline bool operator==(const Gid &first, const Gid &second) {
  return first.AsUint() == second.AsUint();
}
inline bool operator!=(const Gid &first, const Gid &second) {
  return first.AsUint() != second.AsUint();
}
inline bool operator<(const Gid &first, const Gid &second) {
  return first.AsUint() < second.AsUint();
}
inline bool operator>(const Gid &first, const Gid &second) {
  return first.AsUint() > second.AsUint();
}
inline bool operator<=(const Gid &first, const Gid &second) {
  return first.AsUint() <= second.AsUint();
}
inline bool operator>=(const Gid &first, const Gid &second) {
  return first.AsUint() >= second.AsUint();
}

}  // namespace storage
