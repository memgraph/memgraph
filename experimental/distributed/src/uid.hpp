#pragma once

#include "utils/hashing/fnv.hpp"

#include <cstdint>
#include <vector>

/**
 * Globally unique id (in the entire distributed system) of a vertex.
 *
 * It is identified by a pair of a (original memgraph node, local vertex id)
 */
class UniqueVid {
 public:
  UniqueVid(int64_t orig_mnid, int64_t vid)
    : orig_mnid_(orig_mnid), vid_(vid) {}
  /** Original Memgraph node the vertex was created **/
  int64_t orig_mnid_;

  /** Original vertex id it was assigned on creation. **/
  int64_t vid_;

  bool operator==(const UniqueVid &other) const {
    return orig_mnid_ == other.orig_mnid_ &&
           vid_ == other.vid_;
  }

  bool operator!=(const UniqueVid &other) const { return !(*this == other); }
};

/**
 * Specifies where a vertex is in the distributed system.
 */
class GlobalVertAddress {
 public:
  GlobalVertAddress(int64_t cur_mnid, const UniqueVid &uvid)
      : cur_mnid_(cur_mnid), uvid_(uvid) {}

  /** The current Memgraph node where the vertex is **/
  int64_t cur_mnid_;
  UniqueVid uvid_;

  bool operator==(const GlobalVertAddress &other) const {
    return cur_mnid_ == other.cur_mnid_ && uvid_ == other.uvid_;
  }

  bool operator!=(const GlobalVertAddress &other) const {
    return !(*this == other);
  }
};

namespace std {
template <>
struct hash<UniqueVid> {
  size_t operator()(const UniqueVid &uid) const {
    return HashCombine<decltype(uid.orig_mnid_), decltype(uid.vid_)>()(uid.orig_mnid_, uid.vid_);
  }
};

template <>
struct hash<GlobalVertAddress> {
  size_t operator()(const GlobalVertAddress &ga) const {
    return HashCombine<decltype(ga.cur_mnid_), decltype(ga.uvid_)>()(ga.cur_mnid_, ga.uvid_);
  }
};
}
