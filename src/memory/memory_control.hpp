// Copyright 2023 Memgraph Ltd.
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

#include <cstddef>
#include "utils/logging.hpp"
namespace memgraph::memory {

template <typename T, bool ErrOK>
int mallctlHelper(const char *cmd, T *out, T *in) {
  size_t out_len = sizeof(T);
  int err = mallctl(cmd, out, out ? &out_len : nullptr, in, in ? sizeof(T) : 0);
  MG_ASSERT(err != 0 || out_len == sizeof(T));

  return err;
}

template <typename T, bool ErrOK = false>
int mallctlRead(const char *cmd, T *out) {
  return mallctlHelper<T, ErrOK>(cmd, out, static_cast<T *>(nullptr));
}

template <typename T, bool ErrOK = false>
int mallctlWrite(const char *cmd, T in) {
  return mallctlHelper<T, ErrOK>(cmd, static_cast<T *>(nullptr), &in);
}

void PurgeUnusedMemory();
void SetHooks();
void UnSetHooks();
void PrintStats();
int GetArenaForThread();
void TrackMemoryForThread(int arena_ind, size_t size);
void SetGlobalLimit(size_t size);

inline std::atomic<int64_t> allocated_memory{0};
inline std::atomic<int64_t> virtual_allocated_memory{0};
inline size_t global_limit{0};

struct ExtentHooksStats {
  struct Alloc {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct Dalloc {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
    std::atomic<uint64_t> error{0};
  };

  struct Destroy {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct PurgeForced {
    std::atomic<uint64_t> counter{0};
  };

  struct PurgeLazy {
    std::atomic<uint64_t> counter{0};
  };

  struct Merge {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct Split {
    std::atomic<uint64_t> commited{0};
    std::atomic<uint64_t> uncommited{0};
  };

  struct Commit {
    std::atomic<uint64_t> counter{0};
  };

  struct Decommit {
    std::atomic<uint64_t> counter{0};
  };

  Alloc alloc;
  Dalloc dalloc;
  Destroy destroy;
  PurgeForced purge_forced;
  PurgeLazy purge_lazy;
  Merge merge;
  Split split;
  Commit commit;
  Decommit decommit;
};

}  // namespace memgraph::memory
