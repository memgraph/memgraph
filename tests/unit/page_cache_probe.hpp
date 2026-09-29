// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// Observing the page cache from a test.
///
/// Dropping pages is advisory, so a file's contents say nothing about whether it worked: what the
/// cache holds afterwards is the only sign of a release that happened rather than one the kernel
/// ignored. Every assertion built on it has to be gated on the filesystem being one where it means
/// anything, and on the machine not having taken the pages for its own reasons.
#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <vector>

#include "utils/file.hpp"
#include "utils/on_scope_exit.hpp"

namespace memgraph::test {

/// What the page cache holds for a file, and how much of it the kernel has taken back.
///
/// `reclaimed` is what separates a release from memory pressure. Reclaiming a page records that it
/// was there, so the count survives the page; invalidating one, which is what a release does, takes
/// it away leaving nothing behind. A file whose pages are gone with none reclaimed was released.
struct CacheState {
  uint64_t pages;      ///< pages the file occupies
  uint64_t cached;     ///< of those, how many the cache holds
  uint64_t reclaimed;  ///< pages the kernel took back under memory pressure
};

namespace detail {
// The kernel's interface to cachestat(2), declared here because the toolchain's headers predate it.
// The number is the one asm-generic assigns, which both architectures built here take; a kernel
// without the call answers ENOSYS and CacheStateOf reports that it could not tell.
#if defined(__NR_cachestat)
inline constexpr long kCachestat = __NR_cachestat;
#elif defined(__x86_64__) || defined(__aarch64__)
inline constexpr long kCachestat = 451;
#else
inline constexpr long kCachestat = -1;
#endif

struct CachestatRange {
  uint64_t off;
  uint64_t len;
};

struct Cachestat {
  uint64_t nr_cache;
  uint64_t nr_dirty;
  uint64_t nr_writeback;
  uint64_t nr_evicted;
  uint64_t nr_recently_evicted;
};
}  // namespace detail

/// `path`'s state in the page cache, via cachestat(2). std::nullopt when the file is empty, or when
/// this kernel cannot answer, in which case a release and a reclaim cannot be told apart.
inline std::optional<CacheState> CacheStateOf(const std::filesystem::path &path) {
  if constexpr (detail::kCachestat < 0) return std::nullopt;

  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd == -1) return std::nullopt;
  const auto close_fd = utils::OnScopeExit{[fd] { ::close(fd); }};

  struct stat st{};
  if (::fstat(fd, &st) == -1 || st.st_size == 0) return std::nullopt;
  const auto size = static_cast<uint64_t>(st.st_size);

  detail::CachestatRange const range{.off = 0, .len = size};
  detail::Cachestat stats{};
  if (::syscall(detail::kCachestat, fd, &range, &stats, 0U) != 0) return std::nullopt;

  const auto page = static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
  return CacheState{.pages = (size + page - 1) / page, .cached = stats.nr_cache, .reclaimed = stats.nr_evicted};
}

/// Residency of `path` in the page cache, as a fraction of its pages, via mincore(2).
/// std::nullopt when the file is empty or the mapping fails.
inline std::optional<double> ResidentFraction(const std::filesystem::path &path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd == -1) return std::nullopt;
  const auto close_fd = utils::OnScopeExit{[fd] { ::close(fd); }};

  struct stat st{};
  if (::fstat(fd, &st) == -1 || st.st_size == 0) return std::nullopt;
  const auto size = static_cast<size_t>(st.st_size);

  void *addr = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) return std::nullopt;
  const auto unmap = utils::OnScopeExit{[addr, size] { ::munmap(addr, size); }};

  const auto page = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
  const auto pages = (size + page - 1) / page;
  std::vector<unsigned char> resident(pages, 0);
  if (::mincore(addr, size, resident.data()) == -1) return std::nullopt;

  // The low bit is the "in core" flag; the rest are unspecified.
  const auto in_core = std::ranges::count_if(resident, [](unsigned char v) { return (v & 1U) != 0; });
  return static_cast<double>(in_core) / static_cast<double>(pages);
}

/// Whether this filesystem honours POSIX_FADV_DONTNEED at all. tmpfs does not: every page reads as
/// resident forever, so a residency assertion there is a statement about the filesystem rather than
/// about the code. Probed with a throwaway file, because "pages are resident" is true on tmpfs too
/// and cannot tell the two apart.
inline bool PageCacheEvictionObservable(const std::filesystem::path &probe) {
  const std::vector<uint8_t> data(1U << 20, 0xAB);
  utils::NonConcurrentOutputFile file;
  file.Open(probe, utils::NonConcurrentOutputFile::Mode::OVERWRITE_EXISTING);
  file.Write(data.data(), data.size());
  file.Sync();
  const auto before = ResidentFraction(probe);
  file.DropCachedPages();
  const auto after = ResidentFraction(probe);
  file.Close();
  return before && after && *before > 0.9 && *after < 0.05;
}

}  // namespace memgraph::test
