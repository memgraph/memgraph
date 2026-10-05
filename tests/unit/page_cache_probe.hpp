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
/// Dropping pages is advisory, so a file's contents say nothing about whether it worked: residency
/// is the only sign of a release that happened rather than one the kernel ignored. Every assertion
/// built on it has to be gated on the filesystem being one where it means anything, and paired with
/// a file the release did not touch, because memory pressure empties a file just as thoroughly.
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

/// Residency of `path` in the page cache, as a fraction of its pages, via mincore(2).
///
/// cachestat(2) would answer the sharper question, separating pages a release took from pages
/// memory pressure took, and it cannot be used here: it reads the mapping belonging to the file it
/// is given, and on overlayfs, which is what a container's filesystem is, reads and writes are
/// served by the file underneath, so it reports an empty cache for a file that was just read.
/// mmap is passed down to that same lower file, which is why mincore answers correctly there.
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
