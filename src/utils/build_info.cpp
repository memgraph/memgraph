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

#include "build_info.hpp"

#include <link.h>

#include <cstddef>
#include <cstring>
#include <string_view>

namespace memgraph::utils {

namespace {

// dl_iterate_phdr is not async-signal-safe; this only runs at startup and from
// the terminate handler (normal context), never from a signal handler.
std::string ComputeBuildId() {
  std::string result;
  dl_iterate_phdr(
      [](dl_phdr_info *info, size_t /*size*/, void *data) -> int {
        auto *out = static_cast<std::string *>(data);
        for (ElfW(Half) i = 0; i < info->dlpi_phnum; ++i) {
          const ElfW(Phdr) &ph = info->dlpi_phdr[i];
          if (ph.p_type != PT_NOTE) continue;
          // dlpi_addr + p_vaddr is the segment's runtime address; the int-to-ptr cast is intrinsic to ELF.
          // NOLINTNEXTLINE(performance-no-int-to-ptr)
          const auto *p = reinterpret_cast<const unsigned char *>(info->dlpi_addr + ph.p_vaddr);
          const auto *end = p + ph.p_memsz;
          while (p + sizeof(ElfW(Nhdr)) <= end) {
            const auto *n = reinterpret_cast<const ElfW(Nhdr) *>(p);
            const auto name_pad = (n->n_namesz + 3U) & ~3U;
            const char *name = reinterpret_cast<const char *>(p + sizeof(ElfW(Nhdr)));
            const unsigned char *desc = p + sizeof(ElfW(Nhdr)) + name_pad;
            if (n->n_type == NT_GNU_BUILD_ID && n->n_namesz == 4 && std::memcmp(name, "GNU", 4) == 0) {
              static constexpr std::string_view kHex = "0123456789abcdef";
              out->reserve(static_cast<std::size_t>(n->n_descsz) * 2);
              for (ElfW(Word) j = 0; j < n->n_descsz; ++j) {
                out->push_back(kHex[desc[j] >> 4]);
                out->push_back(kHex[desc[j] & 0x0FU]);
              }
              return 1;  // found
            }
            p += sizeof(ElfW(Nhdr)) + name_pad + ((n->n_descsz + 3U) & ~3U);
          }
        }
        return 1;  // only inspect the main executable (the first object)
      },
      &result);
  return result;
}

}  // namespace

std::string GetBuildId() { return ComputeBuildId(); }

BuildInfo GetBuildInfo() {
#ifdef CMAKE_BUILD_TYPE_NAME
  constexpr const char *build_info_name = CMAKE_BUILD_TYPE_NAME;
#else
  constexpr const char *build_info_name = "unknown";
#endif
#ifdef MEMGRAPH_VERSION_STRING
  constexpr const char *build_version = MEMGRAPH_VERSION_STRING;
#else
  constexpr const char *build_version = "unknown";
#endif
  return BuildInfo{.build_name = build_info_name, .version = build_version, .build_id = ComputeBuildId()};
}

}  // namespace memgraph::utils
