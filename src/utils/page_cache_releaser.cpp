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

#include "utils/page_cache_releaser.hpp"

#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::utils {

namespace {
// Non-owning, so the releaser's lifetime is exactly the handle `main` holds. Synchronized because
// install and lookup are not otherwise ordered: a replica loads a received snapshot on a
// replication thread, and a test may install one after those threads exist.
auto &Installed() {
  static Synchronized<std::weak_ptr<PageCacheReleaser>, SpinLock> installed;
  return installed;
}
}  // namespace

std::shared_ptr<PageCacheReleaser> InstallPageCacheReleaser() {
  auto releaser = std::make_shared<PageCacheReleaser>();
  Installed().WithLock([&](auto &installed) { installed = releaser; });
  return releaser;
}

std::weak_ptr<PageCacheReleaser> PageCacheReleaserHandle() {
  return Installed().WithLock([](auto const &installed) { return installed; });
}

}  // namespace memgraph::utils
