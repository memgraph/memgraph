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

#pragma once

#include <memory>
#include <utility>

#include "utils/thread_pool.hpp"

namespace memgraph::utils {

/// Drops fully-read files from the page cache away from the thread that read them.
///
/// The drop costs kernel work proportional to the size of the file, does not get faster with more
/// threads, and nothing waits on its result. On the recovery path that would be time an instance
/// spends not yet serving, for a benefit that is just as good arriving a moment later.
class PageCacheReleaser {
 public:
  PageCacheReleaser() = default;

  PageCacheReleaser(PageCacheReleaser const &) = delete;
  PageCacheReleaser(PageCacheReleaser &&) = delete;
  PageCacheReleaser &operator=(PageCacheReleaser const &) = delete;
  PageCacheReleaser &operator=(PageCacheReleaser &&) = delete;
  ~PageCacheReleaser() = default;

  /// Takes ownership of `file` and drops its pages on the releaser's thread. Ownership is what keeps
  /// the descriptor open until the drop has happened.
  template <typename File>
  void Drop(File file) {
    pool_.AddTask([file = std::move(file)]() mutable { file.DropCachedPages(); });
  }

 private:
  ThreadPool pool_{1};
};

/// Installs the process-wide releaser and returns the handle that owns it.
///
/// The caller must hold the handle for as long as anything can hand this a file, and release it
/// while the process is still running. Letting it reach static destruction instead leaves the
/// worker being joined at a point where its ordering against the loggers and allocators it can
/// touch is undefined. Installing again replaces the handle.
[[nodiscard]] std::shared_ptr<PageCacheReleaser> InstallPageCacheReleaser();

/// The installed releaser, expired when no handle is being held. A caller that cannot lock it
/// drops the pages itself, which reaches the same state and only costs it the wait.
std::weak_ptr<PageCacheReleaser> PageCacheReleaserHandle();

}  // namespace memgraph::utils
