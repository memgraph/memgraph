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

#include <unistd.h>

#include <expected>
#include <filesystem>
#include <system_error>
#include <utility>

#include "utils/file.hpp"

namespace memgraph::utils {

/// An open file descriptor with an owner. Closing it is this type's job, so a function that returns
/// one has said who closes it without a caller having to read for the answer.
class OwnedFd {
 public:
  explicit OwnedFd(int fd) noexcept : fd_{fd} {}

  OwnedFd(OwnedFd const &) = delete;
  auto operator=(OwnedFd const &) -> OwnedFd & = delete;

  OwnedFd(OwnedFd &&other) noexcept : fd_{std::exchange(other.fd_, -1)} {}

  auto operator=(OwnedFd &&other) noexcept -> OwnedFd & {
    if (this != &other) {
      Close();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }

  ~OwnedFd() { Close(); }

  [[nodiscard]] auto Get() const noexcept -> int { return fd_; }

  /// Hands the descriptor to a caller that takes it over, such as a library that closes what it is
  /// given. This object holds nothing afterwards.
  [[nodiscard]] auto Release() noexcept -> int { return std::exchange(fd_, -1); }

 private:
  void Close() noexcept {
    if (auto const fd = std::exchange(fd_, -1); fd != -1) {
      ::close(fd);
    }
  }

  int fd_{-1};
};

/// A file that exists only as an open descriptor, for downloading content that is read once and
/// discarded.
///
/// The name is a fixed-length template rather than anything derived from a caller-supplied URI, so
/// no input can push it past the filesystem's limit on a path component. It is unlinked as soon as
/// it is created, which means the download cannot be left behind by a signal or a crash, and that
/// no consumer can come to depend on what it was called.
class DownloadTempFile {
 public:
  /// Creates the file in `dir`.
  static auto Create(std::filesystem::path const &dir) -> std::expected<DownloadTempFile, std::error_code>;

  /// Creates the file in the directory `TMPDIR` names, falling back to the system default.
  static auto Create() -> std::expected<DownloadTempFile, std::error_code>;

  DownloadTempFile(DownloadTempFile const &) = delete;
  auto operator=(DownloadTempFile const &) -> DownloadTempFile & = delete;

  DownloadTempFile(DownloadTempFile &&other) noexcept;
  auto operator=(DownloadTempFile &&other) noexcept -> DownloadTempFile &;

  ~DownloadTempFile();

  /// A stream positioned at the start of the file, for a writer that closes what it is handed.
  /// Closing it leaves this object's own descriptor open.
  [[nodiscard]] auto OpenStream() -> std::expected<FileUniquePtr, std::error_code>;

  /// A descriptor of its own, positioned at the start of the file.
  [[nodiscard]] auto DupFd() -> std::expected<OwnedFd, std::error_code>;

 private:
  explicit DownloadTempFile(int fd) noexcept : fd_{fd} {}

  int fd_{-1};
};

}  // namespace memgraph::utils
