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

#include "utils/download_temp_file.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace memgraph::utils {

namespace {
auto LastError() -> std::error_code { return std::error_code{errno, std::generic_category()}; }

// Only the last six X are replaced, whatever the template contains, so a longer run of them buys no
// additional randomness. Uniqueness is the kernel's guarantee here, not this string's.
constexpr auto kDownloadTemplate = "memgraph_download_XXXXXX";
}  // namespace

auto DownloadTempFile::Create(std::filesystem::path const &dir) -> std::expected<DownloadTempFile, std::error_code> {
  auto path = (dir / kDownloadTemplate).string();

  auto const fd = ::mkstemp(path.data());
  if (fd == -1) {
    return std::unexpected{LastError()};
  }

  if (::unlink(path.c_str()) == -1) {
    auto const error = LastError();
    ::close(fd);
    return std::unexpected{error};
  }

  return DownloadTempFile{fd};
}

auto DownloadTempFile::Create() -> std::expected<DownloadTempFile, std::error_code> {
  std::error_code error;
  auto const dir = std::filesystem::temp_directory_path(error);
  if (error) {
    return std::unexpected{error};
  }
  return Create(dir);
}

DownloadTempFile::DownloadTempFile(DownloadTempFile &&other) noexcept : fd_{std::exchange(other.fd_, -1)} {}

auto DownloadTempFile::operator=(DownloadTempFile &&other) noexcept -> DownloadTempFile & {
  if (this != &other) {
    if (fd_ != -1) {
      ::close(fd_);
    }
    fd_ = std::exchange(other.fd_, -1);
  }
  return *this;
}

DownloadTempFile::~DownloadTempFile() {
  if (fd_ != -1) {
    ::close(fd_);
  }
}

auto DownloadTempFile::OpenStream() -> std::expected<FileUniquePtr, std::error_code> {
  // The stream gets a descriptor of its own so that a writer which closes what it is handed cannot
  // take the file out from under a reader that has not run yet.
  auto duplicated = DupFd();
  if (!duplicated) {
    return std::unexpected{duplicated.error()};
  }

  auto *stream = ::fdopen(duplicated->Get(), "wb");
  if (stream == nullptr) {
    return std::unexpected{LastError()};
  }
  // fdopen took it over: closing the stream closes the descriptor.
  static_cast<void>(duplicated->Release());

  return FileUniquePtr{stream, &std::fclose};
}

// Seeking moves the offset of the open file description, which every descriptor taken from this one
// shares, so this is not a const operation however little of the object itself it touches.
// NOLINTNEXTLINE(readability-make-member-function-const)
auto DownloadTempFile::DupFd() -> std::expected<OwnedFd, std::error_code> {
  auto duplicated = OwnedFd{::dup(fd_)};
  if (duplicated.Get() == -1) {
    return std::unexpected{LastError()};
  }

  // A duplicate shares its file offset with the original, which a completed write leaves at the end.
  if (::lseek(duplicated.Get(), 0, SEEK_SET) == -1) {
    return std::unexpected{LastError()};
  }

  return duplicated;
}

}  // namespace memgraph::utils
