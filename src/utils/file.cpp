// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/file.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <type_traits>

#include "utils/logging.hpp"

namespace memgraph::utils {

std::filesystem::path GetExecutablePath() { return std::filesystem::read_symlink("/proc/self/exe"); }

std::vector<std::string> ReadLines(const std::filesystem::path &path) noexcept {
  std::vector<std::string> lines;

  std::ifstream stream(path.c_str());
  // We don't have to check the failed bit of the stream because `getline` won't
  // read anything in that case and that is exactly the behavior that we want.
  std::string line;
  while (std::getline(stream, line)) {
    lines.emplace_back(line);
  }

  return lines;
}

bool EnsureDir(const std::filesystem::path &dir) noexcept {
  std::error_code error_code;  // For exception suppression.
  if (std::filesystem::exists(dir, error_code)) return std::filesystem::is_directory(dir, error_code);
  return std::filesystem::create_directories(dir, error_code);
}

void EnsureDirOrDie(const std::filesystem::path &dir) {
  MG_ASSERT(EnsureDir(dir),
            "Couldn't create directory '{}' due to a permission issue or the "
            "path exists and isn't a directory!",
            dir);
}

bool DirExists(const std::filesystem::path &dir) {
  std::error_code error_code;  // For exception suppression.
  return std::filesystem::is_directory(dir, error_code);
}

bool DeleteDir(const std::filesystem::path &dir) noexcept {
  std::error_code error_code;  // For exception suppression.
  if (!std::filesystem::is_directory(dir, error_code)) return false;
  return std::filesystem::remove_all(dir, error_code) > 0;
}

bool DeleteFile(const std::filesystem::path &file) noexcept {
  std::error_code error_code;  // For exception suppression.
  return std::filesystem::remove(file, error_code);
}

bool CopyFile(const std::filesystem::path &src, const std::filesystem::path &dst) noexcept {
  std::error_code error_code;  // For exception suppression.
  return std::filesystem::copy_file(src, dst, error_code);
}

bool RenamePath(const std::filesystem::path &src, const std::filesystem::path &dst) {
  std::error_code error_code;  // For exception suppression.
  std::filesystem::rename(src, dst, error_code);
  return !error_code;
}

static_assert(std::is_same_v<off_t, ssize_t>, "off_t must fit into ssize_t!");

InputFile::~InputFile() { Close(); }

InputFile::InputFile(InputFile &&other) noexcept
    : fd_(other.fd_),
      path_(std::move(other.path_)),
      file_size_(other.file_size_),
      file_position_(other.file_position_),
      buffer_start_(other.buffer_start_),
      buffer_size_(other.buffer_size_),
      buffer_position_(other.buffer_position_) {
  memcpy(buffer_, other.buffer_, kFileBufferSize);
  other.fd_ = -1;
  other.file_size_ = 0;
  other.file_position_ = 0;
  other.buffer_start_ = std::nullopt;
  other.buffer_size_ = 0;
  other.buffer_position_ = 0;
}

InputFile &InputFile::operator=(InputFile &&other) noexcept {
  Close();

  fd_ = other.fd_;
  path_ = std::move(other.path_);
  file_size_ = other.file_size_;
  file_position_ = other.file_position_;
  buffer_start_ = other.buffer_start_;
  buffer_size_ = other.buffer_size_;
  buffer_position_ = other.buffer_position_;
  memcpy(buffer_, other.buffer_, kFileBufferSize);

  other.fd_ = -1;
  other.file_size_ = 0;
  other.file_position_ = 0;
  other.buffer_start_ = std::nullopt;
  other.buffer_size_ = 0;
  other.buffer_position_ = 0;

  return *this;
}

bool InputFile::Open(const std::filesystem::path &path) {
  if (IsOpen()) return false;

  path_ = path;

  while (true) {
    fd_ = open(path_.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd_ == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are fatal errors and are handled with the
      // return value.
      break;
    }
  }

  if (fd_ == -1) return false;

  // Get file size.
  auto size = SetPosition(Position::RELATIVE_TO_END, 0);
  if (!size || !SetPosition(Position::SET, 0)) {
    Close();
    return false;
  }
  file_size_ = *size;

  return true;
}

bool InputFile::IsOpen() const { return fd_ != -1; }

const std::filesystem::path &InputFile::path() const { return path_; }

bool InputFile::Read(uint8_t *data, size_t size) {
  size_t offset = 0;

  while (size > 0) {
    auto buffer_left = buffer_size_ - buffer_position_;
    if (!buffer_start_ || buffer_left == 0) {
      if (!LoadBuffer()) return false;
      continue;
    }
    auto to_copy = size < buffer_left ? size : buffer_left;
    memcpy(data + offset, buffer_ + buffer_position_, to_copy);
    size -= to_copy;
    offset += to_copy;
    buffer_position_ += to_copy;
  }

  return true;
}

bool InputFile::Peek(uint8_t *data, size_t size) {
  auto old_buffer_start = buffer_start_;
  auto old_buffer_position = buffer_position_;
  auto real_position = GetPosition();

  auto ret = Read(data, size);

  if (buffer_start_ == old_buffer_start) {
    // If we are still within the same buffer (eg. the `size` was small enough),
    // we don't reset the buffer and just set the buffer position to the old
    // buffer position.
    buffer_position_ = old_buffer_position;
  } else {
    SetPosition(Position::SET, real_position);
  }

  return ret;
}

size_t InputFile::GetSize() { return file_size_; }

size_t InputFile::GetPosition() {
  if (buffer_start_) return *buffer_start_ + buffer_position_;
  return file_position_;
}

std::optional<size_t> InputFile::SetPosition(Position position, ssize_t offset) {
  int whence;
  switch (position) {
    case Position::SET:
      whence = SEEK_SET;
      break;
    case Position::RELATIVE_TO_CURRENT:
      whence = SEEK_CUR;
      break;
    case Position::RELATIVE_TO_END:
      whence = SEEK_END;
      break;
  }
  while (true) {
    auto pos = lseek(fd_, offset, whence);
    if (pos == -1 && errno == EINTR) {
      continue;
    }
    if (pos < 0) return std::nullopt;
    file_position_ = pos;
    buffer_start_ = std::nullopt;
    buffer_size_ = 0;
    buffer_position_ = 0;
    return pos;
  }
}

void InputFile::Close() noexcept {
  if (!IsOpen()) return;

  int ret = 0;
  while (true) {
    ret = close(fd_);
    if (ret == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are fatal errors and are handled in the
      // MG_ASSERT below.
      break;
    }
  }

  if (ret != 0) {
    spdlog::error("While trying to close {} an error occurred: {} ({})", path_, strerror(errno), errno);
  }

  fd_ = -1;
  path_ = "";
}

bool InputFile::LoadBuffer() {
  buffer_start_ = std::nullopt;
  buffer_size_ = 0;
  buffer_position_ = 0;

  size_t size = kFileBufferSize;
  if (file_position_ + size >= file_size_) {
    size = file_size_ - file_position_;
  }
  if (size == 0) return false;
  buffer_size_ = size;

  size_t offset = 0;
  while (size > 0) {
    auto got = read(fd_, buffer_ + offset, size);
    if (got == -1 && errno == EINTR) {
      continue;
    }

    if (got <= 0) {
      return false;
    }

    size -= got;
    offset += got;
  }

  buffer_start_ = file_position_;
  file_position_ += buffer_size_;

  return true;
}

OutputFile::~OutputFile() {
  if (IsOpen()) Close();
}

OutputFile::OutputFile(OutputFile &&other) noexcept
    : fd_(other.fd_), written_since_last_sync_(other.written_since_last_sync_), path_(std::move(other.path_)) {
  memcpy(buffer_, other.buffer_, kFileBufferSize);
  buffer_position_.store(other.buffer_position_.load());
  other.fd_ = -1;
  other.written_since_last_sync_ = 0;
  other.buffer_position_ = 0;
}

OutputFile &OutputFile::operator=(OutputFile &&other) noexcept {
  if (IsOpen()) Close();

  fd_ = other.fd_;
  written_since_last_sync_ = other.written_since_last_sync_;
  path_ = std::move(other.path_);
  buffer_position_ = other.buffer_position_.load();
  memcpy(buffer_, other.buffer_, kFileBufferSize);

  other.fd_ = -1;
  other.written_since_last_sync_ = 0;
  other.buffer_position_ = 0;

  return *this;
}

void OutputFile::Open(const std::filesystem::path &path, Mode mode) {
  MG_ASSERT(!IsOpen(),
            "While trying to open {} for writing the database"
            " used a handle that already has {} opened in it!",
            path, path_);
  path_ = path;
  written_since_last_sync_ = 0;

  int flags = O_WRONLY | O_CLOEXEC | O_CREAT;
  if (mode == Mode::APPEND_TO_EXISTING) flags |= O_APPEND;

  while (true) {
    // The permissions are set to ((rw-r-----) & ~umask)
    fd_ = open(path_.c_str(), flags, 0640);
    if (fd_ == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are fatal errors and are handled in the
      // MG_ASSERT below.
      break;
    }
  }

  MG_ASSERT(fd_ != -1, "While trying to open {} for writing an error occurred: {} ({})", path_, strerror(errno), errno);
}

bool OutputFile::IsOpen() const { return fd_ != -1; }

const std::filesystem::path &OutputFile::path() const { return path_; }

void OutputFile::Write(const uint8_t *data, size_t size) {
  while (size > 0) {
    FlushBuffer(false);
    {
      // Reading thread can call EnableFlushing which triggers
      // TryFlushing.
      // We can't use a single shared lock for the entire Write
      // because FlushBuffer acquires the unique_lock.
      std::shared_lock flush_guard(flush_lock_);
      const size_t buffer_position = buffer_position_.load();
      auto buffer_left = kFileBufferSize - buffer_position;
      auto to_write = size < buffer_left ? size : buffer_left;
      memcpy(buffer_ + buffer_position, data, to_write);
      size -= to_write;
      data += to_write;
      buffer_position_.fetch_add(to_write);
      written_since_last_sync_ += to_write;
    }
  }
}

void OutputFile::Write(const char *data, size_t size) { Write(reinterpret_cast<const uint8_t *>(data), size); }
void OutputFile::Write(const std::string_view data) { Write(data.data(), data.size()); }

size_t OutputFile::SeekFile(const Position position, const ssize_t offset) {
  int whence;
  switch (position) {
    case Position::SET:
      whence = SEEK_SET;
      break;
    case Position::RELATIVE_TO_CURRENT:
      whence = SEEK_CUR;
      break;
    case Position::RELATIVE_TO_END:
      whence = SEEK_END;
      break;
  }
  while (true) {
    auto pos = lseek(fd_, offset, whence);
    if (pos == -1 && errno == EINTR) {
      continue;
    }
    MG_ASSERT(pos >= 0, "While trying to set the position in {} an error occurred: {} ({})", path_, strerror(errno),
              errno);
    return pos;
  }
}

size_t OutputFile::GetPosition() { return SetPosition(Position::RELATIVE_TO_CURRENT, 0); }

size_t OutputFile::SetPosition(Position position, ssize_t offset) {
  FlushBuffer(true);
  return SeekFile(position, offset);
}

bool OutputFile::AcquireLock() {
  MG_ASSERT(IsOpen(), "Trying to acquire a write lock on an unopened file!");
  int ret = -1;
  while (true) {
    struct flock lock;
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    ret = fcntl(fd_, F_SETLK, &lock);
    if (ret == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are handled in the return below.
      break;
    }
  }
  return ret != -1;
}

void OutputFile::Sync() {
  FlushBuffer(true);

  int ret = 0;
  while (true) {
    ret = fsync(fd_);
    if (ret == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are fatal errors and are handled in the
      // MG_ASSERT below.
      break;
    }
  }

  // In this check we are extremely rigorous because any error except EINTR is
  // treated as a fatal error that will crash the database. The errors that will
  // mainly occur are EIO which indicates an I/O error on the physical device
  // and ENOSPC (documented only in new kernels) which indicates that the
  // physical device doesn't have any space left. If we don't succeed in
  // syncing pending data to the physical device there is no mechanism to
  // determine which parts of the `write` calls weren't synced. That is why
  // we call this a fatal error and we don't continue further.
  //
  // A good description of issues with `fsync` can be seen here:
  // https://stackoverflow.com/questions/42434872/writing-programs-to-cope-with-i-o-errors-causing-lost-writes-on-linux
  //
  // A discussion between PostgreSQL developers of what to do when `fsync`
  // fails can be seen here:
  // https://www.postgresql.org/message-id/flat/CAMsr%2BYE5Gs9iPqw2mQ6OHt1aC5Qk5EuBFCyG%2BvzHun1EqMxyQg%40mail.gmail.com#CAMsr+YE5Gs9iPqw2mQ6OHt1aC5Qk5EuBFCyG+vzHun1EqMxyQg@mail.gmail.com
  //
  // A brief of the `fsync` semantics can be seen here (part of the mailing list
  // discussion linked above):
  // https://www.postgresql.org/message-id/20180402185320.GM11627%40technoir
  //
  // The PostgreSQL developers decided to do the same thing (die) when such an
  // error occurs:
  // https://www.postgresql.org/message-id/20180427222842.in2e4mibx45zdth5@alap3.anarazel.de
  MG_ASSERT(ret == 0,
            "While trying to sync {}, an error occurred: {} ({}). Possibly {} "
            "bytes from previous write calls were lost.",
            path_, strerror(errno), errno, written_since_last_sync_);

  // Reset the counter.
  written_since_last_sync_ = 0;
}

void OutputFile::Close() noexcept {
  FlushBuffer(true);

  int ret = 0;
  while (true) {
    ret = close(fd_);
    if (ret == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are fatal errors and are handled in the
      // MG_ASSERT below.
      break;
    }
  }

  MG_ASSERT(ret == 0,
            "While trying to close {}, an error occurred: {} ({}). Possibly {} "
            "bytes from previous write calls were lost.",
            path_, strerror(errno), errno, written_since_last_sync_);

  fd_ = -1;
  written_since_last_sync_ = 0;
  path_ = "";
}

void OutputFile::FlushBuffer(bool force_flush) {
  MG_ASSERT(IsOpen(), "Flushing an unopened file.");

  if (!force_flush && buffer_position_.load() < kFileBufferSize) return;

  std::unique_lock flush_guard(flush_lock_);
  FlushBufferInternal();
}

void OutputFile::FlushBufferInternal() {
  MG_ASSERT(buffer_position_ <= kFileBufferSize,
            "While trying to write to {} more file was written to the "
            "buffer than the buffer has space!",
            path_);

  auto *buffer = buffer_;
  auto buffer_position = buffer_position_.load();
  while (buffer_position > 0) {
    auto written = write(fd_, buffer, buffer_position_);
    if (written == -1 && errno == EINTR) {
      continue;
    }

    MG_ASSERT(written > 0,
              "while trying to write to {} an error occurred: {} ({}). "
              "Possibly {} bytes of data were lost from this call and "
              "possibly {} bytes were lost from previous calls.",
              path_, strerror(errno), errno, buffer_position_, written_since_last_sync_);

    buffer_position -= written;
    buffer += written;
  }

  buffer_position_.store(buffer_position);
}

void OutputFile::DisableFlushing() { flush_lock_.lock_shared(); }

void OutputFile::EnableFlushing() {
  flush_lock_.unlock_shared();
  TryFlushing();
}

std::pair<const uint8_t *, size_t> OutputFile::CurrentBuffer() const { return {buffer_, buffer_position_.load()}; }

size_t OutputFile::GetSize() {
  // There's an alternative way of fetching the files size using fstat.
  // lseek should be faster for smaller number of clients while fstat
  // should have an advantage for high number of clients.
  // The reason for this is the way those functions implement the
  // support for multi-threading. While lseek uses locks, fstat is lockfree.
  // For now, lseek should be good enough. If at any point this proves to
  // be a bottleneck, fstat should be considered.
  return SeekFile(Position::RELATIVE_TO_END, 0) + buffer_position_.load();
}

void OutputFile::TryFlushing() {
  if (std::unique_lock guard(flush_lock_, std::try_to_lock); guard.owns_lock()) {
    FlushBufferInternal();
  }
}

}  // namespace memgraph::utils
