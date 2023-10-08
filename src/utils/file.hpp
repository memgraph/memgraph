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

/**
 * @file
 *
 * This file contains utilities for operations with files.
 * `InputFile` and `OutputFile` classes are provided which emulate a `fstream`.
 */
#pragma once

#include <atomic>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "utils/rw_lock.hpp"

namespace memgraph::utils {

/// Get the path of the current executable.
///
/// @throw std::filesystem::filesystem_error
/// @throw std::bad_alloc
std::filesystem::path GetExecutablePath();

/// Reads all lines from the file specified by path. If the file doesn't exist
/// or there is an access error the function returns an empty list.
std::vector<std::string> ReadLines(const std::filesystem::path &path) noexcept;

/// Ensures that the given directory either exists after this call. If the
/// directory didn't exist prior to the call it is created, if it existed prior
/// to the call it is left as is.
bool EnsureDir(const std::filesystem::path &dir) noexcept;

/// Calls `EnsureDir` and terminates the program if the call failed. It prints
/// an error message for which directory the ensuring failed.
void EnsureDirOrDie(const std::filesystem::path &dir);

/// Returns a boolean indicating whether the directory exists.
bool DirExists(const std::filesystem::path &dir);

/// Deletes everything from the given directory including the directory.
bool DeleteDir(const std::filesystem::path &dir) noexcept;

/// Deletes just the specified file. Symlinks are not followed.
bool DeleteFile(const std::filesystem::path &file) noexcept;

/// Copies the file from `src` to `dst`.
bool CopyFile(const std::filesystem::path &src, const std::filesystem::path &dst) noexcept;

/// Renames the path from `src` to `dst`. If the `dst` contains directories that
/// don't exist, the renaming fails. Symlinks are not followed.
bool RenamePath(const std::filesystem::path &src, const std::filesystem::path &dst);

/// Checks if process has read access to the file.
bool HasReadAccess(const std::filesystem::path &path);

/// Buffer size used for `InputFile` and `OutputFile` implementations. Using
/// system calls is very expensive and we can't afford to call either `read` or
/// `write` for each of our (very small) logical reads/writes. Because of that,
/// `read` or `write` is only called when the buffer is full and/or needs
/// emptying.
inline constexpr size_t kFileBufferSize = 262144;

/// This class implements a file handler that is used to read binary files. It
/// was developed because the C++ standard library has an awful API and makes
/// handling of binary data extremely tedious.
///
/// This class *isn't* thread safe. It is implemented as a wrapper around low
/// level system calls used for file manipulation.
class InputFile {
 public:
  enum class Position {
    SET,
    RELATIVE_TO_CURRENT,
    RELATIVE_TO_END,
  };

  InputFile() = default;
  ~InputFile();

  InputFile(const InputFile &) = delete;
  InputFile &operator=(const InputFile &) = delete;

  InputFile(InputFile &&other) noexcept;
  InputFile &operator=(InputFile &&other) noexcept;

  /// This method opens the file used for reading. If the file can't be opened
  /// or doesn't exist it returns `false`.
  bool Open(const std::filesystem::path &path);

  /// Returns a boolean indicating whether a file is opened.
  bool IsOpen() const;

  /// Returns the path to the currently opened file. If a file isn't opened the
  /// path is empty.
  const std::filesystem::path &path() const;

  /// Reads `size` bytes from the file into the memory pointed by `data` and
  /// returns a boolean indicating whether the read succeeded. Reading the file
  /// changes the current position in the file.
  bool Read(uint8_t *data, size_t size);

  /// Peeks `size` bytes from the file into the memory pointed by `data` and
  /// returns a boolean indicating whether the peek succeeded. Peeking the file
  /// doesn't change the current position in the file.
  bool Peek(uint8_t *data, size_t size);

  /// This method gets the size of the file.
  size_t GetSize();

  /// This method gets the current absolute position in the file.
  size_t GetPosition();

  /// This method sets the current position in the file and returns the absolute
  /// set position in the file. The position is set to `offset` with the
  /// starting point taken from `position`. On failure it returns
  /// `std::nullopt`.
  std::optional<size_t> SetPosition(Position position, ssize_t offset);

  /// Closes the currently opened file. On failure it crashes the program.
  void Close() noexcept;

 private:
  bool LoadBuffer();

  int fd_{-1};
  std::filesystem::path path_;
  size_t file_size_{0};
  size_t file_position_{0};

  uint8_t buffer_[kFileBufferSize];
  std::optional<size_t> buffer_start_;
  size_t buffer_size_{0};
  size_t buffer_position_{0};
};

/// This class implements a file handler that is used for mission critical files
/// that need to be written and synced to permanent storage. Typical usage for
/// this class is in implementation of write-ahead logging or anything similar
/// that requires that data that is written *must* be stored in permanent
/// storage.
///
/// If any of the methods fails with a critical error *they will crash* the
/// whole program. The reasoning is that if you have some data that is mission
/// critical to be written to permanent storage and you fail in doing so you
/// aren't safe to continue your operation. The errors that can occur are mainly
/// EIO (unrecoverable underlying storage error) or ENOSPC (the underlying
/// storage has no more space).
///
/// The typical usage for this class when writing data to the file is that you
/// call `Write` as many times as necessary to write one logical part of your
/// data and only then you call `Sync`. For the write-ahead log example that
/// would mean that you call `Write` until you write a whole single state delta
/// and only after that you call `Sync` to ensure that the whole delta was
/// written to permanent storage.
///
/// This class *isn't* thread safe. It is implemented as a wrapper around low
/// level system calls used for file manipulation. It allows concurrent
/// READING of the file that is being written. To read the file, disable the
/// flushing of the internal buffer using `DisableFlushing`. Don't forget to
/// enable flushing again after you're done with reading using the
/// 'EnableFlushing' method!
class OutputFile {
 public:
  enum class Mode {
    OVERWRITE_EXISTING,
    APPEND_TO_EXISTING,
  };

  enum class Position {
    SET,
    RELATIVE_TO_CURRENT,
    RELATIVE_TO_END,
  };

  OutputFile() = default;
  ~OutputFile();

  OutputFile(const OutputFile &) = delete;
  OutputFile &operator=(const OutputFile &) = delete;

  OutputFile(OutputFile &&other) noexcept;
  OutputFile &operator=(OutputFile &&other) noexcept;

  /// This method opens a new file used for writing. If the file doesn't exist
  /// it is created. The `mode` flags controls whether data is appended to the
  /// file or the file is wiped on first write. Files are created with a
  /// restrictive permission mask (0640). On failure and misuse it crashes the
  /// program.
  void Open(const std::filesystem::path &path, Mode mode);

  /// Returns a boolean indicating whether a file is opened.
  bool IsOpen() const;

  /// Returns the path to the currently opened file. If a file isn't opened the
  /// path is empty.
  const std::filesystem::path &path() const;

  /// Writes data to the currently opened file. On failure and misuse it crashes
  /// the program.
  void Write(const uint8_t *data, size_t size);
  void Write(const char *data, size_t size);
  void Write(std::string_view data);

  /// This method gets the current absolute position in the file. On failure and
  /// misuse it crashes the program.
  size_t GetPosition();

  /// This method sets the current position in the file and returns the absolute
  /// set position in the file. The position is set to `offset` with the
  /// starting point taken from `position`. On failure and misuse it crashes the
  /// program.
  size_t SetPosition(Position position, ssize_t offset);

  /// This function tries to acquire a POSIX write lock on the file. The
  /// acquired lock is valid during the whole lifetime of the process and can't
  /// be acquired again. The function returns `true` if the lock was required
  /// successfully, `false` is returned otherwise. On misuse it crashes the
  /// program.
  bool AcquireLock();

  /// Syncs currently pending data to the currently opened file. On failure
  /// and misuse it crashes the program.
  void Sync();

  /// Closes the currently opened file. It doesn't perform a `Sync` on the
  /// file. On failure and misuse it crashes the program.
  void Close() noexcept;

  /// Disable flushing of the internal buffer.
  void DisableFlushing();

  /// Enable flushing of the internal buffer.
  /// Before the flushing is enabled, the internal buffer
  /// is flushed.
  void EnableFlushing();

  /// Try flushing the internal buffer.
  void TryFlushing();

  /// Get the internal buffer with its current size.
  std::pair<const uint8_t *, size_t> CurrentBuffer() const;

  /// Get the size of the file.
  size_t GetSize();

 private:
  void FlushBuffer(bool force_flush);
  void FlushBufferInternal();

  size_t SeekFile(Position position, ssize_t offset);

  int fd_{-1};
  size_t written_since_last_sync_{0};
  std::filesystem::path path_;
  uint8_t buffer_[kFileBufferSize];
  std::atomic<size_t> buffer_position_{0};

  // Flushing buffer should be a higher priority
  utils::RWLock flush_lock_{RWLock::Priority::WRITE};
};

}  // namespace memgraph::utils
