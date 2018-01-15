/**
 * @file
 * @brief This file contains C++ wrappers around some C system calls.
 */
#pragma once

#include <experimental/filesystem>

namespace fs = std::experimental::filesystem;

namespace utils {

/**
 * @brief Thin wrapper around system file handle.
 */
class File {
 public:
  /**
   * Constructs an empty file handle.
   */
  File();

  /**
   * Wraps the given system file handle.
   *
   * This class doesn't take ownership of the file handle, it won't close it
   * on destruction.
   *
   * @param fd   System file handle.
   * @param path Pathname naming the file, used only for error handling.
   */
  File(int fd, fs::path path);

  File(File &&);
  File &operator=(File &&);

  File(const File &) = delete;
  File &operator=(const File &) = delete;

  /***
   * Destructor -- crashes the program if the underlying file descriptor is not
   * released or closed.
   */
  ~File();

  /**
   * Gets the path to the underlying file.
   */
  fs::path Path() const;

  /**
   * Gets the underlying file handle.
   */
  int Handle() const;

  /**
   * Checks if there's an underlying file handle.
   */
  bool Empty() const;

  /**
   * Releases the underlying file handle.
   *
   * File descriptor will be empty after the call returns.
   */
  void Release();

 private:
  int fd_;
  fs::path path_;
};

/**
 * Opens a file descriptor.
 * Wrapper around `open` system call (see `man 2 open`).
 *
 * @param  path  Pathname naming the file.
 * @param  flags Opening flags.
 * @param  mode  File mode bits to apply for creation of new file.
 * @return File descriptor referring to the opened file.
 *
 * @throws std::system_error
 */
File OpenFile(const fs::path &path, int flags, ::mode_t mode = 0666);

/**
 * Same as OpenFile, but returns an empty File object instead of
 * throwing an exception.
 */
File TryOpenFile(const fs::path &path, int flags, ::mode_t mode = 0666);

/**
 * Opens a file descriptor for a file inside a directory.
 * Wrapper around `openat` system call (see `man 2 openat`).
 *
 * @param  dir   Directory file descriptor.
 * @param  path  Pathname naming the file.
 * @param  flags Opening flags.
 * @param  mode  File mode bits to apply for creation of new file.
 * @return File descriptor referring to the opened file.
 *
 * @throws std::system_error
 */
File OpenFile(const File &dir, const fs::path &path, int flags,
              ::mode_t mode = 0666);

/**
 * Same as OpenFile, but returns an empty File object instead of
 * throwing an exception.
 */
File TryOpenFile(const File &dir, const fs::path &path, int flags,
                 ::mode_t mode = 0666);

/**
 * Closes a file descriptor.
 * Wrapper around `close` system call (see `man 2 close`). File descriptor will
 * be empty after the call returns.
 *
 * @param file File descriptor to be closed.
 *
 * @throws std::system_error
 */
void Close(File &file);

/**
 * Synchronizes file with the underlying storage device.
 * Wrapper around `fsync` system call (see `man 2 fsync`).
 *
 * @param file File descriptor referring to the file to be synchronized.
 *
 * @throws std::system_error
 */
void Fsync(const File &file);

/**
 * Moves a file from one directory to another.
 *
 * Wrapper around `renameat` system call (see `man 2 renameat`).
 *
 * @param dir1  Source directory.
 * @param path1 Pathname naming the source file.
 * @param dir2  Destination directory.
 * @param path2 Pathname naming the destination file.
 *
 * @throws std::system_error
 */
void Rename(const File &dir1, const fs::path &path1, const File &dir2,
            const fs::path &path2);

/**
 * Synchronizes directory with the underlying storage device.
 *
 * @param path Pathname naming the directory.
 *
 * @throws std::system_error
 */
void SyncDir(const fs::path &path);

/**
 * Opens a directory, creating it if it doesn't exist.
 *
 * @param  path Pathname naming the directory.
 * @return File descriptor referring to the opened directory.
 *
 * @throws std::system_error
 */
File OpenDir(const fs::path &path);

}  // namespace utils
