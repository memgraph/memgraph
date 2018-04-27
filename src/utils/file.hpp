/**
 * @file
 *
 * This file contains utilities for operations with files. Other than utility
 * funtions, a `File` class is provided which wraps a system file handle.
 */
#pragma once

#include <experimental/filesystem>
#include <experimental/optional>
#include <fstream>

namespace fs = std::experimental::filesystem;

namespace utils {

// Higher level utility operations follow.

/**
 * Loads all file paths in the specified directory. Optionally
 * the paths are filtered by extension.
 *
 * NOTE: the call isn't recursive
 *
 * @param directory a path to directory that will be scanned in order to find
 *                  all paths
 * @param extension paths will be filtered by this extension
 *
 * @return std::vector of paths founded in the directory
 */
std::vector<fs::path> LoadFilePaths(const fs::path &directory,
                                    const std::string &extension = "");

// TODO: add error checking
/**
 * Reads all lines from the file specified by path.
 *
 * @param path file path.
 * @return vector of all lines from the file.
 */
std::vector<std::string> ReadLines(const fs::path &path);

/**
 * Writes text into the file specified by path.
 *
 * @param text content which will be written in the file.
 * @param path a path to the file.
 */
void Write(const std::string &text, const fs::path &path);

/**
 * Esures that the given dir either exists or is succsefully created.
 */
bool EnsureDir(const std::experimental::filesystem::path &dir);

/**
 * Ensures the given directory exists and is ready for use. Creates
 * the directory if it doesn't exist.
 */
void CheckDir(const std::string &dir);

// End higher level operations.

// Lower level wrappers around C system calls follow.

/**
 * Thin wrapper around system file handle.
 */
class File {
 public:
  /** Constructs an empty file handle. */
  File();

  /**
   * Take ownership of the given system file handle.
   *
   * @param fd   System file handle.
   * @param path Pathname naming the file, used only for error handling.
   */
  File(int fd, fs::path path);

  File(File &&);
  File &operator=(File &&);

  File(const File &) = delete;
  File &operator=(const File &) = delete;

  /**
   * Closes the underlying file handle.
   *
   * @throws std::system_error
   */
  ~File();

  /** Gets the path to the underlying file. */
  fs::path Path() const;

  /** Gets the underlying file handle. */
  int Handle() const;

  /** Checks if there's an underlying file handle. */
  bool Empty() const;

  /**
   * Closes the underlying file handle.
   *
   * Wrapper around `close` system call (see `man 2 close`). File will
   * be empty after the call returns. You may call Close multiple times, all
   * calls after the first one are no-op.
   *
   * @throws std::system_error
   */
  void Close();

 private:
  int fd_;
  fs::path path_;

  void Release();
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
 * Same as OpenFile, but returns `nullopt` instead of throwing an exception.
 */
std::experimental::optional<File> TryOpenFile(const fs::path &path, int flags,
                                              ::mode_t mode = 0666);

/** Calls File::Close */
inline void Close(File &file) { file.Close(); }

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
 * Same as OpenFile, but returns `nullopt` instead of throwing an exception.
 */
std::experimental::optional<File> TryOpenFile(const File &dir,
                                              const fs::path &path, int flags,
                                              ::mode_t mode = 0666);

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

// End lower level wrappers.

}  // namespace utils
