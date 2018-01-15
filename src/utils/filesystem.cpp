#include "filesystem.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "fmt/format.h"
#include "fmt/ostream.h"
#include "glog/logging.h"

namespace utils {

File::File() : fd_(-1), path_() {}

File::File(int fd, fs::path path) : fd_(fd), path_(std::move(path)) {}

File::~File() {
  CHECK(fd_ == -1) << fmt::format(
      "Underlying file descriptor should be released or closed "
      "before destructing (fd = {}, path = {})",
      fd_, path_);
}

File::File(File &&rhs) : fd_(rhs.fd_), path_(rhs.path_) {
  LOG(INFO) << "Move constructor";
  rhs.Release();
}

File &File::operator=(File &&rhs) {
  if (this != &rhs) {
    fd_ = rhs.fd_;
    path_ = rhs.path_;
    rhs.Release();
  }
  return *this;
}

fs::path File::Path() const { return path_; }
int File::Handle() const { return fd_; }
bool File::Empty() const { return fd_ == -1; }

void File::Release() {
  fd_ = -1;
  path_ = fs::path();
}

File OpenFile(const fs::path &path, int flags, ::mode_t mode) {
  int fd = ::open(path.c_str(), flags, mode);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot open {}", path));
  }

  return File(fd, path);
}

File TryOpenFile(const fs::path &path, int flags, ::mode_t mode) {
  int fd = ::open(path.c_str(), flags, mode);
  return fd == -1 ? File() : File(fd, path);
}

File OpenFile(const File &dir, const fs::path &path, int flags, ::mode_t mode) {
  int fd = ::openat(dir.Handle(), path.c_str(), flags, mode);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot open {}", dir.Path() / path));
  }
  return File(fd, dir.Path() / path);
}

File TryOpenFile(const File &dir, const fs::path &path, int flags,
                 ::mode_t mode) {
  int fd = ::openat(dir.Handle(), path.c_str(), flags, mode);
  return fd == -1 ? File() : File(fd, dir.Path() / path);
}

void Close(File &file) {
  if (::close(file.Handle()) == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot close {}", file.Path()));
  }
  file.Release();
}

void Fsync(const File &file) {
  if (::fsync(file.Handle()) == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot fsync {}", file.Path()));
  }
}

void Rename(const File &dir1, const fs::path &path1, const File &dir2,
            const fs::path &path2) {
  if (::renameat(dir1.Handle(), path1.c_str(), dir2.Handle(), path2.c_str()) !=
      0) {
    throw std::system_error(
        errno, std::generic_category(),
        fmt::format("cannot move {} to {}", dir1.Path() / path1,
                    dir2.Path() / path2));
  }
}

void SyncDir(const fs::path &path) {
  File dir = OpenFile(path, O_DIRECTORY | O_RDONLY);
  Fsync(dir);
  Close(dir);
}

File OpenDir(const fs::path &path) {
  int res = ::mkdir(path.c_str(), 0777);
  if (res == 0) {
    if (path.has_parent_path()) {
      SyncDir(path.parent_path());
    }
  } else {
    if (errno != EEXIST) {
      throw std::system_error(errno, std::generic_category(),
                              fmt::format("cannot create directory {}", path));
    }
  }

  int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot open directory {}", path));
  }

  return File(fd, path);
}

}  // namespace utils
