#include "utils/file.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "fmt/format.h"
#include "fmt/ostream.h"
#include "glog/logging.h"

namespace utils {

std::vector<fs::path> LoadFilePaths(const fs::path &directory,
                                    const std::string &extension) {
  // result container
  std::vector<fs::path> file_paths;

  for (auto &directory_entry : fs::recursive_directory_iterator(directory)) {
    auto path = directory_entry.path().string();

    // skip directories
    if (!fs::is_regular_file(directory_entry)) continue;

    // if extension isn't defined then put all file paths from the directory
    // to the result set
    if (!extension.empty()) {
      // skip paths that don't have appropriate extension
      auto file_extension = path.substr(path.find_last_of(".") + 1);
      if (file_extension != extension) continue;
    }

    file_paths.emplace_back(path);

    // skip paths that don't have appropriate extension
    auto file_extension = path.substr(path.find_last_of(".") + 1);
    if (file_extension != extension) continue;

    // path has the right extension and can be placed in the result
    // container
    file_paths.emplace_back(path);
  }

  return file_paths;
}

std::vector<std::string> ReadLines(const fs::path &path) {
  std::vector<std::string> lines;

  if (!fs::exists(path)) return lines;

  std::ifstream stream(path.c_str());
  std::string line;
  while (std::getline(stream, line)) {
    lines.emplace_back(line);
  }

  return lines;
}

void Write(const std::string &text, const fs::path &path) {
  std::ofstream stream;
  stream.open(path.c_str());
  stream << text;
  stream.close();
}

bool EnsureDir(const fs::path &dir) {
  if (fs::exists(dir)) return true;
  std::error_code error_code;  // Just for exception suppression.
  return fs::create_directories(dir, error_code);
}

void CheckDir(const std::string &dir) {
  if (fs::exists(dir)) {
    CHECK(fs::is_directory(dir)) << "The directory path '" << dir
                                 << "' is not a directory!";
  } else {
    bool success = EnsureDir(dir);
    CHECK(success) << "Failed to create directory '" << dir << "'.";
  }
}

File::File() : fd_(-1), path_() {}

File::File(int fd, fs::path path) : fd_(fd), path_(std::move(path)) {}

File::~File() { Close(); }

File::File(File &&rhs) : fd_(rhs.fd_), path_(rhs.path_) { rhs.Release(); }

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

void File::Close() {
  if (fd_ == -1) {
    return;
  }
  if (::close(fd_) == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot close {}", path_));
  }
  Release();
}

File OpenFile(const fs::path &path, int flags, ::mode_t mode) {
  int fd = ::open(path.c_str(), flags, mode);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot open {}", path));
  }

  return File(fd, path);
}

std::experimental::optional<File> TryOpenFile(const fs::path &path, int flags,
                                              ::mode_t mode) {
  int fd = ::open(path.c_str(), flags, mode);
  return fd == -1 ? std::experimental::nullopt
                  : std::experimental::make_optional(File(fd, path));
}

File OpenFile(const File &dir, const fs::path &path, int flags, ::mode_t mode) {
  int fd = ::openat(dir.Handle(), path.c_str(), flags, mode);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot open {}", dir.Path() / path));
  }
  return File(fd, dir.Path() / path);
}

std::experimental::optional<File> TryOpenFile(const File &dir,
                                              const fs::path &path, int flags,
                                              ::mode_t mode) {
  int fd = ::openat(dir.Handle(), path.c_str(), flags, mode);
  return fd == -1
             ? std::experimental::nullopt
             : std::experimental::make_optional(File(fd, dir.Path() / path));
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
}

File OpenDir(const fs::path &path) {
  int res = ::mkdir(path.c_str(), 0777);
  if (res == 0) {
    if (path.has_parent_path()) {
      SyncDir(path.parent_path());
    }
  } else if (errno != EEXIST) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot create directory {}", path));
  }

  int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(),
                            fmt::format("cannot open directory {}", path));
  }

  return File(fd, path);
}

}  // namespace utils
