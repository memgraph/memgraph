#include "utils/file.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

#include <glog/logging.h>

namespace utils {

std::vector<std::string> ReadLines(
    const std::experimental::filesystem::path &path) noexcept {
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

bool EnsureDir(const std::experimental::filesystem::path &dir) noexcept {
  std::error_code error_code;  // For exception suppression.
  if (std::experimental::filesystem::exists(dir, error_code))
    return std::experimental::filesystem::is_directory(dir, error_code);
  return std::experimental::filesystem::create_directories(dir, error_code);
}

void EnsureDirOrDie(const std::experimental::filesystem::path &dir) {
  CHECK(EnsureDir(dir)) << "Couldn't create directory '" << dir
                        << "' due to a permission issue or the path exists and "
                           "isn't a directory!";
}

bool DeleteDir(const std::experimental::filesystem::path &dir) noexcept {
  std::error_code error_code;  // For exception suppression.
  if (!std::experimental::filesystem::is_directory(dir, error_code))
    return false;
  return std::experimental::filesystem::remove_all(dir, error_code) > 0;
}

bool CopyFile(const std::experimental::filesystem::path &src,
              const std::experimental::filesystem::path &dst) noexcept {
  std::error_code error_code;  // For exception suppression.
  return std::experimental::filesystem::copy_file(src, dst, error_code);
}

LogFile::~LogFile() {
  if (IsOpen()) Close();
}

LogFile::LogFile(LogFile &&other)
    : fd_(other.fd_),
      written_since_last_sync_(other.written_since_last_sync_),
      path_(other.path_) {
  other.fd_ = -1;
  other.written_since_last_sync_ = 0;
  other.path_ = "";
}

LogFile &LogFile::operator=(LogFile &&other) {
  if (IsOpen()) Close();

  fd_ = other.fd_;
  written_since_last_sync_ = other.written_since_last_sync_;
  path_ = other.path_;

  other.fd_ = -1;
  other.written_since_last_sync_ = 0;
  other.path_ = "";

  return *this;
}

void LogFile::Open(const std::experimental::filesystem::path &path) {
  CHECK(!IsOpen())
      << "While trying to open " << path
      << " for writing the database used a handle that already has " << path_
      << " opened in it!";

  path_ = path;
  written_since_last_sync_ = 0;

  while (true) {
    // The permissions are set to ((rw-r-----) & ~umask)
    fd_ = open(path_.c_str(), O_WRONLY | O_CLOEXEC | O_CREAT | O_APPEND, 0640);
    if (fd_ == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are fatal errors and are handled in the CHECK
      // below.
      break;
    }
  }

  CHECK(fd_ != -1) << "While trying to open " << path_
                   << " for writing an error occurred: " << strerror(errno)
                   << " (" << errno << ").";
}

bool LogFile::IsOpen() const { return fd_ != -1; }

const std::experimental::filesystem::path &LogFile::path() const {
  return path_;
}

void LogFile::Write(const char *data, size_t size) {
  while (size > 0) {
    auto written = write(fd_, data, size);
    if (written == -1 && errno == EINTR) {
      continue;
    }

    CHECK(written > 0)
        << "While trying to write to " << path_
        << " an error occurred: " << strerror(errno) << " (" << errno
        << "). Possibly " << size
        << " bytes of data were lost from this call and possibly "
        << written_since_last_sync_ << " bytes were lost from previous calls.";

    size -= written;
    data += written;
    written_since_last_sync_ += written;
  }
}

void LogFile::Write(const uint8_t *data, size_t size) {
  Write(reinterpret_cast<const char *>(data), size);
}
void LogFile::Write(const std::string &data) {
  Write(data.data(), data.size());
}

void LogFile::Sync() {
  int ret = 0;
  while (true) {
    ret = fsync(fd_);
    if (ret == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are fatal errors and are handled in the CHECK
      // below.
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
  CHECK(ret == 0) << "While trying to sync " << path_
                  << " an error occurred: " << strerror(errno) << " (" << errno
                  << "). Possibly " << written_since_last_sync_
                  << " bytes from previous write calls were lost.";

  // Reset the counter.
  written_since_last_sync_ = 0;
}

void LogFile::Close() {
  int ret = 0;
  while (true) {
    ret = close(fd_);
    if (ret == -1 && errno == EINTR) {
      // The call was interrupted, try again...
      continue;
    } else {
      // All other possible errors are fatal errors and are handled in the CHECK
      // below.
      break;
    }
  }

  CHECK(ret == 0) << "While trying to close " << path_
                  << " an error occurred: " << strerror(errno) << " (" << errno
                  << "). Possibly " << written_since_last_sync_
                  << " bytes from previous write calls were lost.";

  fd_ = -1;
  written_since_last_sync_ = 0;
  path_ = "";
}

}  // namespace utils
