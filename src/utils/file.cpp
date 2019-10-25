#include "utils/file.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <type_traits>

#include <glog/logging.h>

namespace utils {

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
  if (std::filesystem::exists(dir, error_code))
    return std::filesystem::is_directory(dir, error_code);
  return std::filesystem::create_directories(dir, error_code);
}

void EnsureDirOrDie(const std::filesystem::path &dir) {
  CHECK(EnsureDir(dir)) << "Couldn't create directory '" << dir
                        << "' due to a permission issue or the path exists and "
                           "isn't a directory!";
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

bool CopyFile(const std::filesystem::path &src,
              const std::filesystem::path &dst) noexcept {
  std::error_code error_code;  // For exception suppression.
  return std::filesystem::copy_file(src, dst, error_code);
}

bool RenamePath(const std::filesystem::path &src,
                const std::filesystem::path &dst) {
  std::error_code error_code;  // For exception suppression.
  std::filesystem::rename(src, dst, error_code);
  return !error_code;
}

static_assert(std::is_same_v<off_t, ssize_t>, "off_t must fit into ssize_t!");

InputFile::~InputFile() { Close(); }

InputFile::InputFile(InputFile &&other) noexcept
    : fd_(other.fd_), path_(std::move(other.path_)) {
  other.fd_ = -1;
}

InputFile &InputFile::operator=(InputFile &&other) noexcept {
  Close();

  fd_ = other.fd_;
  path_ = std::move(other.path_);

  other.fd_ = -1;

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

  return fd_ != -1;
}

bool InputFile::IsOpen() const { return fd_ != -1; }

const std::filesystem::path &InputFile::path() const { return path_; }

bool InputFile::Read(uint8_t *data, size_t size) {
  size_t offset = 0;

  while (size > 0) {
    auto got = read(fd_, data + offset, size);
    if (got == -1 && errno == EINTR) {
      continue;
    }

    if (got <= 0) {
      return false;
    }

    size -= got;
    offset += got;
  }

  return true;
}

bool InputFile::Peek(uint8_t *data, size_t size) {
  size_t offset = 0;

  while (size > 0) {
    auto got = read(fd_, data + offset, size);
    if (got == -1 && errno == EINTR) {
      continue;
    }

    if (got <= 0) {
      SetPosition(Position::RELATIVE_TO_CURRENT, -offset);
      return false;
    }

    size -= got;
    offset += got;
  }

  SetPosition(Position::RELATIVE_TO_CURRENT, -offset);
  return true;
}

std::optional<size_t> InputFile::GetSize() {
  auto current = GetPosition();
  if (!current) return std::nullopt;
  auto size = SetPosition(Position::RELATIVE_TO_END, 0);
  if (!size) return std::nullopt;
  if (!SetPosition(Position::SET, *current)) return std::nullopt;
  return size;
}

std::optional<size_t> InputFile::GetPosition() {
  return SetPosition(Position::RELATIVE_TO_CURRENT, 0);
}

std::optional<size_t> InputFile::SetPosition(Position position,
                                             ssize_t offset) {
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
      // All other possible errors are fatal errors and are handled in the CHECK
      // below.
      break;
    }
  }

  if (ret != 0) {
    LOG(ERROR) << "While trying to close " << path_
               << " an error occurred: " << strerror(errno) << " (" << errno
               << ").";
  }

  fd_ = -1;
}

OutputFile::~OutputFile() {
  if (IsOpen()) Close();
}

OutputFile::OutputFile(OutputFile &&other) noexcept
    : fd_(other.fd_),
      written_since_last_sync_(other.written_since_last_sync_),
      path_(std::move(other.path_)) {
  other.fd_ = -1;
  other.written_since_last_sync_ = 0;
}

OutputFile &OutputFile::operator=(OutputFile &&other) noexcept {
  if (IsOpen()) Close();

  fd_ = other.fd_;
  written_since_last_sync_ = other.written_since_last_sync_;
  path_ = std::move(other.path_);

  other.fd_ = -1;
  other.written_since_last_sync_ = 0;

  return *this;
}

void OutputFile::Open(const std::filesystem::path &path, Mode mode) {
  CHECK(!IsOpen())
      << "While trying to open " << path
      << " for writing the database used a handle that already has " << path_
      << " opened in it!";

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
      // All other possible errors are fatal errors and are handled in the CHECK
      // below.
      break;
    }
  }

  CHECK(fd_ != -1) << "While trying to open " << path_
                   << " for writing an error occurred: " << strerror(errno)
                   << " (" << errno << ").";
}

bool OutputFile::IsOpen() const { return fd_ != -1; }

const std::filesystem::path &OutputFile::path() const { return path_; }

void OutputFile::Write(const char *data, size_t size) {
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

void OutputFile::Write(const uint8_t *data, size_t size) {
  Write(reinterpret_cast<const char *>(data), size);
}
void OutputFile::Write(const std::string_view &data) {
  Write(data.data(), data.size());
}

size_t OutputFile::GetPosition() {
  return SetPosition(Position::RELATIVE_TO_CURRENT, 0);
}

size_t OutputFile::SetPosition(Position position, ssize_t offset) {
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
    CHECK(pos >= 0) << "While trying to set the position in " << path_
                    << " an error occurred: " << strerror(errno) << " ("
                    << errno << ").";
    return pos;
  }
}

void OutputFile::Sync() {
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

void OutputFile::Close() noexcept {
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
}

}  // namespace utils
