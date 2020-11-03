#include "utils/file_locker.hpp"

namespace utils {

namespace {
void DeleteFromSystem(const std::filesystem::path &path) {
  if (!utils::DeleteFile(path)) {
    LOG(WARNING) << "Couldn't delete file " << path << "!";
  }
}
}  // namespace

////// FileRetainer //////
void FileRetainer::DeleteFile(const std::filesystem::path &path) {
  if (active_accessors_.load()) {
    files_for_deletion_.WithLock([&](auto &files) { files.emplace(path); });
    return;
  }
  std::unique_lock guard(main_lock_);
  DeleteOrAddToQueue(path);
}

FileRetainer::FileLocker FileRetainer::AddLocker() {
  const size_t current_locker_id = next_locker_id_.fetch_add(1);
  lockers_.WithLock([&](auto &lockers) {
    lockers.emplace(current_locker_id, std::set<std::filesystem::path>{});
  });
  return FileLocker{this, current_locker_id};
}

FileRetainer::~FileRetainer() {
  CHECK(files_for_deletion_->empty()) << "Files weren't properly deleted";
}

[[nodiscard]] bool FileRetainer::FileLocked(const std::filesystem::path &path) {
  return lockers_.WithLock([&](auto &lockers) {
    for (const auto &[_, paths] : lockers) {
      if (paths.count(path)) {
        return true;
      }
    }
    return false;
  });
}

void FileRetainer::DeleteOrAddToQueue(const std::filesystem::path &path) {
  if (FileLocked(path)) {
    files_for_deletion_.WithLock([&](auto &files) { files.emplace(path); });
  } else {
    DeleteFromSystem(path);
  }
}

void FileRetainer::CleanQueue() {
  files_for_deletion_.WithLock([&](auto &files) {
    for (auto it = files.cbegin(); it != files.cend();) {
      if (!FileLocked(*it)) {
        DeleteFromSystem(*it);
        it = files.erase(it);
      } else {
        ++it;
      }
    }
  });
}

////// FileLocker //////
FileRetainer::FileLocker::~FileLocker() {
  file_retainer_->lockers_.WithLock(
      [this](auto &lockers) { lockers.erase(locker_id_); });
  std::unique_lock guard(file_retainer_->main_lock_);
  file_retainer_->CleanQueue();
}

FileRetainer::FileLockerAccessor FileRetainer::FileLocker::Access() {
  return FileLockerAccessor{file_retainer_, locker_id_};
}

////// FileLockerAccessor //////
FileRetainer::FileLockerAccessor::FileLockerAccessor(FileRetainer *retainer,
                                                     size_t locker_id)
    : file_retainer_{retainer},
      retainer_guard_{retainer->main_lock_},
      locker_id_{locker_id} {
  file_retainer_->active_accessors_.fetch_add(1);
}

bool FileRetainer::FileLockerAccessor::AddFile(
    const std::filesystem::path &path) {
  // TODO (antonio2368): Maybe return error with explanation here
  if (!std::filesystem::exists(path)) return false;
  file_retainer_->lockers_.WithLock(
      [&](auto &lockers) { lockers[locker_id_].emplace(path); });
  return true;
}
FileRetainer::FileLockerAccessor::~FileLockerAccessor() {
  file_retainer_->active_accessors_.fetch_sub(1);
}

}  // namespace utils
