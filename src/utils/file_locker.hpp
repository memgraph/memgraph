#pragma once
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <set>

#include "utils/file.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace utils {

// Class used for delaying the delation of files
class FileLockerManager {
 public:
  struct FileLockerAccess {
    explicit FileLockerAccess(FileLockerManager *manager, size_t locker_id,
                              std::unique_lock<std::mutex> lock)
        : locker_manager_{manager},
          locker_id_{locker_id},
          lock_{std::move(lock)} {}

    bool AddFile(const std::filesystem::path &path) {
      // TODO (antonio2368): Maybe return error with explanation here
      if (!std::filesystem::exists(path)) return false;
      locker_manager_->lockers_[locker_id_].emplace(path);
      return true;
    }

    // define copy and move constructor
    FileLockerAccess(const FileLockerAccess &) = delete;
    FileLockerAccess(FileLockerAccess &&) = default;
    FileLockerAccess &operator=(const FileLockerAccess &) = delete;
    FileLockerAccess &operator=(FileLockerAccess &&) = default;

    size_t LockerId() const { return locker_id_; }

    ~FileLockerAccess() {
      lock_.unlock();
      locker_manager_->DeleteFromQueue();
    }

   private:
    FileLockerManager *locker_manager_;
    size_t locker_id_;
    std::unique_lock<std::mutex> lock_;
  };

  void DeleteFile(const std::filesystem::path &path) {
    if (!lock_.try_lock()) {
      files_for_deletion.WithLock([&](auto &files) { files.emplace(path); });
      return;
    }
    DeleteOrAddToQueue(path);
    lock_.unlock();
  }

  FileLockerAccess AddLocker() {
    // If you call this in the same thread that has a locker, deadlock can occur
    std::unique_lock guard(lock_);
    const size_t current_locker_id = next_locker_id_;
    lockers_.emplace(current_locker_id, std::set<std::filesystem::path>{});
    ++next_locker_id_;
    return FileLockerAccess{this, current_locker_id, std::move(guard)};
  }

  void DeleteLocker(size_t locker_id) {
    std::lock_guard guard(lock_);
    lockers_.erase(locker_id);
    DeleteFromQueue();
  }

  explicit FileLockerManager() = default;
  // define copy move as deleted
  FileLockerManager(const FileLockerManager &) = delete;
  FileLockerManager(FileLockerManager &&) = delete;
  FileLockerManager &operator=(const FileLockerManager &) = delete;
  FileLockerManager &operator=(FileLockerManager &&) = delete;

  ~FileLockerManager() {
    // Clean the queue
    CHECK(files_for_deletion->size() == 0) << "Files weren't properly deleted";
  }

 private:
  static void DeleteFromSystem(const std::filesystem::path &path) {
    if (!utils::DeleteFile(path)) {
      LOG(WARNING) << "Couldn't delete file " << path << "!";
    }
  }

  [[nodiscard]] bool FileLocked(const std::filesystem::path &path) {
    for (const auto &[_, paths] : lockers_) {
      if (paths.count(path)) {
        return true;
      }
    }
    return false;
  }

  void DeleteOrAddToQueue(const std::filesystem::path &path) {
    if (FileLocked(path)) {
      files_for_deletion.WithLock([&](auto &files) { files.emplace(path); });
    } else {
      DeleteFromSystem(path);
    }
  }

  void DeleteFromQueue() {
    DLOG(INFO) << "Cleaning the queue";
    files_for_deletion.WithLock([&](auto &files) {
      for (auto it = files.cbegin(); it != files.cend(); ++it) {
        if (!FileLocked(*it)) {
          DeleteFromSystem(*it);
          it = files.erase(it);
        } else {
          ++it;
        }
      }
    });
  }

  std::mutex lock_;
  size_t next_locker_id_{0};
  std::map<size_t, std::set<std::filesystem::path>> lockers_;

  utils::Synchronized<std::set<std::filesystem::path>, utils::SpinLock>
      files_for_deletion;
};

}  // namespace utils
