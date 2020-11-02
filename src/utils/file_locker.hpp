#pragma once
#include <atomic>
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
class FileRetainer {
 public:
  struct FileLockerAccess;

  struct FileLocker {
    friend FileRetainer;
    ~FileLocker() {
      std::lock_guard guard(locker_manager_->main_lock_);
      locker_manager_->lockers_.WithLock(
          [this](auto &lockers) { lockers.erase(locker_id_); });
      locker_manager_->CleanQueue();
    }

    FileLockerAccess Access() {
      return FileLockerAccess{locker_manager_, locker_id_};
    }

    FileLocker(const FileLocker &) = delete;
    FileLocker(FileLocker &&) = default;
    FileLocker &operator=(const FileLocker &) = delete;
    FileLocker &operator=(FileLocker &&) = default;

   private:
    explicit FileLocker(FileRetainer *manager, size_t locker_id)
        : locker_manager_{manager}, locker_id_{locker_id} {}

    FileRetainer *locker_manager_;
    size_t locker_id_;
  };

  struct FileLockerAccess {
    friend FileLocker;

    bool AddFile(const std::filesystem::path &path) {
      // TODO (antonio2368): Maybe return error with explanation here
      if (!std::filesystem::exists(path)) return false;
      locker_manager_->lockers_.WithLock(
          [&](auto &lockers) { lockers[locker_id_].emplace(path); });
      return true;
    }

    // define copy and move constructor
    FileLockerAccess(const FileLockerAccess &) = delete;
    FileLockerAccess(FileLockerAccess &&) = default;
    FileLockerAccess &operator=(const FileLockerAccess &) = delete;
    FileLockerAccess &operator=(FileLockerAccess &&) = default;

    ~FileLockerAccess() { locker_manager_->CleanQueue(); }

   private:
    explicit FileLockerAccess(FileRetainer *manager, size_t locker_id)
        : locker_manager_{manager},
          locker_id_{locker_id},
          lock_{manager->main_lock_} {}

    FileRetainer *locker_manager_;
    size_t locker_id_;
    std::unique_lock<utils::SpinLock> lock_;
  };

  void DeleteFile(const std::filesystem::path &path) {
    if (!main_lock_.try_lock()) {
      files_for_deletion.WithLock([&](auto &files) { files.emplace(path); });
      return;
    }
    DeleteOrAddToQueue(path);
    main_lock_.unlock();
  }

  FileLocker AddLocker() {
    const size_t current_locker_id = next_locker_id_.fetch_add(1);
    lockers_.WithLock([&](auto &lockers) {
      lockers.emplace(current_locker_id, std::set<std::filesystem::path>{});
    });
    return FileLocker{this, current_locker_id};
  }

  explicit FileRetainer() = default;
  // define copy move as deleted
  FileRetainer(const FileRetainer &) = delete;
  FileRetainer(FileRetainer &&) = delete;
  FileRetainer &operator=(const FileRetainer &) = delete;
  FileRetainer &operator=(FileRetainer &&) = delete;

  ~FileRetainer() {
    // Clean the queue
    CHECK(files_for_deletion->empty()) << "Files weren't properly deleted";
  }

 private:
  static void DeleteFromSystem(const std::filesystem::path &path) {
    if (!utils::DeleteFile(path)) {
      LOG(WARNING) << "Couldn't delete file " << path << "!";
    }
  }

  [[nodiscard]] bool FileLocked(const std::filesystem::path &path) {
    return lockers_.WithLock([&](auto &lockers) {
      for (const auto &[_, paths] : lockers) {
        if (paths.count(path)) {
          return true;
        }
      }
      return false;
    });
  }

  void DeleteOrAddToQueue(const std::filesystem::path &path) {
    if (FileLocked(path)) {
      files_for_deletion.WithLock([&](auto &files) { files.emplace(path); });
    } else {
      DeleteFromSystem(path);
    }
  }

  void CleanQueue() {
    files_for_deletion.WithLock([&](auto &files) {
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

  utils::SpinLock main_lock_;
  std::atomic<size_t> next_locker_id_{0};
  utils::Synchronized<std::map<size_t, std::set<std::filesystem::path>>,
                      utils::SpinLock>
      lockers_;

  utils::Synchronized<std::set<std::filesystem::path>, utils::SpinLock>
      files_for_deletion;
};

}  // namespace utils
