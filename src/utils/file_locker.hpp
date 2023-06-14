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

#pragma once
#include <atomic>
#include <deque>
#include <functional>
#include <set>
#include <shared_mutex>
#include <unordered_map>

#include "utils/file.hpp"
#include "utils/result.hpp"
#include "utils/rw_lock.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::utils {

/**
 * Helper class used for safer modifying and reading of files
 * by preventing a deletion of a file until the file is not used in any of
 * currently running threads.
 * Also, while a single thread modifies it's list of locked files, the deletion
 * of ALL the files is delayed.
 *
 * Basic usage of FileRetainer consists of following parts:
 * - Defining a global FileRetainer object which is used for locking and
 *   deleting of the files.
 * - Each thread that wants to lock a single or multiple files first creates a
 *   FileLocker object.
 * - Modifying a FileLocker is only possible through the FileLockerAccessor.
 * - FileLockerAccessor prevents deletion of any file, so you can safely add
 *   multiple files to the locker with no risk of having files deleted during
 *   the process.
 * - You can also add directories to the locker which prevents deletion
 *   of ANY files in that directory.
 * - After a FileLocker or FileLockerAccessor is destroyed, FileRetainer scans
 *   the list of the files that wait to be deleted, and deletes all the files
 *   that are not inside any of currently present lockers.
 *
 * e.g.
 * FileRetainer file_retainer;
 * std::filesystem::path file1;
 * std::filesystem::path file2;
 *
 * void Foo() {
 *   // I want to lock a list of files
 *   // Create a locker
 *   auto locker = file_retainer.AddLocker();
 *   {
 *     // Create accessor to the locker so you can
 *     // add the files which need to be locked.
 *     // Accessor prevents deletion of any files
 *     // so you safely add multiple files in atomic way
 *     auto accessor = locker.Access();
 *     accessor.AddPath(file1);
 *     accessor.AddPath(file2);
 *   }
 *   // DO SOMETHING WITH THE FILES
 * }
 *
 * void Bar() {
 *   // I want to delete file1.
 *   file_retiner.DeleteFile(file1);
 * }
 *
 * int main() {
 * // Run Foo() and Bar() in different threads.
 * }
 *
 */
class FileRetainer {
 public:
  struct FileLockerAccessor;

  /**
   * A single locker inside the FileRetainer that contains a list
   * of files that are guarded from deletion.
   */
  struct FileLocker {
    friend FileRetainer;
    ~FileLocker();

    /**
     * Access the FileLocker so you can modify it.
     */
    FileLockerAccessor Access();

    FileLocker(const FileLocker &) = delete;
    FileLocker(FileLocker &&) = default;
    FileLocker &operator=(const FileLocker &) = delete;
    FileLocker &operator=(FileLocker &&) = default;

   private:
    explicit FileLocker(FileRetainer *retainer, size_t locker_id) : file_retainer_{retainer}, locker_id_{locker_id} {}

    FileRetainer *file_retainer_;
    size_t locker_id_;
  };

  /**
   * Accessor to the FileLocker.
   * All the modification to the FileLocker are done
   * using this struct.
   */
  struct FileLockerAccessor {
    friend FileLocker;

    enum class Error : uint8_t {
      NonexistentPath = 0,
    };

    using ret_type = utils::BasicResult<FileRetainer::FileLockerAccessor::Error, bool>;

    /**
     * Checks if a single path is in the current locker.
     */
    ret_type IsPathLocked(const std::filesystem::path &path);

    /**
     * Add a single path to the current locker.
     */
    ret_type AddPath(const std::filesystem::path &path);

    /**
     * Remove a single path form the current locker.
     */
    ret_type RemovePath(const std::filesystem::path &path);

    FileLockerAccessor(const FileLockerAccessor &) = delete;
    FileLockerAccessor(FileLockerAccessor &&) = default;
    FileLockerAccessor &operator=(const FileLockerAccessor &) = delete;
    FileLockerAccessor &operator=(FileLockerAccessor &&) = default;

    ~FileLockerAccessor();

   private:
    explicit FileLockerAccessor(FileRetainer *retainer, size_t locker_id);

    FileRetainer *file_retainer_;
    std::shared_lock<utils::RWLock> retainer_guard_;
    size_t locker_id_;
  };

  /**
   * Delete a file.
   * If the file is inside any of the lockers or some thread is modifying
   * any of the lockers, the file will be deleted after all the locks are
   * lifted.
   */
  void DeleteFile(const std::filesystem::path &path);

  /**
   * Create and return a new locker.
   */
  FileLocker AddLocker();

  /**
   * Delete the files that were queued for deletion.
   * This is already called after a locker is destroyed.
   * Call this only if you want to trigger cleaning of the
   * queue before a locker is destroyed (e.g. a file was removed
   * from a locker).
   * This method CANNOT be called from a thread which has an active
   * accessor as it will produce a deadlock.
   */
  void CleanQueue();

  explicit FileRetainer() = default;
  FileRetainer(const FileRetainer &) = delete;
  FileRetainer(FileRetainer &&) = delete;
  FileRetainer &operator=(const FileRetainer &) = delete;
  FileRetainer &operator=(FileRetainer &&) = delete;

  ~FileRetainer();

 private:
  [[nodiscard]] bool FileLocked(const std::filesystem::path &path);
  void DeleteOrAddToQueue(const std::filesystem::path &path);

  utils::RWLock main_lock_{RWLock::Priority::WRITE};

  std::atomic<size_t> active_accessors_{0};
  std::atomic<size_t> next_locker_id_{0};

  class LockerEntry {
   public:
    bool LockPath(const std::filesystem::path &path);
    bool RemovePath(const std::filesystem::path &path);
    [[nodiscard]] bool LocksFile(const std::filesystem::path &path) const;

   private:
    std::set<std::filesystem::path> directories_;
    std::set<std::filesystem::path> files_;
  };

  utils::Synchronized<std::unordered_map<size_t, LockerEntry>, utils::SpinLock> lockers_;
  utils::Synchronized<std::set<std::filesystem::path>, utils::SpinLock> files_for_deletion_;
};

}  // namespace memgraph::utils
