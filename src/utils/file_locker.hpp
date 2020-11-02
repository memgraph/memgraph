#pragma once
#include <atomic>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>

#include "utils/file.hpp"
#include "utils/rw_lock.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace utils {

/**
 * Helper class used for safer modifying and reading of files
 * by preventing a deletion of a file until the file is not used in any of
 * currently running threads.
 * Also, while a single thread modyifies it's list of locked files, the deletion
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
 *     // Accesor prevents deletion of any files
 *     // so you safely add multiple files in atomic way
 *     auto accessor = locker.Access();
 *     accessor.AddFile(file1);
 *     accessor.AddFile(file2);
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
    explicit FileLocker(FileRetainer *retainer, size_t locker_id)
        : file_retainer_{retainer}, locker_id_{locker_id} {}

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

    /**
     * Add a single file to the current locker.
     */
    bool AddFile(const std::filesystem::path &path);

    FileLockerAccessor(const FileLockerAccessor &) = delete;
    FileLockerAccessor(FileLockerAccessor &&) = default;
    FileLockerAccessor &operator=(const FileLockerAccessor &) = delete;
    FileLockerAccessor &operator=(FileLockerAccessor &&) = default;

    ~FileLockerAccessor();

   private:
    explicit FileLockerAccessor(FileRetainer *retainer, size_t locker_id);

    FileRetainer *file_retainer_;
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

  explicit FileRetainer() = default;
  FileRetainer(const FileRetainer &) = delete;
  FileRetainer(FileRetainer &&) = delete;
  FileRetainer &operator=(const FileRetainer &) = delete;
  FileRetainer &operator=(FileRetainer &&) = delete;

  ~FileRetainer();

 private:
  [[nodiscard]] bool FileLocked(const std::filesystem::path &path);
  void AddToQueue(const std::filesystem::path &path);
  void DeleteOrAddToQueue(const std::filesystem::path &path);
  void CleanQueue();

  void Lock() {
    while (spin_lock_.test_and_set(std::memory_order_acquire))
      ;
  }

  void Unlock() { spin_lock_.clear(std::memory_order_release); }

  void WaitForActiveLockerAccessors() {
    while (active_locker_accessors_.load(std::memory_order_acquire) > 0)
      ;
  }

  void LockForDelete() {
    Lock();
    WaitForActiveLockerAccessors();
  }

  std::atomic_flag spin_lock_ = ATOMIC_FLAG_INIT;
  std::atomic<size_t> active_locker_accessors_;

  std::atomic<size_t> next_locker_id_{0};
  utils::Synchronized<std::map<size_t, std::set<std::filesystem::path>>,
                      utils::SpinLock>
      lockers_;

  utils::Synchronized<std::set<std::filesystem::path>, utils::SpinLock>
      files_for_deletion;
};

}  // namespace utils
