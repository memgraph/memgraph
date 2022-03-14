// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/file_locker.hpp"
#include <filesystem>

namespace memgraph::utils {

namespace {
void DeleteFromSystem(const std::filesystem::path &path) {
  if (!utils::DeleteFile(path)) {
    spdlog::warn("Couldn't delete file {}!", path);
  }
}
}  // namespace

////// FileRetainer //////
void FileRetainer::DeleteFile(const std::filesystem::path &path) {
  if (!std::filesystem::exists(path)) {
    spdlog::info("File {} doesn't exist.", path);
    return;
  }

  auto absolute_path = std::filesystem::absolute(path);
  if (active_accessors_.load()) {
    files_for_deletion_.WithLock([&](auto &files) { files.emplace(std::move(absolute_path)); });
    return;
  }
  std::unique_lock guard(main_lock_);
  DeleteOrAddToQueue(absolute_path);
}

FileRetainer::FileLocker FileRetainer::AddLocker() {
  const size_t current_locker_id = next_locker_id_.fetch_add(1);
  lockers_.WithLock([&](auto &lockers) { lockers.emplace(current_locker_id, LockerEntry{}); });
  return FileLocker{this, current_locker_id};
}

FileRetainer::~FileRetainer() { MG_ASSERT(files_for_deletion_->empty(), "Files weren't properly deleted"); }

[[nodiscard]] bool FileRetainer::FileLocked(const std::filesystem::path &path) {
  return lockers_.WithLock([&](auto &lockers) {
    for (const auto &[_, locker_entry] : lockers) {
      if (locker_entry.LocksFile(path)) {
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
  std::unique_lock guard(main_lock_);
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

////// LockerEntry //////
void FileRetainer::LockerEntry::LockPath(const std::filesystem::path &path) {
  auto absolute_path = std::filesystem::absolute(path);
  if (std::filesystem::is_directory(absolute_path)) {
    directories_.emplace(std::move(absolute_path));
    return;
  }
  files_.emplace(std::move(absolute_path));
}

bool FileRetainer::LockerEntry::RemovePath(const std::filesystem::path &path) {
  auto absolute_path = std::filesystem::absolute(path);
  if (std::filesystem::is_directory(absolute_path)) {
    return directories_.erase(absolute_path);
  }

  return files_.erase(absolute_path);
}

bool FileRetainer::LockerEntry::LocksFile(const std::filesystem::path &path) const {
  MG_ASSERT(path.is_absolute(), "Absolute path needed to check if the file is locked.");

  if (files_.count(path)) {
    return true;
  }

  for (const auto &directory : directories_) {
    auto directory_path_it = directory.begin();
    auto path_it = path.begin();
    while (directory_path_it != directory.end() && path_it != path.end()) {
      if (*directory_path_it != *path_it) {
        break;
      }
      ++directory_path_it;
      ++path_it;
    }

    if (directory_path_it == directory.end()) {
      return true;
    }
  }

  return false;
}

////// FileLocker //////
FileRetainer::FileLocker::~FileLocker() {
  file_retainer_->lockers_.WithLock([this](auto &lockers) { lockers.erase(locker_id_); });
  file_retainer_->CleanQueue();
}

FileRetainer::FileLockerAccessor FileRetainer::FileLocker::Access() {
  return FileLockerAccessor{file_retainer_, locker_id_};
}

////// FileLockerAccessor //////
FileRetainer::FileLockerAccessor::FileLockerAccessor(FileRetainer *retainer, size_t locker_id)
    : file_retainer_{retainer}, retainer_guard_{retainer->main_lock_}, locker_id_{locker_id} {
  file_retainer_->active_accessors_.fetch_add(1);
}

bool FileRetainer::FileLockerAccessor::AddPath(const std::filesystem::path &path) {
  if (!std::filesystem::exists(path)) return false;
  file_retainer_->lockers_.WithLock([&](auto &lockers) { lockers[locker_id_].LockPath(path); });
  return true;
}

bool FileRetainer::FileLockerAccessor::RemovePath(const std::filesystem::path &path) {
  return file_retainer_->lockers_.WithLock([&](auto &lockers) { return lockers[locker_id_].RemovePath(path); });
}

FileRetainer::FileLockerAccessor::~FileLockerAccessor() { file_retainer_->active_accessors_.fetch_sub(1); }

}  // namespace memgraph::utils
