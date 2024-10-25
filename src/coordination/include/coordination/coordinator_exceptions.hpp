// Copyright 2024 Memgraph Ltd.
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

#ifdef MG_ENTERPRISE

#include "utils/exceptions.hpp"

namespace memgraph::coordination {
class CoordinatorRegisterInstanceException final : public utils::BasicException {
 public:
  explicit CoordinatorRegisterInstanceException(const std::string_view what) noexcept
      : BasicException("Failed to create instance: " + std::string(what)) {}

  template <class... Args>
  explicit CoordinatorRegisterInstanceException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : CoordinatorRegisterInstanceException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(CoordinatorRegisterInstanceException)
};

class RaftServerStartException final : public utils::BasicException {
 public:
  explicit RaftServerStartException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit RaftServerStartException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : RaftServerStartException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(RaftServerStartException)
};

class RaftAddServerException final : public utils::BasicException {
 public:
  explicit RaftAddServerException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit RaftAddServerException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : RaftAddServerException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(RaftAddServerException)
};

class RaftBecomeLeaderException final : public utils::BasicException {
 public:
  explicit RaftBecomeLeaderException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit RaftBecomeLeaderException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : RaftBecomeLeaderException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(RaftBecomeLeaderException)
};

class RaftCouldNotFindEntryException final : public utils::BasicException {
 public:
  explicit RaftCouldNotFindEntryException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit RaftCouldNotFindEntryException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : RaftCouldNotFindEntryException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(RaftCouldNotFindEntryException)
};

class RaftCouldNotParseFlagsException final : public utils::BasicException {
 public:
  explicit RaftCouldNotParseFlagsException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit RaftCouldNotParseFlagsException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : RaftCouldNotParseFlagsException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(RaftCouldNotParseFlagsException)
};

class InvalidRoutingTableException final : public utils::BasicException {
 public:
  explicit InvalidRoutingTableException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit InvalidRoutingTableException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : InvalidRoutingTableException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(InvalidRoutingTableException)
};

class StoreSnapshotToDiskException final : public utils::BasicException {
 public:
  explicit StoreSnapshotToDiskException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit StoreSnapshotToDiskException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : StoreSnapshotToDiskException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(StoreSnapshotToDiskException)
};

class NoSnapshotOnDiskException final : public utils::BasicException {
 public:
  explicit NoSnapshotOnDiskException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit NoSnapshotOnDiskException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : NoSnapshotOnDiskException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(NoSnapshotOnDiskException)
};

class DeleteSnapshotFromDiskException final : public utils::BasicException {
 public:
  explicit DeleteSnapshotFromDiskException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit DeleteSnapshotFromDiskException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : DeleteSnapshotFromDiskException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(DeleteSnapshotFromDiskException)
};

class StoreClusterConfigException final : public utils::BasicException {
 public:
  explicit StoreClusterConfigException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit StoreClusterConfigException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : DeleteSnapshotFromDiskException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(StoreClusterConfigException)
};

class VersionMigrationException final : public utils::BasicException {
 public:
  explicit VersionMigrationException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit VersionMigrationException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : VersionMigrationException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(VersionMigrationException)
};

class CoordinatorStateMachineVersionMigrationException final : public utils::BasicException {
 public:
  explicit CoordinatorStateMachineVersionMigrationException(std::string_view what) noexcept : BasicException(what) {}

  template <class... Args>
  explicit CoordinatorStateMachineVersionMigrationException(fmt::format_string<Args...> fmt, Args &&...args) noexcept
      : CoordinatorStateMachineVersionMigrationException(fmt::format(fmt, std::forward<Args>(args)...)) {}

  SPECIALIZE_GET_EXCEPTION_NAME(CoordinatorStateMachineVersionMigrationException)
};

}  // namespace memgraph::coordination
#endif
