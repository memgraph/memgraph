// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/file_owner.hpp"

#include <pwd.h>
#include <sys/stat.h>

#include <fmt/format.h>

namespace memgraph::utils {

std::string UsernameFor(uid_t uid) {
  auto const *info = getpwuid(uid);
  if (info == nullptr) return std::to_string(uid);
  return info->pw_name;
}

std::optional<uid_t> OwnerOf(std::filesystem::path const &path) {
  struct stat statbuf{};
  if (stat(path.c_str(), &statbuf) != 0) return std::nullopt;
  return statbuf.st_uid;
}

std::optional<std::string> OwnerMismatchFact(std::filesystem::path const &path, uid_t process_euid, uid_t path_owner) {
  if (process_euid == path_owner) return std::nullopt;
  return fmt::format("The process is running as user {}, but '{}' is owned by user {}.",
                     UsernameFor(process_euid),
                     path.string(),
                     UsernameFor(path_owner));
}

}  // namespace memgraph::utils
