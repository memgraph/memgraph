// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "auth/models.hpp"
#include "auth/repository.hpp"

namespace memgraph::auth::rules {

/// Auth's read model, as plain functions over a repository.
///
/// Each takes the store to read from, so none of them can tell whether they are reading durable state or a
/// transaction's buffered overlay. Nothing here touches auth modules, the permission-cache epoch, resource
/// monitoring or replication: those are side effects, and they stay with `Auth`.

/// Resolves a user's roles, global then per-database, and attaches them. Reads the link entries.
void LinkUser(Repository const &repo, User &user);

std::optional<User> GetUser(Repository const &repo, std::string_view username);

std::optional<Role> GetRole(Repository const &repo, std::string_view rolename);

std::vector<User> AllUsers(Repository const &repo);

std::vector<std::string> AllUsernames(Repository const &repo);

std::vector<Role> AllRoles(Repository const &repo);

std::vector<std::string> AllRolenames(Repository const &repo);

std::vector<User> AllUsersForRole(Repository const &repo, std::string_view rolename);

std::vector<std::string> AllUsernamesForRole(Repository const &repo, std::string_view rolename);

bool HasUser(Repository const &repo, std::string_view username);

bool HasRole(Repository const &repo, std::string_view rolename);

bool HasUsers(Repository const &repo);

}  // namespace memgraph::auth::rules
