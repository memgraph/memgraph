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

#pragma once

#include <filesystem>
#include <utility>

#include "auth/auth.hpp"
#include "glue/auth_handler.hpp"

// A real auth store and handler, so an interpreter test can run auth queries and auth transactions. Plug it in
// with `interpreter_context.auth = &fixture.handler`.
struct AuthQueryHandlerFixture {
  explicit AuthQueryHandlerFixture(std::filesystem::path dir) : dir{std::move(dir)} {}

  ~AuthQueryHandlerFixture() { std::filesystem::remove_all(dir); }

  AuthQueryHandlerFixture(AuthQueryHandlerFixture const &) = delete;
  AuthQueryHandlerFixture &operator=(AuthQueryHandlerFixture const &) = delete;

  std::filesystem::path dir;
  memgraph::auth::SynchedAuth auth{dir, memgraph::auth::Auth::Config{}};
  memgraph::glue::AuthQueryHandler handler{&auth};
};
