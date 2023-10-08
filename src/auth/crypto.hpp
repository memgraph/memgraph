// Copyright 2023 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <cstdint>
#include <string>

namespace memgraph::auth {
enum class PasswordEncryptionAlgorithm : uint8_t { BCRYPT, SHA256, SHA256_MULTIPLE };

/// @throw AuthException if unable to encrypt the password.
std::string EncryptPassword(const std::string &password);

/// @throw AuthException if unable to verify the password.
bool VerifyPassword(const std::string &password, const std::string &hash);
}  // namespace memgraph::auth
