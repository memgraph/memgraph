// Copyright 2021 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#pragma once

#include <string>

namespace auth {

/// @throw AuthException if unable to encrypt the password.
const std::string EncryptPassword(const std::string &password);

/// @throw AuthException if unable to verify the password.
bool VerifyPassword(const std::string &password, const std::string &hash);

}  // namespace auth
