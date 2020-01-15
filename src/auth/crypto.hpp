#pragma once

#include <string>

namespace auth {

/// @throw AuthException if unable to encrypt the password.
const std::string EncryptPassword(const std::string &password);

/// @throw AuthException if unable to verify the password.
bool VerifyPassword(const std::string &password, const std::string &hash);

}  // namespace auth
