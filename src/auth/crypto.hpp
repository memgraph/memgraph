#pragma once

#include <string>

namespace auth {

const std::string EncryptPassword(const std::string &password);

bool VerifyPassword(const std::string &password, const std::string &hash);

}  // namespace auth
