// Copyright 2023 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include <iomanip>
#include <sstream>

#include "auth/crypto.hpp"

#include <libbcrypt/bcrypt.h>
#include <openssl/sha.h>

#include "auth/exceptions.hpp"

namespace memgraph::auth {
const std::string EncryptPassword(const std::string &password) {
  // char salt[BCRYPT_HASHSIZE];
  // char hash[BCRYPT_HASHSIZE];

  // // We use `-1` as the workfactor for `bcrypt_gensalt` to let it fall back to
  // // its default value of `12`. Increasing the workfactor increases the time
  // // needed to generate the salt.
  // if (bcrypt_gensalt(-1, salt) != 0) {
  //   throw AuthException("Couldn't generate hashing salt!");
  // }

  // if (bcrypt_hashpw(password.c_str(), salt, hash) != 0) {
  //   throw AuthException("Couldn't hash password!");
  // }

  // return std::string(hash);

  unsigned char hash[SHA256_DIGEST_LENGTH];

  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, password.c_str(), password.size());
  SHA256_Final(hash, &sha256);

  std::stringstream ss;

  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
  }
  return ss.str();
}

bool VerifyPassword(const std::string &password, const std::string &hash) {
  // int ret = bcrypt_checkpw(password.c_str(), hash.c_str());
  // if (ret == -1) {
  //   throw AuthException("Couldn't check password!");
  // }
  // return ret == 0;

  auto password_hash = EncryptPassword(password);
  return password_hash == hash;
}

}  // namespace memgraph::auth
