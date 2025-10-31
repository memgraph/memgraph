// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/logging.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include <regex>

std::string memgraph::logging::MaskSensitiveInformation(std::string_view input) {
  // 1) password: 'secret'
  static const std::regex re_password_colon(R"((password\s*:\s*['"])([^'"]*)(['"]))", std::regex_constants::icase);

  // 2) SET PASSWORD TO 'newpassword'
  //    also covers "PASSWORD TO ..." in the middle of the string
  static const std::regex re_password_to(R"((pas+word\s+to\s*['"])([^'"]*)(['"]))", std::regex_constants::icase);

  // 3) REPLACE 'oldpassword'
  static const std::regex re_replace(R"((re?pl?ac?e?\s*['"])([^'"]*)(['"]))", std::regex_constants::icase);

  // 4) IDENTIFIED BY '0042'
  static const std::regex re_identified_by(R"((identified\s+by\s*['"])([^'"]*)(['"]))", std::regex_constants::icase);

  // 5) SET PASSWORD FOR user_name TO 'new_password'
  //    note: pas+word → matches PASWORD, PASSWORD, PASSWORD
  static const std::regex re_password_for_to(R"((pas+word\s+for\s+\w+\s+to\s*['"])([^'"]*)(['"]))",
                                             std::regex_constants::icase);

  // 6) {'aws_access_key': 'test'}  or  aws_access_key = "test"
  static const std::regex re_aws_access_key_assign(R"((['"]?aws_access_key['"]?\s*[:=]\s*['"])([^'"]*)(['"]))",
                                                   std::regex_constants::icase);

  // 7) {'aws_secret_key': 'test1'}
  static const std::regex re_aws_secret_key_assign(R"((['"]?aws_secret_key['"]?\s*[:=]\s*['"])([^'"]*)(['"]))",
                                                   std::regex_constants::icase);

  // 8) set database setting 'aws.access_key' to 'test'
  static const std::regex re_aws_access_key_to(R"((['"]?aws[._-]?access[._-]?key['"]?\s+to\s*['"])([^'"]*)(['"]))",
                                               std::regex_constants::icase);

  // 9) set database setting 'aws.secret_key' to 'test'
  static const std::regex re_aws_secret_key_to(R"((['"]?aws[._-]?secret[._-]?key['"]?\s+to\s*['"])([^'"]*)(['"]))",
                                               std::regex_constants::icase);

  // work on a copy
  std::string s{input};

  // apply all masks
  s = std::regex_replace(s, re_password_colon, "$1****$3");
  s = std::regex_replace(s, re_password_to, "$1****$3");
  s = std::regex_replace(s, re_replace, "$1****$3");
  s = std::regex_replace(s, re_identified_by, "$1****$3");
  s = std::regex_replace(s, re_password_for_to, "$1****$3");
  s = std::regex_replace(s, re_aws_access_key_assign, "$1****$3");
  s = std::regex_replace(s, re_aws_secret_key_assign, "$1****$3");
  s = std::regex_replace(s, re_aws_access_key_to, "$1****$3");
  s = std::regex_replace(s, re_aws_secret_key_to, "$1****$3");

  return s;
}

void memgraph::logging::AssertFailed(std::source_location const loc, char const *expr, std::string const &message) {
  spdlog::critical(
      "\nAssertion failed in file {} at line {}."
      "\n\tExpression: '{}'"
      "{}",
      loc.file_name(), loc.line(), expr, !message.empty() ? fmt::format("\n\tMessage: '{}'", message) : "");
  std::terminate();
}

void memgraph::logging::RedirectToStderr() { spdlog::set_default_logger(spdlog::stderr_color_mt("stderr")); }
