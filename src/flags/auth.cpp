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

#include <filesystem>

#include "flags/auth.hpp"
#include "glue/auth_global.hpp"
#include "utils/exceptions.hpp"
#include "utils/flag_validation.hpp"
#include "utils/string.hpp"

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables,misc-unused-parameters)
DEFINE_VALIDATED_string(
    auth_module_mappings, "",
    "Associates auth schemes to external modules. A mapping is structured as follows: \"<scheme>:<absolute path>\", "
    "and individual entries are separated with \";\". If the mapping contains whitespace, enclose all of it inside "
    "quotation marks: \" \"",
    {
      if (value.empty()) return true;
      std::cout << value << "\n";
      for (const auto &mapping : memgraph::utils::Split(value, ";")) {
        const auto module_and_scheme = memgraph::utils::Split(mapping, ":");
        if (module_and_scheme.empty()) {
          throw memgraph::utils::BasicException(
              "Empty auth module mapping: each entry should follow the \"auth_scheme:module_path\" syntax, e.g. "
              "\"saml-entra-id:usr/lib/saml.py\"!");
        }
        const auto scheme_name = std::string{memgraph::utils::Trim(module_and_scheme[0])};

        const auto n_values_provided = module_and_scheme.size();
        const auto use_default = n_values_provided == 1 && DEFAULT_SSO_MAPPINGS.contains(scheme_name);
        if (n_values_provided != 2 && !use_default) {
          throw memgraph::utils::BasicException(
              "Entries in the auth module mapping follow the \"auth_scheme:module_path\" syntax, e.g. "
              "\"saml-entra-id:usr/lib/saml.py\"!");
        }
        auto module_path =
            use_default ? DEFAULT_SSO_MAPPINGS.at(scheme_name) : memgraph::utils::Trim(module_and_scheme[1]);
        auto module_file = std::filesystem::status(module_path);
        if (!std::filesystem::is_regular_file(module_file)) {
          std::cerr << "The auth module path doesn’t exist or isn’t a file!\n";
          return false;
        }
      }
      return true;
    });

DEFINE_VALIDATED_int32(auth_module_timeout_ms, 10000,
                       "Timeout (in milliseconds) used when waiting for a response from the auth module.",
                       FLAG_IN_RANGE(100, 1800000));

DEFINE_string(auth_user_or_role_name_regex, memgraph::glue::kDefaultUserRoleRegex.data(),
              "Set to the regular expression that each user or role name must fulfill.");

DEFINE_bool(auth_password_permit_null, true, "Set to false to disable null passwords.");

DEFINE_string(
    auth_password_strength_regex, memgraph::glue::kDefaultPasswordRegex.data(),
    "The regular expression that should be used to match the entire entered password to ensure its strength.");
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables,misc-unused-parameters)
