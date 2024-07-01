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
#pragma once

#include <unordered_map>

#include "gflags/gflags.h"

const std::unordered_map<std::string, std::string> DEFAULT_SSO_MAPPINGS{
    {"saml-entra-id", "/usr/lib/memgraph/auth_module/saml.py"},
    {"saml-okta", "/usr/lib/memgraph/auth_module/saml.py"},
    {"oidc-entra-id", "/usr/lib/memgraph/auth_module/oidc.py"},
    {"oidc-okta", "/usr/lib/memgraph/auth_module/oidc.py"}};

// Auth flags.

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(auth_module_mappings);
DECLARE_int32(auth_module_timeout_ms);
DECLARE_string(auth_user_or_role_name_regex);
DECLARE_bool(auth_password_permit_null);
DECLARE_string(auth_password_strength_regex);
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)
