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

#include "gflags/gflags.h"

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
DECLARE_string(sso_identity_provider);
DECLARE_string(sso_protocol);
DECLARE_string(sso_saml_assertion_audience);
DECLARE_string(sso_saml_message_recipient);
DECLARE_string(sso_saml_identity_provider_id);
DECLARE_string(sso_saml_private_decryption_key);
DECLARE_bool(sso_saml_require_encryption);
DECLARE_bool(sso_saml_require_signature_validation);
DECLARE_bool(sso_saml_use_NameID);
DECLARE_string(sso_saml_username_attribute);
DECLARE_string(sso_saml_x509_certificate);
DECLARE_string(sso_saml_role_mapping);
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)
