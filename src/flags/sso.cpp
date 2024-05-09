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

#include "sso.hpp"

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(sso_identity_provider, "", "The identity provider used for SSO.");
DEFINE_string(sso_protocol, "", "The protocol used for SSO.");
DEFINE_string(sso_saml_assertion_audience, "", "The URI of the SAML response assertion audience.");
DEFINE_string(sso_saml_message_recipient, "", "The recipient for a wrapped encryption key in SAML responses.");
DEFINE_string(sso_saml_identity_provider_id, "", "The identifier URI of the identity provider for SAML SSO.");
DEFINE_string(sso_saml_private_decryption_key, "", "The path to the private key intended for the SAML SSO module.");
DEFINE_bool(sso_saml_require_encryption, false, "Require that SAML responses for SSO are encrypted.");
DEFINE_bool(sso_saml_require_signature_validation, false, "Require signature validation in SAML responses for SSO.");
DEFINE_bool(sso_saml_use_NameID, false, "Retrieve the username from the NameID SAML identifier.");
DEFINE_string(sso_saml_username_attribute, "", "The name of of the SAML response attribute containing the username.");
DEFINE_string(sso_saml_x509_certificate, "", "The path to the X.509 certificate intended for the SAML SSO module.");
DEFINE_string(sso_saml_role_mapping, "",
              "The authorization mapping from the identity provider roles to user-defined Memgraph-internal roles.");
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)
