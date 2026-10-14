// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "glue/websocket_auth.hpp"

#include <string>

#include "dbms/constants.hpp"
#include "query/query_user.hpp"

namespace memgraph::glue {

bool SafeAuth::Authenticate(const std::string &username, const std::string &password) const {
  if (auto result = auth::Authenticate(*auth_, username, password)) {
    user_or_role_.emplace(auth_, std::move(*result));
  } else {
    user_or_role_.reset();
  }
  return user_or_role_.has_value();
}

bool SafeAuth::HasWebsocketPermission() const {
  // NOTE: websocket authenticates only if there is a user, so no need to check if access controlled
  if (!user_or_role_) return false;
  // An inaccessible main db throws; the websocket session reports it as a failed authentication.
#ifdef MG_ENTERPRISE
  const std::string db_name = user_or_role_->GetDefaultDB();
#else
  const std::string db_name{dbms::kDefaultDB};
#endif
  return user_or_role_->IsAuthorized({query::AuthQuery::Privilege::WEBSOCKET}, db_name, &query::up_to_date_policy);
}

bool SafeAuth::AccessControlled() const { return auth_->ReadLock()->AccessControlled(); }

}  // namespace memgraph::glue
