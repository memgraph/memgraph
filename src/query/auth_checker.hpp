#pragma once

#include "query/frontend/ast/ast.hpp"

namespace query {
class AuthChecker {
 public:
  virtual bool IsUserAuthorized(const std::optional<std::string> &username,
                                const std::vector<query::AuthQuery::Privilege> &privileges) const = 0;
};
}  // namespace query