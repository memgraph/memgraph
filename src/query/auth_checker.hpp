#pragma once

#include "query/frontend/ast/ast.hpp"

namespace query {
class AuthChecker {
 public:
  virtual bool IsUserAuthorized(const std::optional<std::string> &username,
                                const std::vector<query::AuthQuery::Privilege> &privileges) const = 0;
};

class AllowEverythingAuthChecker final : public query::AuthChecker {
  bool IsUserAuthorized(const std::optional<std::string> &username,
                        const std::vector<query::AuthQuery::Privilege> &privileges) const override {
    return true;
  }
};
}  // namespace query