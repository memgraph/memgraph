#include <string>

#include "query/frontend/ast/ast.hpp"
#include "spdlog/spdlog.h"

namespace memgraph::utils {
inline std::optional<std::string> GetFrameChangeId(memgraph::query::InListOperator &in_list) {
  if (in_list.expression2_->GetTypeInfo() == memgraph::query::ListLiteral::kType) {
    std::stringstream ss;
    ss << static_cast<const void *>(in_list.expression2_);
    return ss.str();
  }
  if (in_list.expression2_->GetTypeInfo() == memgraph::query::Identifier::kType) {
    auto *identifier = utils::Downcast<memgraph::query::Identifier>(in_list.expression2_);
    return identifier->name_;
  }
  return {};
};

}  // namespace memgraph::utils
