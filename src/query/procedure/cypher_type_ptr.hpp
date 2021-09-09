#pragma once

#include <functional>
#include <memory>

namespace query::procedure {
class CypherType;
using CypherTypePtr = std::unique_ptr<CypherType, std::function<void(CypherType *)>>;
}  // namespace query::procedure