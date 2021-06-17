#pragma once

#include <cstdint>

namespace storage {

enum class IsolationLevel : std::uint8_t { SNAPSHOT_ISOLATION, READ_COMMITTED, READ_UNCOMMITTED };

}  // namespace storage
