#pragma once

#include <cstdint>

namespace memgraph::storage {

enum class StorageMode : std::uint8_t { IN_MEMORY_ANALYTICAL, IN_MEMORY_TRANSACTIONAL };

}  // namespace memgraph::storage
