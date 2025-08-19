#pragma once

#include <cstdint>

namespace memgraph::storage {

enum StorageAccessType : uint8_t {
  NO_ACCESS = 0,  // Modifies nothing in the storage
  UNIQUE = 1,     // An operation that requires mutral exclusive access to the storage
  WRITE = 2,      // Writes to the data of storage
  READ = 3,       // Either reads the data of storage, or a metadata operation that doesn't require unique access
  READ_ONLY = 4,  // Ensures writers have gone
};

}  // namespace memgraph::storage