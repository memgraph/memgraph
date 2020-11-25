#pragma once

#include <cstdint>
#include <string>
#include <type_traits>

namespace storage::durability {

// The current version of snapshot and WAL encoding / decoding.
// IMPORTANT: Please bump this version for every snapshot and/or WAL format
// change!!!
const uint64_t kVersion{14};

const uint64_t kOldestSupportedVersion{14};
const uint64_t kUniqueConstraintVersion{14};

// Magic values written to the start of a snapshot/WAL file to identify it.
const std::string kSnapshotMagic{"MGsn"};
const std::string kWalMagic{"MGwl"};

static_assert(std::is_same_v<uint8_t, unsigned char>);

// Checks whether the loaded snapshot/WAL version is supported.
inline bool IsVersionSupported(uint64_t version) {
  return version >= kOldestSupportedVersion && version <= kVersion;
}

}  // namespace storage::durability
