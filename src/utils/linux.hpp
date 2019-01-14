#pragma once

namespace utils {

// This is the default Linux page size found on all architectures. The proper
// way to check for this constant is to call `sysconf(_SC_PAGESIZE)`, but we
// can't use that as a `constexpr`.
const uint64_t kLinuxPageSize = 4096;

}  // namespace utils
