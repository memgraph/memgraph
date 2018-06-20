#include "utils/uuid.hpp"

#include <uuid/uuid.h>

namespace utils {

std::string GenerateUUID() {
  uuid_t uuid;
  char decoded[37];  // magic size from: man 2 uuid_unparse
  uuid_generate(uuid);
  uuid_unparse(uuid, decoded);
  return std::string(decoded);
}

}  // namespace utils
