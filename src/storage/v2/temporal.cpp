#include "storage/v2/temporal.hpp"

namespace storage {
TemporalData::TemporalData(TemporalType type, int64_t microseconds) : type{type}, microseconds{microseconds} {}

}  // namespace storage
