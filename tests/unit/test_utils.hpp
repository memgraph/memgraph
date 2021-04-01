#include <memory>

#include "query/procedure/mg_procedure_impl.hpp"

namespace test_utils {
using MgpValueOwningPtr = std::unique_ptr<mgp_value, void (*)(mgp_value *)>;

MgpValueOwningPtr CreateValueOwningPtr(mgp_value *value) { return MgpValueOwningPtr(value, &mgp_value_destroy); }
}  // namespace test_utils
