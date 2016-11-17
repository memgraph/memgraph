#pragma once

#include <cassert>
#include <iostream>
#include <map>
#include <map>
#include <type_traits>
#include <utility>
#include <vector>

#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "communication/bolt/v1/serialization/record_stream.hpp"
#include "database/db.hpp"
#include "database/db.hpp"
#include "database/db_accessor.hpp"
#include "database/db_accessor.hpp"
#include "io/network/socket.hpp"
#include "mvcc/id.hpp"
#include "query/util.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/indexes/index_definition.hpp"
#include "storage/label/label.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/property.hpp"
#include "utils/border.hpp"
#include "utils/iterator/iterator.hpp"
#include "utils/iterator/iterator.hpp"
#include "utils/option_ptr.hpp"
#include "utils/reference_wrapper.hpp"

using namespace std;

namespace hardcode
{
using query_functions_t =
    std::map<uint64_t, std::function<bool(properties_t &&)>>;
}
