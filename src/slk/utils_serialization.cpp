// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "slk/serialization.hpp"
#include "utils/safe_string.hpp"
#include "utils/uuid.hpp"

namespace memgraph::slk {

void Save(const utils::UUID &self, Builder *builder) {
  const auto &arr = static_cast<utils::UUID::arr_t>(self);
  // Fully qualify to dispatch to the array overload, not back to this function
  ::memgraph::slk::Save(arr, builder);
}

void Load(utils::UUID *self, Reader *reader) { Load(&self->uuid, reader); }

void Save(const utils::SafeString &self, Builder *builder) {
  // str_view() returns a lock-holding RAII wrapper; operator* gives the protected string
  auto locked = self.str_view();
  Save(*locked, builder);
}

void Load(utils::SafeString *self, Reader *reader) {
  std::string str;
  Load(&str, reader);
  *self = std::move(str);
}

}  // namespace memgraph::slk
