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

#include "cxx.hpp"
// NOTE: Error is not present under mgcxx_text_search.hpp (the way how cxx
// works), that's why cxx.hpp is required here.
#include "mgcxx_text_search.hpp"

// USAGE NOTE:
//   * Error returned from cxx calls are transformed into ::rust::Error
//     exception (that's by [cxx](https://cxx.rs/) design).
//   * All other text search functionality if located under ::mgcxx::text_search namespace.
