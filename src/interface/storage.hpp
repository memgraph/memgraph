// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

// All the necessary includes from the generated thrift files has to be listed here and nowhere else in other header or
// source files.
#include "interface/gen-cpp2/StorageAsyncClient.h"

// The CHECK macro is defined in glog, but our grammar contains a function with the same name which causes copmilation
// errors.
#ifdef CHECK
#undef CHECK
#endif