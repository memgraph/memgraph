// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "build_info.hpp"

namespace memgraph::utils {

BuildInfo GetBuildInfo() {
#ifdef CMAKE_BUILD_TYPE_NAME
  constexpr const char *build_info_name = CMAKE_BUILD_TYPE_NAME;
#else
  constexpr const char *build_info_name = "unknown";
#endif
  BuildInfo info{build_info_name};
  return info;
}

}  // namespace memgraph::utils
