// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "flags/experimental.hpp"
#include "range/v3/all.hpp"
#include "utils/string.hpp"

#include <map>
#include <string_view>

// Bolt server flags.
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(experimental_enabled, "",
              "Experimental features to be used, comma-separated. Options [system-replication, text-search, high-availability]");
using namespace std::string_view_literals;

namespace memgraph::flags {

auto const mapping = std::map{std::pair{"system-replication"sv, Experiments::SYSTEM_REPLICATION},
                              std::pair{"text-search"sv, Experiments::TEXT_SEARCH}},
                              std::pair{"high-availability"sv, Experiments::HIGH_AVAILABILITY}};

auto ExperimentsInstance() -> Experiments & {
  static auto instance = Experiments{};
  return instance;
}

bool AreExperimentsEnabled(Experiments experiments) {
  using t = std::underlying_type_t<Experiments>;

  auto actual = static_cast<t>(ExperimentsInstance());
  auto check = static_cast<t>(experiments);

  return (actual & check) == check;
}

void InitializeExperimental() {
  namespace rv = ranges::views;

  auto const canonicalize_string = [](auto &&rng) {
    auto const is_space = [](auto c) { return c == ' '; };
    auto const to_lower = [](unsigned char c) { return std::tolower(c); };

    return rng | rv::drop_while(is_space) | rv::take_while(std::not_fn(is_space)) | rv::transform(to_lower) |
           ranges::to<std::string>;
  };

  auto const mapping_end = mapping.cend();
  using underlying_type = std::underlying_type_t<Experiments>;
  auto to_set = underlying_type{};
  for (auto &&experiment : FLAGS_experimental_enabled | rv::split(',') | rv::transform(canonicalize_string)) {
    if (auto it = mapping.find(experiment); it != mapping_end) {
      to_set |= static_cast<underlying_type>(it->second);
    }
  }

  ExperimentsInstance() = static_cast<Experiments>(to_set);
}

}  // namespace memgraph::flags
