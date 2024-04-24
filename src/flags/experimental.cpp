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

#include <map>
#include <string>
#include <string_view>
#include <type_traits>

#include "flags/experimental.hpp"
#include "range/v3/all.hpp"
#include "utils/string.hpp"

#include <range/v3/view/split.hpp>
#include <range/v3/view/transform.hpp>

// Bolt server flags.
// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(
    experimental_enabled, "",
    "Experimental features to be used, comma-separated. Options [system-replication, text-search, high-availability]");
using namespace std::string_view_literals;

namespace memgraph::flags {

auto const mapping = std::map{std::pair{"system-replication"sv, Experiments::SYSTEM_REPLICATION},
                              std::pair{"text-search"sv, Experiments::TEXT_SEARCH},
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

auto ReadExperimental(std::string const &flags_experimental) -> Experiments {
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

  for (auto &&experiment : flags_experimental | rv::split(',') | rv::transform(canonicalize_string)) {
    if (auto it = mapping.find(experiment); it != mapping_end) {
      to_set |= static_cast<underlying_type>(it->second);
    }
  }

  return static_cast<Experiments>(to_set);
}

void SetExperimental(Experiments const &experiments) { ExperimentsInstance() = experiments; }

void AppendExperimental(Experiments const &experiments) {
  using underlying_type = std::underlying_type_t<Experiments>;
  auto current_state = static_cast<underlying_type>(ExperimentsInstance());
  auto new_experiments = static_cast<underlying_type>(experiments);
  auto to_set = underlying_type{};
  to_set |= current_state;
  to_set |= new_experiments;

  SetExperimental(static_cast<Experiments>(to_set));
}

}  // namespace memgraph::flags
