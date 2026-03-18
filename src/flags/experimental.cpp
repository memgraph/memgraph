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

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <cctype>
#include <functional>
#include <map>
#include <nlohmann/json.hpp>
#include <range/v3/algorithm/any_of.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/iterator/basic_iterator.hpp>
#include <range/v3/range/conversion.hpp>
#include <range/v3/utility/get.hpp>
#include <range/v3/view/drop_while.hpp>
#include <range/v3/view/single.hpp>
#include <range/v3/view/split.hpp>
#include <range/v3/view/take_while.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/view.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "flags/experimental.hpp"
#include "utils/flag_validation.hpp"

using namespace std::string_view_literals;
namespace rv = ranges::views;

// The mapping tables MUST be defined before DEFINE_VALIDATED_string so the
// help-string composition below sees them initialised.  Static-init order
// within a single TU is source order for dynamically-initialised objects;
// gflags' FlagRegisterer (constructed by DEFINE_VALIDATED_string further
// down) reads description as `const char*` at construction, so we point it
// at a static std::string built once from the mappings.
namespace memgraph::flags {

auto const mapping = std::map{
    std::pair{"planner-v2"sv, Experiments::PLANNER_V2},
};
auto const reverse_mapping = std::map{
    std::pair{Experiments::PLANNER_V2, "planner-v2"sv},
};
auto const config_mapping = std::map<std::string_view, Experiments>{};

}  // namespace memgraph::flags

namespace {

auto const canonicalize_string = [](auto &&rng) {
  auto const is_space = [](auto c) { return c == ' '; };
  auto const to_lower = [](unsigned char c) { return std::tolower(c); };

  return rng | rv::drop_while(is_space) | rv::take_while(std::not_fn(is_space)) | rv::transform(to_lower) |
         ranges::to<std::string>;
};

/// Compose "prefix Options [name1, name2, ...]" from a mapping table.
/// Used to keep --help text in sync with the actual recognised options
/// without hand-maintaining the list at every DEFINE_*.
template <typename Mapping>
auto BuildHelp(std::string_view prefix, Mapping const &m) -> std::string {
  std::string out{prefix};
  out += " Options [";
  bool first = true;
  for (auto const &[name, _] : m) {
    if (!first) out += ", ";
    first = false;
    out.append(name.data(), name.size());
  }
  out += "]";
  return out;
}

// Static lifetime ⇒ pointer is valid for the program's lifetime, which is
// the gflags registry's only requirement on description strings.
std::string const kEnabledHelp =
    BuildHelp("Experimental features to be used, comma-separated.", memgraph::flags::mapping);
std::string const kConfigHelp =
    BuildHelp("Experimental features to be used, JSON object.", memgraph::flags::config_mapping);

}  // namespace

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_string(experimental_enabled, "", kEnabledHelp.c_str(),
                        { return memgraph::flags::ValidExperimentalFlag(value); });

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_string(experimental_config, "", kConfigHelp.c_str(),
                        { return memgraph::flags::ValidExperimentalConfig(value); });

namespace memgraph::flags {

auto ExperimentsInstance() -> Experiments & {
  static auto instance = Experiments{};
  return instance;
}

bool AreExperimentsEnabled(Experiments experiments) {
  auto actual = std::to_underlying(ExperimentsInstance());
  auto check = std::to_underlying(experiments);

  return (actual & check) == check;
}

auto ReadExperimental(std::string const &flags_experimental) -> Experiments {
  auto const mapping_end = mapping.cend();
  using underlying_type = std::underlying_type_t<Experiments>;
  auto to_set = underlying_type{};

  for (auto &&experiment : flags_experimental | rv::split(',') | rv::transform(canonicalize_string)) {
    if (auto it = mapping.find(experiment); it != mapping_end) {
      spdlog::info(fmt::format("Experimental feature {} is enabled.", it->first));
      to_set |= std::to_underlying(it->second);
    }
  }

  return static_cast<Experiments>(to_set);
}

void SetExperimental(Experiments const &experiments) { ExperimentsInstance() = experiments; }

void AppendExperimental(Experiments const &experiments) {
  using underlying_type = std::underlying_type_t<Experiments>;
  auto current_state = std::to_underlying(ExperimentsInstance());
  auto new_experiments = std::to_underlying(experiments);
  auto to_set = underlying_type{};
  to_set |= current_state;
  to_set |= new_experiments;

  SetExperimental(static_cast<Experiments>(to_set));
}

auto ValidExperimentalFlag(std::string_view value) -> bool {
  if (value.empty()) {
    return true;
  }
  auto const mapping_end = mapping.cend();
  return !ranges::any_of(value | rv::split(',') | rv::transform(canonicalize_string),
                         [&mapping_end](auto &&experiment) { return mapping.find(experiment) == mapping_end; });
}

auto ValidExperimentalConfig(std::string_view json_config) -> bool {
  if (json_config.empty()) {
    return true;
  }

  try {
    auto json_flags = nlohmann::json::parse(json_config);
    if (!json_flags.is_object()) {
      return false;
    }

    auto const config_mapping_end = config_mapping.cend();
    for (auto const &[key, _] : json_flags.items()) {
      auto const canonical_key = canonicalize_string(key);
      if (config_mapping.find(canonical_key) == config_mapping_end) {
        return false;
      }
    }
  } catch (nlohmann::json::parse_error const &e) {
    return false;
  }

  return true;
}

auto ParseExperimentalConfig(Experiments experiment) -> nlohmann::json {
  const auto &json_config = FLAGS_experimental_config;

  if (json_config.empty()) {
    return nlohmann::json::object();
  }

  try {
    auto json_flags = nlohmann::json::parse(json_config);
    if (!json_flags.is_object()) {
      throw std::invalid_argument("Experimental config must be a JSON object.");
    }

    auto mapping_it = reverse_mapping.find(experiment);
    if (mapping_it == reverse_mapping.end()) {
      throw std::invalid_argument("Unknown experimental feature in experimental config.");
    }

    auto experiment_json_config = json_flags.find(mapping_it->second);
    if (experiment_json_config == json_flags.end()) {
      throw std::invalid_argument("Experimental feature configuration missing in JSON.");
    }

    return *experiment_json_config;

  } catch (const nlohmann::json::parse_error &e) {
    throw std::invalid_argument("Invalid experimental config: " + std::string(e.what()));
  }
}

}  // namespace memgraph::flags
