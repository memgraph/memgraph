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
#include <shared_mutex>

#include "utils/parameters.hpp"

namespace memgraph::utils {

Parameters::Parameters(std::filesystem::path storage_path) {
  std::lock_guard parameters_guard{parameters_lock_};
  storage_.emplace(std::move(storage_path));
}

std::string_view ParameterScopeToString(ParameterScope scope) {
  switch (scope) {
    case ParameterScope::GLOBAL:
      return "global";
    case ParameterScope::DATABASE:
      return "database";
    case ParameterScope::SESSION:
      return "session";
    default:
      throw utils::BasicException("Invalid parameter scope");
  }
}

bool Parameters::SetParameter(std::string_view name, std::string_view value, ParameterScope scope) {
  std::lock_guard parameters_guard{parameters_lock_};
  if (!storage_) return false;

  if (!storage_->Put(name, value)) {
    SPDLOG_ERROR("Failed to set parameter '{}' with scope '{}'", name, static_cast<int>(scope));
    return false;
  }

  SPDLOG_DEBUG("Set parameter '{}' = '{}' with scope '{}'", name, value, static_cast<int>(scope));
  return true;
}

std::optional<std::string> Parameters::GetParameter(std::string_view name, ParameterScope /*scope*/) const {
  std::shared_lock parameters_guard{parameters_lock_};
  if (!storage_) return std::nullopt;

  return storage_->Get(name);
}

bool Parameters::UnsetParameter(std::string_view name, ParameterScope scope) {
  std::lock_guard parameters_guard{parameters_lock_};
  if (!storage_) return false;

  if (!storage_->Delete(name)) {
    SPDLOG_ERROR("Failed to delete parameter '{}' with scope '{}'", name, static_cast<int>(scope));
    return false;
  }

  SPDLOG_DEBUG("Unset parameter '{}' with scope '{}'", name, static_cast<int>(scope));
  return true;
}

std::vector<ParameterInfo> Parameters::GetAllParameters(ParameterScope scope) const {
  std::shared_lock parameters_guard{parameters_lock_};
  if (!storage_) return {};

  std::vector<ParameterInfo> parameters;

  // Iterate through all stored parameters
  for (const auto &[key, value] : *storage_) {
    parameters.emplace_back(ParameterInfo{.name = key, .value = value, .scope = scope});
  }

  return parameters;
}

}  // namespace memgraph::utils
