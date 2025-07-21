// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/context.hpp"

#include "query/db_accessor.hpp"

namespace memgraph::query {

std::vector<storage::PropertyId> NamesToProperties(std::vector<std::string> const &property_names, DbAccessor *dba) {
  std::vector<storage::PropertyId> properties;
  properties.reserve(property_names.size());
  for (const auto &name : property_names) {
    properties.push_back(dba->NameToProperty(name));
  }
  return properties;
}

std::vector<storage::LabelId> NamesToLabels(std::vector<std::string> const &label_names, DbAccessor *dba) {
  std::vector<storage::LabelId> labels;
  labels.reserve(label_names.size());
  for (const auto &name : label_names) {
    labels.push_back(dba->NameToLabel(name));
  }
  return labels;
}

auto ExecutionContext::commit_args() -> storage::CommitArgs {
  return storage::CommitArgs::make_main(protector->clone());
}

}  // namespace memgraph::query
