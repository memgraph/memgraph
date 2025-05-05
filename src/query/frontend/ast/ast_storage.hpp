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

#pragma once

#include "utils/typeinfo.hpp"

#include <memory>
#include <string>
#include <vector>

namespace memgraph::query {

struct LabelIx {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }
  friend bool operator==(const LabelIx &a, const LabelIx &b) { return a.ix == b.ix; }
  friend bool operator<(const LabelIx &a, const LabelIx &b) { return a.ix < b.ix; }

  std::string name;
  int64_t ix;
};

struct PropertyIx {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }
  friend bool operator==(const PropertyIx &a, const PropertyIx &b) { return a.ix == b.ix; }
  friend bool operator<(const PropertyIx &a, const PropertyIx &b) { return a.ix < b.ix; }

  std::string name;
  int64_t ix;
};

struct EdgeTypeIx {
  static const utils::TypeInfo kType;
  const utils::TypeInfo &GetTypeInfo() const { return kType; }
  friend bool operator==(const EdgeTypeIx &a, const EdgeTypeIx &b) { return a.ix == b.ix; }
  friend bool operator<(const EdgeTypeIx &a, const EdgeTypeIx &b) { return a.ix < b.ix; }

  std::string name;
  int64_t ix;
};

}  // namespace memgraph::query

namespace std {

template <>
struct hash<memgraph::query::LabelIx> {
  size_t operator()(const memgraph::query::LabelIx &label) const { return label.ix; }
};

template <>
struct hash<memgraph::query::PropertyIx> {
  size_t operator()(const memgraph::query::PropertyIx &prop) const { return prop.ix; }
};

template <>
struct hash<memgraph::query::EdgeTypeIx> {
  size_t operator()(const memgraph::query::EdgeTypeIx &edge_type) const { return edge_type.ix; }
};

}  // namespace std

namespace memgraph::query {
class Tree;

// It would be better to call this AstTree, but we already have a class Tree,
// which could be renamed to Node or AstTreeNode, but we also have a class
// called NodeAtom...
class AstStorage {
 public:
  AstStorage() = default;
  AstStorage(const AstStorage &) = delete;
  AstStorage &operator=(const AstStorage &) = delete;
  AstStorage(AstStorage &&) = default;
  AstStorage &operator=(AstStorage &&) = default;

  template <typename T, typename... Args>
  T *Create(Args &&...args) {
    T *ptr = new T(std::forward<Args>(args)...);
    std::unique_ptr<T> tmp(ptr);
    storage_.emplace_back(std::move(tmp));
    return ptr;
  }

  LabelIx GetLabelIx(const std::string &name) { return LabelIx{name, FindOrAddName(name, &labels_)}; }

  PropertyIx GetPropertyIx(const std::string &name) { return PropertyIx{name, FindOrAddName(name, &properties_)}; }

  EdgeTypeIx GetEdgeTypeIx(const std::string &name) { return EdgeTypeIx{name, FindOrAddName(name, &edge_types_)}; }

  // TODO: would be good if these were stable memory locations, then *Ix could have string_view rather than stringq
  std::vector<std::string> labels_;
  std::vector<std::string> edge_types_;
  std::vector<std::string> properties_;

  // Public only for serialization access
  std::vector<std::unique_ptr<Tree>> storage_;

 private:
  int64_t FindOrAddName(const std::string &name, std::vector<std::string> *names) {
    for (int64_t i = 0; i < names->size(); ++i) {
      if ((*names)[i] == name) {
        return i;
      }
    }
    names->push_back(name);
    return names->size() - 1;
  }
};

class Tree {
 public:
  static const utils::TypeInfo kType;
  virtual const utils::TypeInfo &GetTypeInfo() const { return kType; }

  Tree() = default;
  virtual ~Tree() = default;

  virtual Tree *Clone(AstStorage *storage) const = 0;

 private:
  friend class AstStorage;
};

}  // namespace memgraph::query
