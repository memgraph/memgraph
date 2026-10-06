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

#pragma once

#include "utils/on_scope_exit.hpp"
#include "utils/typeinfo.hpp"

#include <algorithm>
#include <concepts>
#include <cstddef>
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
    Adopt(std::unique_ptr<Tree>(ptr));
    return ptr;
  }

  // Taking ownership through the base pointer keeps the vector's allocator
  // machinery out of Create, which is instantiated once per node type.
  void Adopt(std::unique_ptr<Tree> node);

  /// Copies `node` and everything it reaches into this storage. A node the source reaches by
  /// more than one path is copied once and shared again in the copy, so a shape that names a
  /// value once and means it twice does not come back as a pair that can drift apart.
  ///
  /// This is the only way to copy an AST node. The record of what has already been copied lives
  /// for as long as the outermost call and is discarded with it, so reaching a node for a second
  /// time finds the copy only while that first call is still running.
  template <typename T>
    requires std::derived_from<T, Tree>
  T *Copy(T const *node) {
    if (!node) return nullptr;
    ++copy_depth_;
    auto const finished = utils::OnScopeExit{[this] {
      if (--copy_depth_ == 0) copied_.clear();
    }};
    // A query is tens of nodes, so scanning what has been made costs less than hashing it would.
    auto const made = std::ranges::find(copied_, static_cast<Tree const *>(node), &CopiedNode::source);
    if (made != copied_.end()) return static_cast<T *>(made->copy);
    auto *copy = node->CloneImpl(this);
    copied_.emplace_back(node, copy);
    return copy;
  }

  /// Makes the copies taken while the returned object lives one copy, so a node reached from two
  /// of them is copied once. Copying a plan needs this: an operator holds expressions, and two
  /// operators can hold the same one, so the copies have to agree with each other.
  [[nodiscard]] auto CopyScope() {
    ++copy_depth_;
    return utils::OnScopeExit{[this] {
      if (--copy_depth_ == 0) copied_.clear();
    }};
  }

  LabelIx GetLabelIx(const std::string &name) { return LabelIx{name, FindOrAddName(name, &labels_)}; }

  PropertyIx GetPropertyIx(const std::string &name) { return PropertyIx{name, FindOrAddName(name, &properties_)}; }

  EdgeTypeIx GetEdgeTypeIx(const std::string &name) { return EdgeTypeIx{name, FindOrAddName(name, &edge_types_)}; }

  int64_t FindOrAddUserFunction(const std::string &name) { return FindOrAddName(name, &user_functions_); }

  int64_t FindOrAddCallProcedure(const std::string &name) { return FindOrAddName(name, &call_procedures_); }

  /// True when building this AST read the query-module registry, so facts taken from it
  /// (a procedure's result fields, graph access and required privilege; whether a function
  /// name resolves at all) are baked in here and go stale when a module is reloaded.
  bool DependsOnModules() const { return !user_functions_.empty() || !call_procedures_.empty(); }

  // TODO: would be good if these were stable memory locations, then *Ix could have string_view rather than stringq
  std::vector<std::string> labels_;
  std::vector<std::string> edge_types_;
  std::vector<std::string> properties_;
  std::vector<std::string> user_functions_;
  std::vector<std::string> call_procedures_;

  /// How many nodes this storage owns, reachable from a query or not. A storage that
  /// received only what a query reaches holds exactly as many nodes as a copy of that
  /// query does, because copying follows the same edges.
  std::size_t NodeCount() const { return storage_.size(); }

  // Public only for serialization access
  std::vector<std::unique_ptr<Tree>> storage_;

 private:
  struct CopiedNode {
    Tree const *source;
    Tree *copy;
  };

  /// What the copy in progress has already made, so a node reached again is not made twice.
  /// Only meaningful while a copy is running, which is what `copy_depth_` tracks.
  std::vector<CopiedNode> copied_;
  int copy_depth_{0};

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

  /// Copies this node and everything it reaches into `storage`, copying a node once however many
  /// paths reach it, so a shape that names a value once and means it twice does not come back as a
  /// pair that can drift apart.
  template <typename Self>
  Self *Copy(this Self const &self, AstStorage *storage) {
    return storage->Copy(&self);
  }

  /// Makes this one node in `storage` and asks for copies of what it holds. Only `AstStorage::Copy`
  /// may call it: it keeps the record that makes a node reached twice one node again, and a caller
  /// starting here would be outside that record and get one copy per path.
  virtual Tree *CloneImpl(AstStorage *storage) const = 0;

 protected:
  Tree(const Tree &) = default;
  Tree(Tree &&) noexcept = default;
  Tree &operator=(const Tree &) = default;
  Tree &operator=(Tree &&) noexcept = default;

 private:
  friend class AstStorage;
};

}  // namespace memgraph::query
