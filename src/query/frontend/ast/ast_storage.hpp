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

#include <boost/unordered/unordered_flat_map.hpp>

#include "utils/typeinfo.hpp"

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
  /// What a clone has already made, so a node it reaches again is not made twice. Lives on the
  /// stack of the outermost clone, which is as long as it means anything. Looked up rather than
  /// scanned: every node is looked up once before it is made, so a scan would cost each of them a
  /// walk over all the ones before it, and a generated query can be thousands of nodes.
  struct CloneRecord {
    Tree *Find(Tree const *source) const {
      auto const made = made_.find(source);
      return made == made_.end() ? nullptr : made->second;
    }

    void Remember(Tree const *source, Tree *copy) { made_.emplace(source, copy); }

   private:
    boost::unordered_flat_map<Tree const *, Tree *> made_;
  };

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

  /// Makes the clones taken while it lives one clone, so a node reached from two of them is made
  /// once. It holds the record of what has been made, which is why it outlives none of them.
  class [[nodiscard]] CloneScope {
   public:
    explicit CloneScope(AstStorage &storage) : storage_{storage.cloning_.record == nullptr ? &storage : nullptr} {
      if (storage_ != nullptr) storage_->cloning_.record = &record_;
    }

    ~CloneScope() {
      if (storage_ != nullptr) storage_->cloning_.record = nullptr;
    }

    CloneScope(CloneScope const &) = delete;
    CloneScope(CloneScope &&) = delete;
    CloneScope &operator=(CloneScope const &) = delete;
    CloneScope &operator=(CloneScope &&) = delete;

   private:
    /// Null when a clone was already running, which leaves that one's record in place.
    AstStorage *storage_;
    CloneRecord record_;
  };

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

  /// Every node this storage owns, whether a query reaches it or not.
  std::size_t NodeCount() const { return storage_.size(); }

  // Public only for serialization access
  std::vector<std::unique_ptr<Tree>> storage_;

 private:
  friend class Tree;

  /// What `Tree::Clone` dispatches through.
  template <typename T>
    requires std::derived_from<T, Tree>
  T *Clone(T const *node) {
    CloneScope const one_clone{*this};
    if (auto *made = cloning_.record->Find(node)) return static_cast<T *>(made);
    auto *copy = node->DoClone(this);
    cloning_.record->Remember(node, copy);
    return copy;
  }

  /// Names the record of a clone running into this storage, and nothing the rest of the time. It
  /// names rather than holds: a storage outlives the clones made into it, and a record kept past
  /// its own clone would hold keys into a source that may be gone by the next one. Living in the
  /// scope that started the clone makes that a stack frame rather than a rule, and leaves a
  /// nested scope able to see that a record is already in place and let it be.
  ///
  /// A move leaves both sides naming nothing, since a storage is only handed on once the clone
  /// that filled it has finished. Saying that here rather than in a move operator is what lets
  /// the move operators stay defaulted, so a member added later is still moved.
  struct RunningClone {
    RunningClone() = default;

    RunningClone(RunningClone && /*other*/) noexcept {}

    RunningClone &operator=(RunningClone && /*other*/) noexcept {
      record = nullptr;
      return *this;
    }

    RunningClone(RunningClone const &) = delete;
    RunningClone &operator=(RunningClone const &) = delete;

    CloneRecord *record{nullptr};
  };

  RunningClone cloning_;

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

  /// Copies this node and everything it reaches into `storage`. A node reached by several paths is
  /// copied once, so the copy shares what the source shared.
  template <typename Self>
  Self *Clone(this Self const &self, AstStorage *storage) {
    return storage->Clone(&self);
  }

 protected:
  /// Makes this one node in `storage`, asking `Clone` for copies of what it holds. A copy started
  /// here rather than at `Clone` is outside the record, and takes one copy per path.
  virtual Tree *DoClone(AstStorage *storage) const = 0;

  Tree(const Tree &) = default;
  Tree(Tree &&) noexcept = default;
  Tree &operator=(const Tree &) = default;
  Tree &operator=(Tree &&) noexcept = default;

 private:
  friend class AstStorage;
};

}  // namespace memgraph::query
