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

#include <atomic>
#include <optional>
#include <thread>
#include "utils/skip_list.hpp"

struct ObjT {
  int key{};
  int value{};
  bool deleted = false;
  friend bool operator==(ObjT const &lhs, ObjT const &rhs) { return lhs.key == rhs.key; }
  friend bool operator<(ObjT const &lhs, ObjT const &rhs) { return lhs.key < rhs.key; }
};

struct IndexEntry {
  ObjT *ref;
  friend auto operator<=>(IndexEntry const &, IndexEntry const &) = default;
};

namespace mu = memgraph::utils;

int main() {
  auto objects = mu::SkipList<ObjT>{};
  auto index = mu::SkipList<IndexEntry>{};  // TODO: SL wrapper that referres to another SLType

  // A background transaction which will always scan the index
  auto some_transaction = std::jthread{[&](std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
      auto pin_object_acc = objects.access();
      std::this_thread::sleep_for(std::chrono::microseconds(200));  // simulate OS context switch
      auto index_acc = index.access();             // SL which reference entries in another SL MUST pin that SL
      for (auto const &index_entry : index_acc) {  // skip list iteration will be safe
        std::this_thread::sleep_for(std::chrono::microseconds(300));  // simulate OS context switch
        if (!index_entry.ref->deleted)  // DANGER we refer to object in different skip list
          ++std::atomic_ref{index_entry.ref->value};
      }
    }
  }};

  auto some_transaction2 = std::jthread{[&](std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
      auto pin_object_acc = objects.access();
      std::this_thread::sleep_for(std::chrono::microseconds(500));  // simulate OS context switch
      auto index_acc = index.access();             // SL which reference entries in another SL MUST pin that SL
      for (auto const &index_entry : index_acc) {  // skip list iteration will be safe
        std::this_thread::sleep_for(std::chrono::microseconds(11000));  // simulate OS context switch
        if (!index_entry.ref->deleted)  // DANGER we refer to object in different skip list
          ++std::atomic_ref{index_entry.ref->value};
      }
    }
  }};
  // 2 	3 	5 	7 	11 	13 	17 	19 	23 	29 	31 	37 	41 	43 	47
  // 53

  while (true) {
    ObjT *indexed_thing = nullptr;
    std::optional<mu::SkipList<ObjT>::Accessor> defer_acc;
    {
      // Make new entry
      auto obj_acc = objects.access();
      auto [it, _] = obj_acc.insert({.key = 42});
      indexed_thing = &*it;
      // Add to index
      auto index_acc = index.access();
      index_acc.insert({.ref = indexed_thing});
    }
    {
      auto obj_acc = objects.access();                               //------ ACCESS ID HERE
      std::this_thread::sleep_for(std::chrono::microseconds(1300));  // simulate OS context switch
      // "delete" the entry... ie. pass to GC
      indexed_thing->deleted = true;

      // Also unindex + un constraint

      obj_acc.remove(*indexed_thing);  // will unlink from objects
                                       //      defer_acc.emplace(std::move(obj_acc));
    }
    std::this_thread::sleep_for(std::chrono::microseconds(1700));  // simulate OS context switch
    /// background GC runs
    {  // index cleanup
      auto index_acc = index.access();
      // timestamp based
      index_acc.remove(IndexEntry{.ref = indexed_thing});  // will unlink from index (not deleted may still be accessed)
    }
    {  // remove object
       //     defer_acc.reset();
    }
    std::this_thread::sleep_for(std::chrono::microseconds(1900));  // simulate OS contex
    index.run_gc();  // index GC (some transaction may have an accessor which prevents removal)
    std::this_thread::sleep_for(std::chrono::microseconds(2300));  // simulate OS contex //// <---------------------
    objects.run_gc();  // object GC (now not safe to deref objects in index)  // FREE OBJECT
    std::this_thread::sleep_for(std::chrono::microseconds(2900));  // simulate OS contex
  }
}

//  U SLAO SLAI II D IA
