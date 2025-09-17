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

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "planner/core/egraph.hpp"
#include "planner/core/processing_context.hpp"

namespace memgraph::planner::core {

// Global flag for verbose output
static bool g_verbose = false;

// Macro for conditional verbose output
#define VERBOSE_OUT \
  if (g_verbose) std::cerr

// Simple test symbols for fuzzing
enum class FuzzSymbol : uint32_t {
  A = 0,
  B = 1,
  C = 2,
  D = 3,
  E = 4,
  F = 10,
  G = 11,
  H = 12,
  Plus = 13,
  Mul = 14,
};

struct FuzzAnalysis {};

// ============================================================================
// Validation Functions
// ============================================================================

template <typename Symbol, typename Analysis>
std::string CreateCanonicalSignature(const ENode<Symbol> &enode, EGraph<Symbol, Analysis> &egraph) {
  std::string sig = std::to_string(static_cast<uint32_t>(enode.symbol()));

  for (auto child_id : enode.children()) {
    auto canonical_child = egraph.find(child_id);
    sig += "_" + std::to_string(canonical_child);
  }

  if (enode.is_leaf()) {
    sig += "_D" + std::to_string(enode.disambiguator());
  }

  return sig;
}

template <typename Symbol, typename Analysis>
bool ValidateCanonicalClasses(EGraph<Symbol, Analysis> &egraph) {
  for (auto id : egraph.canonical_class_ids()) {
    if (egraph.find(id) != id) {
      std::cerr << "Non-canonical class ID " << id << " in canonical_class_ids()\n";
      return false;
    }
  }
  return true;
}

template <typename Symbol, typename Analysis>
bool ValidateCongruenceClosure(EGraph<Symbol, Analysis> &egraph) {
  std::unordered_map<std::string, EClassId> canonical_forms;

  for (auto const &[class_id, eclass] : egraph.canonical_classes()) {
    for (auto enode_id : eclass.nodes()) {
      std::string sig = CreateCanonicalSignature(egraph.get_enode(enode_id), egraph);

      if (canonical_forms.contains(sig)) {
        if (canonical_forms[sig] != class_id) {
          std::cerr << "Congruent e-nodes in different e-classes: " << canonical_forms[sig] << " vs " << class_id
                    << "\n";
          std::cerr << "Signature: " << sig << "\n";
          return false;
        }
      } else {
        canonical_forms[sig] = class_id;
      }
    }
  }
  return true;
}

template <typename Symbol, typename Analysis>
bool ValidateParentChildRelationships(EGraph<Symbol, Analysis> &egraph) {
  // Single pass: collect all parent->child relationships from nodes
  std::unordered_map<ENodeId, std::unordered_set<EClassId>> node_to_children;

  for (auto const &[class_id, eclass] : egraph.canonical_classes()) {
    for (auto enode_id : eclass.nodes()) {
      const auto &enode = egraph.get_enode(enode_id);
      for (auto child_id : enode.children()) {
        node_to_children[enode_id].insert(egraph.find(child_id));
      }
    }
  }

  // Now validate: each parent reference should match actual children
  for (auto const &[class_id, eclass] : egraph.canonical_classes()) {
    for (auto enode_id : eclass.parents()) {
      auto canonical_parent = egraph.find(enode_id);

      if (!egraph.has_class(canonical_parent)) {
        std::cerr << "Parent class ID " << enode_id << " canonicalizes to non-existent class " << canonical_parent
                  << "\n";
        return false;
      }

      // Check if this parent node actually references this child class
      if (auto it = node_to_children.find(enode_id); it != node_to_children.end() && !it->second.contains(class_id)) {
        std::cerr << "Parent e-node doesn't reference child e-class " << class_id << "\n";
        return false;
      }
    }
  }
  return true;
}

template <typename Symbol, typename Analysis>
bool ValidateRebuildComplete(EGraph<Symbol, Analysis> &egraph) {
  if (egraph.needs_rebuild()) {
    std::cerr << "Worklist not empty after rebuild (size: " << egraph.worklist_size() << ")\n";
    return false;
  }
  return true;
}

template <typename Symbol, typename Analysis>
bool ValidateEGraphInvariants(EGraph<Symbol, Analysis> &egraph, ProcessingContext<Symbol> &ctx) {
  try {
    return ValidateCanonicalClasses(egraph) && ValidateCongruenceClosure(egraph) &&
           ValidateParentChildRelationships(egraph) && ValidateRebuildComplete(egraph);
  } catch (const std::exception &e) {
    std::cerr << "Exception during validation: " << e.what() << "\n";
    return false;
  }
}

// ============================================================================
// Operation Execution
// ============================================================================

class FuzzerState {
 public:
  bool execute_operation(uint8_t op, const uint8_t *data, size_t &pos, size_t size) {
    operation_count++;
    VERBOSE_OUT << "\n--- Operation #" << operation_count << " ---\n";

    int operation_type = op % 5;
    const char *op_names[] = {"CREATE_LEAF", "CREATE_COMPOUND", "MERGE_CLASSES", "REBUILD", "CREATE_CONGRUENT"};
    VERBOSE_OUT << "Op: " << op_names[operation_type] << " (raw: " << (int)op << ")\n";

    switch (operation_type) {
      case 0:
        return create_leaf_node(data, pos, size);
      case 1:
        return create_compound_node(data, pos, size);
      case 2:
        return merge_classes(data, pos, size);
      case 3:
        return rebuild();
      case 4:
        return create_congruent_pattern(data, pos, size);  // NEW!
      default:
        return true;
    }
  }

  bool create_leaf_node(const uint8_t *data, size_t &pos, size_t size) {
    if (pos >= size) return true;

    uint8_t symbol = data[pos++] % 5;  // A-E
    uint64_t disambiguator = pos < size ? data[pos++] : 0;

    auto sym = static_cast<FuzzSymbol>(symbol);
    auto id = egraph.emplace(sym, disambiguator);
    created_ids.push_back(id);

    VERBOSE_OUT << "Created leaf: " << (char)('A' + symbol) << "(D" << disambiguator << ") -> " << id << "\n";
    VERBOSE_OUT << "Total classes: " << egraph.num_classes() << ", Total nodes: " << egraph.num_nodes() << "\n";
    return true;
  }

  bool create_compound_node(const uint8_t *data, size_t &pos, size_t size) {
    if (pos + 2 >= size || created_ids.empty()) return true;

    uint8_t symbol = data[pos++] % 5 + 10;       // F-Mul
    uint8_t num_children = data[pos++] % 3 + 1;  // 1-3 children

    if (pos + num_children > size) return true;

    std::vector<EClassId> children;
    for (uint8_t i = 0; i < num_children; ++i) {
      uint8_t child_idx = data[pos++] % created_ids.size();
      children.push_back(created_ids[child_idx]);
    }

    auto sym = static_cast<FuzzSymbol>(symbol);
    auto id = egraph.emplace(sym, utils::small_vector<EClassId>(children.begin(), children.end()));
    created_ids.push_back(id);
    return true;
  }

  bool merge_classes(const uint8_t *data, size_t &pos, size_t size) {
    if (size < pos + 2 || created_ids.empty()) return true;

    uint8_t idx1 = data[pos++] % created_ids.size();
    uint8_t idx2 = data[pos++] % created_ids.size();

    auto id1 = created_ids[idx1];
    auto id2 = created_ids[idx2];

    VERBOSE_OUT << "Merging class " << id1 << " with " << id2 << "\n";
    auto merged_id = egraph.merge(id1, id2);
    VERBOSE_OUT << "Merge result: " << merged_id << "\n";
    VERBOSE_OUT << "Total classes: " << egraph.num_classes() << ", Worklist size: " << egraph.worklist_size() << "\n";
    return true;
  }

  bool rebuild() {
    egraph.rebuild(ctx);

    // Validate invariants after rebuild
    if (!ValidateEGraphInvariants(egraph, ctx)) {
      std::cerr << "\n!!! INVARIANT VIOLATION DETECTED !!!\n";
      std::cerr << "After operation #" << operation_count << " (REBUILD)\n";
      std::cerr << "Num classes: " << egraph.num_classes() << "\n";
      std::cerr << "Num nodes: " << egraph.num_nodes() << "\n";
      abort();
    }

    return true;
  }

  // NEW: Create congruent patterns that require merge_eclasses
  // Pattern: f(a), f(b), then merge(a, b) - forces congruence detection
  bool create_congruent_pattern(const uint8_t *data, size_t &pos, size_t size) {
    if (size < pos + 2) return true;

    // Create two distinct leaf nodes
    uint8_t symbol_a = data[pos++] % 5;                         // A-E
    uint8_t symbol_b = (symbol_a + 1 + (data[pos++] % 4)) % 5;  // Different from symbol_a

    auto leaf_a = egraph.emplace(static_cast<FuzzSymbol>(symbol_a), pos);
    auto leaf_b = egraph.emplace(static_cast<FuzzSymbol>(symbol_b), pos + 1);
    created_ids.push_back(leaf_a);
    created_ids.push_back(leaf_b);

    // Create compound nodes with same symbol but different children: f(a), f(b)
    if (pos < size) {
      uint8_t compound_symbol = 10 + (data[pos++] % 5);  // F-Mul
      auto fa = egraph.emplace(static_cast<FuzzSymbol>(compound_symbol), utils::small_vector<EClassId>{leaf_a});
      auto fb = egraph.emplace(static_cast<FuzzSymbol>(compound_symbol), utils::small_vector<EClassId>{leaf_b});
      created_ids.push_back(fa);
      created_ids.push_back(fb);

      // Now merge the leaves - this should make f(a) and f(b) congruent after rebuild
      egraph.merge(leaf_a, leaf_b);

      // Don't rebuild here - let the fuzzer decide when to rebuild
      // When rebuild() is called later, it should trigger merge_eclasses for f(a) and f(b)
    }

    return true;
  }

  EGraph<FuzzSymbol, FuzzAnalysis> egraph;
  ProcessingContext<FuzzSymbol> ctx;
  std::vector<EClassId> created_ids;
  size_t operation_count = 0;

  static constexpr size_t MAX_OPERATIONS = 1000;
};

// ============================================================================
// Main Fuzzer Entry Point
// ============================================================================

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  if (size < 2) return 0;

  // Initialize verbose flag from environment variable
  static bool initialized = false;
  if (!initialized) {
    const char *verbose_env = std::getenv("FUZZ_VERBOSE");
    g_verbose = verbose_env != nullptr && (std::strcmp(verbose_env, "1") == 0 || std::strcmp(verbose_env, "true") == 0);
    initialized = true;
  }

  FuzzerState state;
  size_t pos = 0;

  // Execute operations from input
  while (pos < size) {
    uint8_t op = data[pos++];
    if (!state.execute_operation(op, data, pos, size)) {
      break;
    }
  }

  // Final rebuild and validation
  state.egraph.rebuild(state.ctx);

  if (!ValidateEGraphInvariants(state.egraph, state.ctx)) {
    std::cerr << "\n!!! FINAL INVARIANT VIOLATION !!!\n";
    abort();
  }

  // Test idempotent rebuild
  size_t classes_before = state.egraph.num_classes();
  state.egraph.rebuild(state.ctx);
  if (state.egraph.num_classes() != classes_before) {
    std::cerr << "\n!!! REBUILD NOT IDEMPOTENT !!!\n";
    std::cerr << "Classes changed from " << classes_before << " to " << state.egraph.num_classes() << "\n";
    abort();
  }

  return 0;
}

}  // namespace memgraph::planner::core
