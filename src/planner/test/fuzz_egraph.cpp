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
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "planner/core/egraph.hpp"
#include "planner/core/processing_context.hpp"

namespace memgraph::planner::core {

// Simple test symbols for fuzzing
enum class FuzzSymbol : uint32_t {
  A = 0,
  B = 1,
  C = 2,
  D = 3,
  E = 4,
  // Functions
  F = 10,
  G = 11,
  H = 12,
  Plus = 13,
  Mul = 14,
};

struct FuzzAnalysis {};

// Validate all e-graph invariants using only public APIs
template <typename Symbol, typename Analysis>
bool ValidateEGraphInvariants(EGraph<Symbol, Analysis> &egraph, ProcessingContext<Symbol> &ctx) {
  try {
    // Get all canonical class IDs using public API
    std::vector<EClassId> canonical_ids;
    for (auto id : egraph.canonical_class_ids()) {
      canonical_ids.push_back(id);
    }

    // 1. Verify all classes are canonical and congruence is closed
    std::unordered_map<std::string, EClassId> canonical_forms;

    for (auto class_id : canonical_ids) {
      // Check this is indeed canonical
      if (egraph.find(class_id) != class_id) {
        std::cerr << "Non-canonical class ID " << class_id << " in canonical_class_ids()\n";
        return false;
      }

      const auto &eclass = egraph.eclass(class_id);

      // Check each e-node in the class
      for (auto enode_id : eclass.nodes()) {
        const auto &enode = egraph.get_enode(enode_id);

        // Create canonical form signature for congruence checking
        std::string sig = std::to_string(static_cast<uint32_t>(enode.symbol()));
        for (auto child_id : enode.children()) {
          auto canonical_child = egraph.find(child_id);
          sig += "_" + std::to_string(canonical_child);
        }
        if (enode.is_leaf()) {
          sig += "_D" + std::to_string(enode.disambiguator());
        }

        // Check congruence closure
        if (canonical_forms.count(sig)) {
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

      // 2. Verify parent-child relationships
      for (auto [parent_enode_id, parent_class_id] : eclass.parents()) {
        // Parent class should be canonical
        auto canonical_parent = egraph.find(parent_class_id);
        if (canonical_parent != parent_class_id) {
          std::cerr << "Non-canonical parent class ID: " << parent_class_id << " (canonical: " << canonical_parent
                    << ")\n";
          return false;
        }

        // Verify parent actually references this child
        const auto &parent_enode = egraph.get_enode(parent_enode_id);
        if (parent_enode.arity() > 0) {
          bool has_child = false;
          for (auto child_id : parent_enode.children()) {
            if (egraph.find(child_id) == class_id) {
              has_child = true;
              break;
            }
          }
          if (!has_child) {
            std::cerr << "Parent e-node doesn't reference child e-class " << class_id << "\n";
            return false;
          }
        }
      }
    }

    // 3. Verify rebuild is complete
    if (egraph.needs_rebuild()) {
      std::cerr << "Worklist not empty after rebuild (size: " << egraph.worklist_size() << ")\n";
      return false;
    }

    // 4. Count total nodes and verify consistency
    size_t total_nodes = 0;
    for (auto class_id : canonical_ids) {
      const auto &eclass = egraph.eclass(class_id);
      total_nodes += eclass.size();
    }

    if (egraph.num_nodes() != total_nodes) {
      std::cerr << "Node count mismatch: num_nodes()=" << egraph.num_nodes() << " vs actual=" << total_nodes << "\n";
      return false;
    }

    // 5. Verify number of classes matches
    if (egraph.num_classes() != canonical_ids.size()) {
      std::cerr << "Class count mismatch: num_classes()=" << egraph.num_classes()
                << " vs canonical_ids.size()=" << canonical_ids.size() << "\n";
      return false;
    }

    // 6. Test that creating duplicate structures returns existing e-classes
    // This validates hashcons is working correctly
    if (!canonical_ids.empty()) {
      auto test_id = canonical_ids[0];
      const auto &test_eclass = egraph.eclass(test_id);
      if (test_eclass.size() > 0) {
        auto test_enode_id = *test_eclass.nodes().begin();
        const auto &test_enode = egraph.get_enode(test_enode_id);

        // Try to create the same structure
        EClassId duplicate_id = test_id;
        if (test_enode.is_leaf()) {
          duplicate_id = egraph.emplace(test_enode.symbol(), test_enode.disambiguator());
        } else if (test_enode.arity() > 0) {
          // Canonicalize children first
          std::vector<EClassId> canonical_children;
          for (auto child : test_enode.children()) {
            canonical_children.push_back(egraph.find(child));
          }
          duplicate_id = egraph.emplace(
              test_enode.symbol(), utils::small_vector<EClassId>(canonical_children.begin(), canonical_children.end()));
        }

        // Should return the same canonical e-class
        if (egraph.find(duplicate_id) != test_id) {
          std::cerr << "Hashcons failed: duplicate structure created new e-class\n";
          return false;
        }
      }
    }

    return true;
  } catch (const std::exception &e) {
    std::cerr << "Exception during validation: " << e.what() << "\n";
    return false;
  }
}

// Operation log for debugging
struct OperationLog {
  enum OpType { CREATE_LEAF, CREATE_NODE, MERGE, REBUILD };
  OpType type;
  std::string description;
};

static std::vector<OperationLog> operation_history;
static bool verbose_logging = getenv("FUZZ_VERBOSE") != nullptr;

void LogOperation(OperationLog::OpType type, const std::string &desc) {
  operation_history.push_back({type, desc});
  if (verbose_logging) {
    std::cerr << "[Op " << operation_history.size() << "] " << desc << "\n";
  }
}

void DumpOperationHistory() {
  std::cerr << "\n=== OPERATION HISTORY ===\n";
  for (size_t i = 0; i < operation_history.size(); ++i) {
    const auto &op = operation_history[i];
    std::cerr << "[" << (i + 1) << "] ";
    switch (op.type) {
      case OperationLog::CREATE_LEAF:
        std::cerr << "CREATE_LEAF: ";
        break;
      case OperationLog::CREATE_NODE:
        std::cerr << "CREATE_NODE: ";
        break;
      case OperationLog::MERGE:
        std::cerr << "MERGE: ";
        break;
      case OperationLog::REBUILD:
        std::cerr << "REBUILD: ";
        break;
    }
    std::cerr << op.description << "\n";
  }
  std::cerr << "=========================\n\n";
}

const char *SymbolToString(FuzzSymbol sym) {
  switch (sym) {
    case FuzzSymbol::A:
      return "A";
    case FuzzSymbol::B:
      return "B";
    case FuzzSymbol::C:
      return "C";
    case FuzzSymbol::D:
      return "D";
    case FuzzSymbol::E:
      return "E";
    case FuzzSymbol::F:
      return "F";
    case FuzzSymbol::G:
      return "G";
    case FuzzSymbol::H:
      return "H";
    case FuzzSymbol::Plus:
      return "Plus";
    case FuzzSymbol::Mul:
      return "Mul";
    default:
      return "Unknown";
  }
}

// Fuzzer entry point
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  if (size < 2) return 0;

  operation_history.clear();
  EGraph<FuzzSymbol, FuzzAnalysis> egraph;
  ProcessingContext<FuzzSymbol> ctx;
  std::vector<EClassId> created_ids;

  // Parse operations from fuzz input
  size_t pos = 0;
  size_t op_count = 0;

  while (pos < size) {
    uint8_t op = data[pos++];
    op_count++;

    switch (op % 4) {
      case 0: {  // Add leaf node
        if (pos >= size) break;
        uint8_t symbol = data[pos++] % 5;  // A-E
        uint64_t disambiguator = pos < size ? data[pos++] : 0;

        try {
          auto sym = static_cast<FuzzSymbol>(symbol);
          auto id = egraph.emplace(sym, disambiguator);
          created_ids.push_back(id);

          std::string desc = std::string(SymbolToString(sym)) + "(disambig=" + std::to_string(disambiguator) +
                             ") -> eclass#" + std::to_string(id);
          LogOperation(OperationLog::CREATE_LEAF, desc);
        } catch (const std::exception &e) {
          if (verbose_logging) {
            std::cerr << "  Exception: " << e.what() << "\n";
          }
        }
        break;
      }

      case 1: {  // Add node with children
        if (pos + 2 >= size) break;
        uint8_t symbol = data[pos++] % 5 + 10;       // F-Mul
        uint8_t num_children = data[pos++] % 3 + 1;  // 1-3 children

        std::vector<EClassId> children;
        std::string children_str = "[";
        for (uint8_t i = 0; i < num_children && pos < size; ++i) {
          if (!created_ids.empty()) {
            uint8_t child_idx = data[pos++] % created_ids.size();
            auto child_id = created_ids[child_idx];
            children.push_back(child_id);
            if (i > 0) children_str += ", ";
            children_str += "#" + std::to_string(child_id);
          }
        }
        children_str += "]";

        if (!children.empty()) {
          try {
            auto sym = static_cast<FuzzSymbol>(symbol);
            auto id = egraph.emplace(sym, utils::small_vector<EClassId>(children.begin(), children.end()));
            created_ids.push_back(id);

            std::string desc = std::string(SymbolToString(sym)) + children_str + " -> eclass#" + std::to_string(id);
            LogOperation(OperationLog::CREATE_NODE, desc);
          } catch (const std::exception &e) {
            if (verbose_logging) {
              std::cerr << "  Exception: " << e.what() << "\n";
            }
          }
        }
        break;
      }

      case 2: {  // Merge two e-classes
        if (pos + 1 >= size || created_ids.size() < 2) break;
        uint8_t idx1 = data[pos++] % created_ids.size();
        uint8_t idx2 = data[pos++] % created_ids.size();

        try {
          auto id1 = created_ids[idx1];
          auto id2 = created_ids[idx2];
          auto merged = egraph.merge(id1, id2);

          std::string desc =
              "eclass#" + std::to_string(id1) + " = eclass#" + std::to_string(id2) + " -> #" + std::to_string(merged);
          LogOperation(OperationLog::MERGE, desc);
        } catch (const std::exception &e) {
          if (verbose_logging) {
            std::cerr << "  Exception: " << e.what() << "\n";
          }
        }
        break;
      }

      case 3: {  // Rebuild
        try {
          size_t classes_before = egraph.num_classes();
          size_t nodes_before = egraph.num_nodes();

          egraph.rebuild(ctx);

          std::string desc = "classes: " + std::to_string(classes_before) + " -> " +
                             std::to_string(egraph.num_classes()) + ", nodes: " + std::to_string(nodes_before) +
                             " -> " + std::to_string(egraph.num_nodes());
          LogOperation(OperationLog::REBUILD, desc);

          // Validate invariants after rebuild
          if (!ValidateEGraphInvariants(egraph, ctx)) {
            // Found an invariant violation!
            std::cerr << "\n!!! INVARIANT VIOLATION DETECTED !!!\n";
            std::cerr << "After operation #" << op_count << " (REBUILD)\n";
            std::cerr << "Input size: " << size << "\n";
            std::cerr << "Num classes: " << egraph.num_classes() << "\n";
            std::cerr << "Num nodes: " << egraph.num_nodes() << "\n";
            DumpOperationHistory();

            // Dump the failing input bytes for reproduction
            std::cerr << "Failing input (hex): ";
            for (size_t i = 0; i < size; ++i) {
              fprintf(stderr, "%02x", data[i]);
            }
            std::cerr << "\n";

            abort();  // Crash to signal the fuzzer we found a bug
          }
        } catch (const std::exception &e) {
          if (verbose_logging) {
            std::cerr << "  Exception during rebuild: " << e.what() << "\n";
          }
        }
        break;
      }
    }
  }

  // Final rebuild and validation
  try {
    size_t classes_before = egraph.num_classes();
    egraph.rebuild(ctx);

    LogOperation(OperationLog::REBUILD, "Final rebuild: " + std::to_string(classes_before) + " -> " +
                                            std::to_string(egraph.num_classes()) + " classes");

    if (!ValidateEGraphInvariants(egraph, ctx)) {
      std::cerr << "\n!!! FINAL INVARIANT VIOLATION !!!\n";
      std::cerr << "After final rebuild\n";
      DumpOperationHistory();
      abort();
    }

    // Test idempotent rebuild
    classes_before = egraph.num_classes();
    egraph.rebuild(ctx);
    if (egraph.num_classes() != classes_before) {
      std::cerr << "\n!!! REBUILD NOT IDEMPOTENT !!!\n";
      std::cerr << "Classes changed from " << classes_before << " to " << egraph.num_classes() << "\n";
      DumpOperationHistory();
      abort();
    }
  } catch (const std::exception &e) {
    if (verbose_logging) {
      std::cerr << "Final exception: " << e.what() << "\n";
    }
  }

  return 0;
}

}  // namespace memgraph::planner::core
