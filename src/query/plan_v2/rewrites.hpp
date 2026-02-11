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

#include <chrono>
#include <cstddef>
#include <limits>

#include "query/plan_v2/egraph.hpp"

namespace memgraph::query::plan::v2 {

/**
 * @brief Configuration for the rewrite engine
 */
struct RewriteConfig {
  /// Maximum number of rewrite iterations before stopping
  std::size_t max_iterations = 10;

  /// Maximum number of e-nodes allowed in the e-graph
  /// Rewriting stops if this limit is exceeded
  std::size_t max_enodes = 10000;

  /// Maximum time budget for rewriting
  /// Rewriting stops if this timeout is exceeded
  std::chrono::milliseconds timeout = std::chrono::milliseconds{1000};

  /// Default configuration with reasonable limits
  static auto Default() -> RewriteConfig { return RewriteConfig{}; }

  /// Unlimited configuration (for testing)
  /// Uses very large but safe values that won't cause overflow in duration comparisons
  static auto Unlimited() -> RewriteConfig {
    return RewriteConfig{
        .max_iterations = std::numeric_limits<std::size_t>::max(),
        .max_enodes = std::numeric_limits<std::size_t>::max(),
        .timeout = std::chrono::hours{24 * 365},  // 1 year - effectively unlimited
    };
  }
};

/**
 * @brief Result of applying rewrites
 */
struct RewriteResult {
  /// Total number of rewrites (merges) applied
  std::size_t rewrites_applied = 0;

  /// Number of iterations completed
  std::size_t iterations = 0;

  /// Why rewriting stopped
  enum class StopReason {
    Saturated,       ///< Fixed point reached (no more rewrites possible)
    IterationLimit,  ///< Reached max_iterations
    ENodeLimit,      ///< Exceeded max_enodes
    Timeout,         ///< Exceeded timeout
  } stop_reason = StopReason::Saturated;

  [[nodiscard]] auto saturated() const -> bool { return stop_reason == StopReason::Saturated; }
};

/**
 * @brief Apply identifier inlining rewrite to the e-graph
 *
 * This rewrite finds patterns where:
 * - Bind(input, sym, expr) binds an identifier to an expression
 * - Identifier(sym) references that identifier
 *
 * It merges the Identifier's e-class with expr's e-class, effectively
 * inlining the bound expression wherever the identifier is used.
 *
 * Example:
 *   RETURN x AS y, y + 1
 *   Before: Bind(Once, sym_x, x), Identifier(sym_x) for "y + 1"
 *   After:  Identifier(sym_x) merged with x's e-class
 *
 * @param eg The e-graph to apply the rewrite to
 * @return Number of merges performed
 */
auto ApplyInlineRewrite(egraph &eg) -> std::size_t;

/**
 * @brief Apply all optimization rewrites to the e-graph
 *
 * Runs all available rewrites until a fixed point or a limit is reached.
 *
 * @param eg The e-graph to optimize
 * @param config Configuration controlling limits and timeout
 * @return Result containing statistics and stop reason
 */
auto ApplyAllRewrites(egraph &eg, RewriteConfig const &config = RewriteConfig::Default()) -> RewriteResult;

}  // namespace memgraph::query::plan::v2
