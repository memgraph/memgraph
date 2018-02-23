#pragma once

#include "communication/rpc/server.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "distributed/plan_rpc_messages.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/operator.hpp"

namespace distributed {

/** Handles plan consumption from master. Creates and holds a local cache of
 * plans. Worker side. */
class PlanConsumer {
 public:
  struct PlanPack {
    PlanPack(std::shared_ptr<query::plan::LogicalOperator> plan,
             SymbolTable symbol_table, AstTreeStorage storage)
        : plan(plan),
          symbol_table(std::move(symbol_table)),
          storage(std::move(storage)) {}

    std::shared_ptr<query::plan::LogicalOperator> plan;
    SymbolTable symbol_table;
    const AstTreeStorage storage;
  };

  explicit PlanConsumer(communication::rpc::Server &server);

  /** Return cached plan and symbol table for a given plan id. */
  PlanPack &PlanForId(int64_t plan_id) const;

 private:
  communication::rpc::Server &server_;
  // TODO remove unique_ptr. This is to get it to work, emplacing into a
  // ConcurrentMap is tricky.
  mutable ConcurrentMap<int64_t, std::unique_ptr<PlanPack>> plan_cache_;
};

}  // namespace distributed
