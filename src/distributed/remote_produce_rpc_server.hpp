#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "communication/rpc/server.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/plan_consumer.hpp"
#include "query/context.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "query/typed_value.hpp"
#include "transactions/engine.hpp"
#include "transactions/type.hpp"

namespace distributed {

/// Handles the execution of a plan on the worker, requested by the remote
/// master. Assumes that (tx_id, plan_id) uniquely identifies an execution, and
/// that there will never be parallel requests for the same execution thus
/// identified.
class RemoteProduceRpcServer {
  /// Encapsulates a Cursor execution in progress. Can be used for pulling a
  /// single result from the execution, or pulling all and accumulating the
  /// results. Accumulations are used for synchronizing updates in distributed
  /// MG (see query::plan::Synchronize).
  class OngoingProduce {
   public:
    OngoingProduce(database::GraphDb &db, tx::transaction_id_t tx_id,
                   std::shared_ptr<query::plan::LogicalOperator> op,
                   query::SymbolTable symbol_table, Parameters parameters,
                   std::vector<query::Symbol> pull_symbols);

    /// Returns a vector of typed values (one for each `pull_symbol`), and an
    /// indication of the pull result. The result data is valid only if the
    /// returned state is CURSOR_IN_PROGRESS.
    std::pair<std::vector<query::TypedValue>, RemotePullState> Pull();

    /// Accumulates all the frames pulled from the cursor and returns
    /// CURSOR_EXHAUSTED. If an error occurs, an appropriate value is returned.
    RemotePullState Accumulate();

   private:
    database::GraphDbAccessor dba_;
    std::unique_ptr<query::plan::Cursor> cursor_;
    query::Context context_;
    std::vector<query::Symbol> pull_symbols_;
    query::Frame frame_;
    RemotePullState cursor_state_{RemotePullState::CURSOR_IN_PROGRESS};
    std::vector<std::vector<query::TypedValue>> accumulation_;

    /// Pulls and returns a single result from the cursor.
    std::pair<std::vector<query::TypedValue>, RemotePullState>
    PullOneFromCursor();
  };

 public:
  RemoteProduceRpcServer(database::GraphDb &db, tx::Engine &tx_engine,
                         communication::rpc::Server &server,
                         const distributed::PlanConsumer &plan_consumer);

  /// Clears the cache of local transactions that have expired. The signature of
  /// this method is dictated by `distributed::TransactionalCacheCleaner`.
  void ClearTransactionalCache(tx::transaction_id_t oldest_active);

 private:
  database::GraphDb &db_;
  communication::rpc::Server &remote_produce_rpc_server_;
  const distributed::PlanConsumer &plan_consumer_;
  ConcurrentMap<std::pair<tx::transaction_id_t, int64_t>, OngoingProduce>
      ongoing_produces_;
  tx::Engine &tx_engine_;

  /// Gets an ongoing produce for the given pull request. Creates a new one if
  /// there is none currently existing.
  OngoingProduce &GetOngoingProduce(const RemotePullReq &req);

  /// Performs a single remote pull for the given request.
  RemotePullResData RemotePull(const RemotePullReq &req);
};

}  // namespace distributed
