#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <utility>
#include <vector>

#include "communication/rpc/server.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/plan_consumer.hpp"
#include "distributed/remote_pull_produce_rpc_messages.hpp"
#include "query/context.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/frame.hpp"
#include "query/parameters.hpp"
#include "query/plan/operator.hpp"
#include "query/typed_value.hpp"
#include "transactions/type.hpp"

namespace distributed {

/**
 * Handles the execution of a plan on the worker, requested by the remote
 * master. Assumes that (tx_id, plan_id) uniquely identifies an execution, and
 * that there will never be parallel requests for the same execution thus
 * identified.
 */
class RemoteProduceRpcServer {
  /** Encapsulates an execution in progress. */
  class OngoingProduce {
   public:
    OngoingProduce(database::GraphDb &db, tx::transaction_id_t tx_id,
                   std::shared_ptr<query::plan::LogicalOperator> op,
                   query::SymbolTable symbol_table, Parameters parameters,
                   std::vector<query::Symbol> pull_symbols)
        : dba_{db, tx_id},
          cursor_(op->MakeCursor(dba_)),
          context_(dba_),
          pull_symbols_(std::move(pull_symbols)),
          frame_(symbol_table.max_position()) {
      context_.symbol_table_ = std::move(symbol_table);
      context_.parameters_ = std::move(parameters);
    }

    /** Returns a vector of typed values (one for each `pull_symbol`), and a
     * `bool` indicating if the pull was successful (or the cursor is
     * exhausted). */
    std::pair<std::vector<query::TypedValue>, bool> Pull() {
      std::vector<query::TypedValue> results;
      auto success = cursor_->Pull(frame_, context_);
      if (success) {
        results.reserve(pull_symbols_.size());
        for (const auto &symbol : pull_symbols_) {
          results.emplace_back(std::move(frame_[symbol]));
        }
      }
      return std::make_pair(std::move(results), success);
    }

   private:
    // TODO currently each OngoingProduce has it's own GDBA. There is no sharing
    // of them in the same transaction. This should be correct, but it's
    // inefficient in multi-command queries, and when a single query will get
    // broken down into multiple parts.
    database::GraphDbAccessor dba_;
    std::unique_ptr<query::plan::Cursor> cursor_;
    query::Context context_;
    std::vector<query::Symbol> pull_symbols_;
    query::Frame frame_;
  };

 public:
  RemoteProduceRpcServer(database::GraphDb &db,
                         communication::rpc::System &system,
                         const distributed::PlanConsumer &plan_consumer)
      : db_(db),
        remote_produce_rpc_server_(system, kRemotePullProduceRpcName),
        plan_consumer_(plan_consumer) {
    remote_produce_rpc_server_.Register<RemotePullRpc>(
        [this](const RemotePullReq &req) {
          return std::make_unique<RemotePullRes>(RemotePull(req));
        });

    remote_produce_rpc_server_.Register<EndRemotePullRpc>([this](
        const EndRemotePullReq &req) {
      std::lock_guard<std::mutex> guard{ongoing_produces_lock_};
      auto it = ongoing_produces_.find(req.member);
      CHECK(it != ongoing_produces_.end()) << "Failed to find ongoing produce";
      ongoing_produces_.erase(it);
      return std::make_unique<EndRemotePullRes>();
    });
  }

 private:
  database::GraphDb &db_;
  communication::rpc::Server remote_produce_rpc_server_;
  const distributed::PlanConsumer &plan_consumer_;

  std::map<std::pair<tx::transaction_id_t, int64_t>, OngoingProduce>
      ongoing_produces_;
  std::mutex ongoing_produces_lock_;

  auto &GetOngoingProduce(const RemotePullReq &req) {
    std::lock_guard<std::mutex> guard{ongoing_produces_lock_};
    auto found = ongoing_produces_.find({req.tx_id, req.plan_id});
    if (found != ongoing_produces_.end()) {
      return found->second;
    }

    auto &plan_pack = plan_consumer_.PlanForId(req.plan_id);
    return ongoing_produces_
        .emplace(std::piecewise_construct,
                 std::forward_as_tuple(req.tx_id, req.plan_id),
                 std::forward_as_tuple(db_, req.tx_id, plan_pack.plan,
                                       plan_pack.symbol_table, req.params,
                                       req.symbols))
        .first->second;
  }

  RemotePullResData RemotePull(const RemotePullReq &req) {
    auto &ongoing_produce = GetOngoingProduce(req);

    RemotePullResData result{db_.WorkerId(), req.send_old, req.send_new};

    result.state_and_frames.pull_state = RemotePullState::CURSOR_IN_PROGRESS;

    for (int i = 0; i < req.batch_size; ++i) {
      // TODO exception handling (Serialization errors)
      // when full CRUD. Maybe put it in OngoingProduce::Pull
      auto pull_result = ongoing_produce.Pull();
      if (!pull_result.second) {
        result.state_and_frames.pull_state = RemotePullState::CURSOR_EXHAUSTED;
        break;
      }
      result.state_and_frames.frames.emplace_back(std::move(pull_result.first));
    }

    return result;
  }
};
}  // namespace distributed
