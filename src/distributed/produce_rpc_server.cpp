#include "distributed/produce_rpc_server.hpp"
#include "distributed/data_manager.hpp"
#include "distributed/pull_produce_rpc_messages.hpp"
#include "query/common.hpp"
#include "query/exceptions.hpp"
#include "transactions/engine_worker.hpp"

namespace distributed {

ProduceRpcServer::OngoingProduce::OngoingProduce(
    database::GraphDb &db, tx::TransactionId tx_id,
    std::shared_ptr<query::plan::LogicalOperator> op,
    query::SymbolTable symbol_table, Parameters parameters,
    std::vector<query::Symbol> pull_symbols)
    : dba_{db, tx_id},
      context_(dba_),
      pull_symbols_(std::move(pull_symbols)),
      frame_(symbol_table.max_position()),
      cursor_(op->MakeCursor(dba_)) {
  context_.symbol_table_ = std::move(symbol_table);
  context_.parameters_ = std::move(parameters);
}

std::pair<std::vector<query::TypedValue>, PullState>
ProduceRpcServer::OngoingProduce::Pull() {
  if (!accumulation_.empty()) {
    auto results = std::move(accumulation_.back());
    accumulation_.pop_back();
    for (auto &element : results) {
      try {
        query::ReconstructTypedValue(element);
      } catch (query::ReconstructionException &) {
        cursor_state_ = PullState::RECONSTRUCTION_ERROR;
        return std::make_pair(std::move(results), cursor_state_);
      }
    }

    return std::make_pair(std::move(results), PullState::CURSOR_IN_PROGRESS);
  }

  return PullOneFromCursor();
}

PullState ProduceRpcServer::OngoingProduce::Accumulate() {
  while (true) {
    auto result = PullOneFromCursor();
    if (result.second != PullState::CURSOR_IN_PROGRESS)
      return result.second;
    else
      accumulation_.emplace_back(std::move(result.first));
  }
}

std::pair<std::vector<query::TypedValue>, PullState>
ProduceRpcServer::OngoingProduce::PullOneFromCursor() {
  std::vector<query::TypedValue> results;

  // Check if we already exhausted this cursor (or it entered an error
  // state). This happens when we accumulate before normal pull.
  if (cursor_state_ != PullState::CURSOR_IN_PROGRESS) {
    return std::make_pair(results, cursor_state_);
  }

  try {
    if (cursor_->Pull(frame_, context_)) {
      results.reserve(pull_symbols_.size());
      for (const auto &symbol : pull_symbols_) {
        results.emplace_back(std::move(frame_[symbol]));
      }
    } else {
      cursor_state_ = PullState::CURSOR_EXHAUSTED;
    }
  } catch (const mvcc::SerializationError &) {
    cursor_state_ = PullState::SERIALIZATION_ERROR;
  } catch (const utils::LockTimeoutException &) {
    cursor_state_ = PullState::LOCK_TIMEOUT_ERROR;
  } catch (const RecordDeletedError &) {
    cursor_state_ = PullState::UPDATE_DELETED_ERROR;
  } catch (const query::ReconstructionException &) {
    cursor_state_ = PullState::RECONSTRUCTION_ERROR;
  } catch (const query::RemoveAttachedVertexException &) {
    cursor_state_ = PullState::UNABLE_TO_DELETE_VERTEX_ERROR;
  } catch (const query::QueryRuntimeException &) {
    cursor_state_ = PullState::QUERY_ERROR;
  } catch (const query::HintedAbortError &) {
    cursor_state_ = PullState::HINTED_ABORT_ERROR;
  }
  return std::make_pair(std::move(results), cursor_state_);
}

ProduceRpcServer::ProduceRpcServer(
    database::GraphDb &db, tx::Engine &tx_engine,
    communication::rpc::Server &server,
    const distributed::PlanConsumer &plan_consumer)
    : db_(db),
      produce_rpc_server_(server),
      plan_consumer_(plan_consumer),
      tx_engine_(tx_engine) {
  produce_rpc_server_.Register<PullRpc>([this](const PullReq &req) {
    return std::make_unique<PullRes>(Pull(req));
  });

  produce_rpc_server_.Register<TransactionCommandAdvancedRpc>(
      [this](const TransactionCommandAdvancedReq &req) {
        tx_engine_.UpdateCommand(req.member);
        db_.data_manager().ClearCacheForSingleTransaction(req.member);
        return std::make_unique<TransactionCommandAdvancedRes>();
      });
}

void ProduceRpcServer::FinishAndClearOngoingProducePlans(
    tx::TransactionId tx_id) {
  std::lock_guard<std::mutex> guard{ongoing_produces_lock_};
  for (auto it = ongoing_produces_.begin(); it != ongoing_produces_.end();) {
    if (std::get<0>(it->first) == tx_id) {
      it = ongoing_produces_.erase(it);
    } else {
      ++it;
    }
  }
}

ProduceRpcServer::OngoingProduce &ProduceRpcServer::GetOngoingProduce(
    const PullReq &req) {
  auto key_tuple = std::make_tuple(req.tx_id, req.command_id, req.plan_id);
  std::lock_guard<std::mutex> guard{ongoing_produces_lock_};
  auto found = ongoing_produces_.find(key_tuple);
  if (found != ongoing_produces_.end()) {
    return found->second;
  }
  if (db_.type() == database::GraphDb::Type::DISTRIBUTED_WORKER) {
    // On the worker cache the snapshot to have one RPC less.
    dynamic_cast<tx::WorkerEngine &>(tx_engine_)
        .RunningTransaction(req.tx_id, req.tx_snapshot);
  }
  auto &plan_pack = plan_consumer_.PlanForId(req.plan_id);
  return ongoing_produces_
      .emplace(std::piecewise_construct, std::forward_as_tuple(key_tuple),
               std::forward_as_tuple(db_, req.tx_id, plan_pack.plan,
                                     plan_pack.symbol_table, req.params,
                                     req.symbols))
      .first->second;
}

PullResData ProduceRpcServer::Pull(const PullReq &req) {
  auto &ongoing_produce = GetOngoingProduce(req);

  PullResData result{db_.WorkerId(), req.send_old, req.send_new};
  result.state_and_frames.pull_state = PullState::CURSOR_IN_PROGRESS;

  if (req.accumulate) {
    result.state_and_frames.pull_state = ongoing_produce.Accumulate();
    // If an error ocurred, we need to return that error.
    if (result.state_and_frames.pull_state != PullState::CURSOR_EXHAUSTED) {
      return result;
    }
  }

  for (int i = 0; i < req.batch_size; ++i) {
    auto pull_result = ongoing_produce.Pull();
    result.state_and_frames.pull_state = pull_result.second;
    if (pull_result.second != PullState::CURSOR_IN_PROGRESS) break;
    result.state_and_frames.frames.emplace_back(std::move(pull_result.first));
  }

  return result;
}

}  // namespace distributed
