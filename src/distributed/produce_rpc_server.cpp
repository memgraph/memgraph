#include "distributed/produce_rpc_server.hpp"

#include "database/distributed/distributed_graph_db.hpp"
#include "distributed/data_manager.hpp"
#include "distributed/pull_produce_rpc_messages.hpp"
#include "query/common.hpp"
#include "query/exceptions.hpp"
#include "transactions/distributed/engine_worker.hpp"

namespace distributed {

ProduceRpcServer::OngoingProduce::OngoingProduce(
    database::Worker *db, tx::TransactionId tx_id,
    const PlanConsumer::PlanPack &plan_pack, int64_t timestamp,
    const query::Parameters &parameters,
    std::vector<query::Symbol> pull_symbols)
    : dba_(db->Access(tx_id)),
      context_{dba_.get()},
      pull_symbols_(std::move(pull_symbols)),
      frame_(plan_pack.symbol_table.max_position()),
      execution_memory_(std::make_unique<utils::MonotonicBufferResource>(
          query::kExecutionMemoryBlockSize)),
      cursor_(plan_pack.plan->MakeCursor(dba_.get(), execution_memory_.get())) {
  context_.symbol_table = plan_pack.symbol_table;
  // TODO: Maybe we want a seperate MemoryResource per pull evaluation
  context_.evaluation_context.memory = execution_memory_.get();
  context_.evaluation_context.timestamp = timestamp;
  context_.evaluation_context.parameters = parameters;
  context_.evaluation_context.properties =
      query::NamesToProperties(plan_pack.storage.properties_, dba_.get());
  context_.evaluation_context.labels =
      query::NamesToLabels(plan_pack.storage.labels_, dba_.get());
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

void ProduceRpcServer::OngoingProduce::Reset() {
  cursor_->Reset();
  accumulation_.clear();
  cursor_state_ = PullState::CURSOR_IN_PROGRESS;
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
        results.push_back(frame_[symbol]);
      }
    } else {
      cursor_state_ = PullState::CURSOR_EXHAUSTED;
      cursor_->Shutdown();
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

ProduceRpcServer::ProduceRpcServer(database::Worker *db,
                                   tx::EngineWorker *tx_engine,
                                   distributed::Coordination *coordination,
                                   const PlanConsumer &plan_consumer,
                                   DataManager *data_manager)
    : db_(db),
      plan_consumer_(plan_consumer),
      tx_engine_(tx_engine) {
  coordination->Register<PullRpc>(
      [this](auto *req_reader, auto *res_builder) {
        PullReq req;
        slk::Load(&req, req_reader);
        PullRes res(Pull(req));
        slk::Save(res, res_builder);
      });

  coordination->Register<ResetCursorRpc>(
      [this](auto *req_reader, auto *res_builder) {
        ResetCursorReq req;
        slk::Load(&req, req_reader);
        Reset(req);
        ResetCursorRes res;
        slk::Save(res, res_builder);
      });

  CHECK(data_manager);

  coordination->Register<TransactionCommandAdvancedRpc>(
      [this, data_manager](auto *req_reader, auto *res_builder) {
        TransactionCommandAdvancedReq req;
        slk::Load(&req, req_reader);
        tx_engine_->UpdateCommand(req.member);
        data_manager->ClearCacheForSingleTransaction(req.member);
        TransactionCommandAdvancedRes res;
        slk::Save(res, res_builder);
      });
}

void ProduceRpcServer::ClearTransactionalCache(
    tx::TransactionId oldest_active) {
  std::lock_guard<std::mutex> guard{ongoing_produces_lock_};
  for (auto it = ongoing_produces_.begin(); it != ongoing_produces_.end();) {
    if (std::get<0>(it->first) < oldest_active) {
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
  // On the worker cache the snapshot to have one RPC less.
  tx_engine_->RunningTransaction(req.tx_id, req.tx_snapshot);
  auto &plan_pack = plan_consumer_.PlanForId(req.plan_id);
  return ongoing_produces_
      .emplace(std::piecewise_construct, std::forward_as_tuple(key_tuple),
               std::forward_as_tuple(db_, req.tx_id, plan_pack, req.timestamp,
                                     req.parameters, req.symbols))
      .first->second;
}

PullResData ProduceRpcServer::Pull(const PullReq &req) {
  auto &ongoing_produce = GetOngoingProduce(req);

  PullResData result(db_->WorkerId(), req.send_versions);
  result.pull_state = PullState::CURSOR_IN_PROGRESS;

  if (req.accumulate) {
    result.pull_state = ongoing_produce.Accumulate();
    // If an error ocurred, we need to return that error.
    if (result.pull_state != PullState::CURSOR_EXHAUSTED) {
      return result;
    }
  }

  for (int i = 0; i < req.batch_size; ++i) {
    auto pull_result = ongoing_produce.Pull();
    result.pull_state = pull_result.second;
    if (pull_result.second != PullState::CURSOR_IN_PROGRESS) break;
    result.frames.emplace_back(std::move(pull_result.first));
  }

  return result;
}

void ProduceRpcServer::Reset(const ResetCursorReq &req) {
  auto key_tuple = std::make_tuple(req.tx_id, req.command_id, req.plan_id);
  std::lock_guard<std::mutex> guard{ongoing_produces_lock_};
  auto found = ongoing_produces_.find(key_tuple);
  // It is fine if the cursor doesn't exist yet. Creating a new cursor is the
  // same thing as reseting an existing one.
  if (found != ongoing_produces_.end()) {
    found->second.Reset();
  }
}

}  // namespace distributed
