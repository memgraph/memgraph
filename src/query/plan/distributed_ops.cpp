#include "query/plan/distributed_ops.hpp"

#include "database/distributed_graph_db.hpp"
#include "distributed/bfs_rpc_clients.hpp"
#include "distributed/pull_produce_rpc_messages.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "distributed/updates_rpc_clients.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"

DEFINE_HIDDEN_int32(remote_pull_sleep_micros, 10,
                    "Sleep between remote result pulling in microseconds");

// macro for the default implementation of LogicalOperator::Accept
// that accepts the visitor and visits it's input_ operator
#define ACCEPT_WITH_INPUT(class_name)                                    \
  bool class_name::Accept(HierarchicalLogicalOperatorVisitor &visitor) { \
    if (visitor.PreVisit(*this)) {                                       \
      input_->Accept(visitor);                                           \
    }                                                                    \
    return visitor.PostVisit(*this);                                     \
  }

namespace query::plan {

bool PullRemote::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    if (input_) input_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

std::vector<Symbol> PullRemote::OutputSymbols(const SymbolTable &table) const {
  return input_ ? input_->OutputSymbols(table) : std::vector<Symbol>{};
}

std::vector<Symbol> PullRemote::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = symbols_;
  if (input_) {
    auto input_symbols = input_->ModifiedSymbols(table);
    symbols.insert(symbols.end(), input_symbols.begin(), input_symbols.end());
  }
  return symbols;
}

std::vector<Symbol> Synchronize::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  if (pull_remote_) {
    auto pull_symbols = pull_remote_->ModifiedSymbols(table);
    symbols.insert(symbols.end(), pull_symbols.begin(), pull_symbols.end());
  }
  return symbols;
}

bool Synchronize::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  if (visitor.PreVisit(*this)) {
    // pull_remote_ is optional here, so visit it only if we continue visiting
    // and pull_remote_ does exist.
    input_->Accept(visitor) && pull_remote_ && pull_remote_->Accept(visitor);
  }
  return visitor.PostVisit(*this);
}

PullRemoteOrderBy::PullRemoteOrderBy(
    const std::shared_ptr<LogicalOperator> &input, int64_t plan_id,
    const std::vector<std::pair<Ordering, Expression *>> &order_by,
    const std::vector<Symbol> &symbols)
    : input_(input), plan_id_(plan_id), symbols_(symbols) {
  CHECK(input_ != nullptr)
      << "PullRemoteOrderBy should always be constructed with input!";
  std::vector<Ordering> ordering;
  ordering.reserve(order_by.size());
  order_by_.reserve(order_by.size());
  for (const auto &ordering_expression_pair : order_by) {
    ordering.emplace_back(ordering_expression_pair.first);
    order_by_.emplace_back(ordering_expression_pair.second);
  }
  compare_ = TypedValueVectorCompare(ordering);
}

ACCEPT_WITH_INPUT(PullRemoteOrderBy);

std::vector<Symbol> PullRemoteOrderBy::OutputSymbols(
    const SymbolTable &table) const {
  return input_->OutputSymbols(table);
}

std::vector<Symbol> PullRemoteOrderBy::ModifiedSymbols(
    const SymbolTable &table) const {
  return input_->ModifiedSymbols(table);
}

namespace {

/** Helper class that wraps remote pulling for cursors that handle results from
 * distributed workers.
 *
 * The command_id should be the command_id at the initialization of a cursor.
 */
class RemotePuller {
 public:
  RemotePuller(distributed::PullRpcClients *pull_clients,
               database::GraphDbAccessor &db,
               const std::vector<Symbol> &symbols, int64_t plan_id,
               tx::CommandId command_id)
      : pull_clients_(pull_clients),
        db_(db),
        symbols_(symbols),
        plan_id_(plan_id),
        command_id_(command_id) {
    CHECK(pull_clients_);
    worker_ids_ = pull_clients_->GetWorkerIds();
    // Remove master from the worker ids list.
    worker_ids_.erase(std::find(worker_ids_.begin(), worker_ids_.end(), 0));
  }

  void Initialize(Context &context) {
    if (!remote_pulls_initialized_) {
      VLOG(10) << "[RemotePuller] [" << context.db_accessor_.transaction_id()
               << "] [" << plan_id_ << "] [" << command_id_ << "] initialized";
      for (auto &worker_id : worker_ids_) {
        UpdatePullForWorker(worker_id, context);
      }
      remote_pulls_initialized_ = true;
    }
  }

  void Update(Context &context) {
    // If we don't have results for a worker, check if his remote pull
    // finished and save results locally.

    auto move_frames = [this, &context](int worker_id, auto remote_results) {
      VLOG(10) << "[RemotePuller] [" << context.db_accessor_.transaction_id()
               << "] [" << plan_id_ << "] [" << command_id_
               << "] received results from " << worker_id;
      remote_results_[worker_id] = std::move(remote_results.frames);
      // Since we return and remove results from the back of the vector,
      // reverse the results so the first to return is on the end of the
      // vector.
      std::reverse(remote_results_[worker_id].begin(),
                   remote_results_[worker_id].end());
    };

    for (auto &worker_id : worker_ids_) {
      if (!remote_results_[worker_id].empty()) continue;

      auto found_it = remote_pulls_.find(worker_id);
      if (found_it == remote_pulls_.end()) continue;

      auto &remote_pull = found_it->second;
      if (!remote_pull.IsReady()) continue;

      auto remote_results = remote_pull.get();
      switch (remote_results.pull_state) {
        case distributed::PullState::CURSOR_EXHAUSTED:
          VLOG(10) << "[RemotePuller] ["
                   << context.db_accessor_.transaction_id() << "] [" << plan_id_
                   << "] [" << command_id_ << "] cursor exhausted from "
                   << worker_id;
          move_frames(worker_id, remote_results);
          remote_pulls_.erase(found_it);
          break;
        case distributed::PullState::CURSOR_IN_PROGRESS:
          VLOG(10) << "[RemotePuller] ["
                   << context.db_accessor_.transaction_id() << "] [" << plan_id_
                   << "] [" << command_id_ << "] cursor in progress from "
                   << worker_id;
          move_frames(worker_id, remote_results);
          UpdatePullForWorker(worker_id, context);
          break;
        case distributed::PullState::SERIALIZATION_ERROR:
          throw mvcc::SerializationError(
              "Serialization error occured during PullRemote !");
        case distributed::PullState::LOCK_TIMEOUT_ERROR:
          throw utils::LockTimeoutException(
              "LockTimeout error occured during PullRemote !");
        case distributed::PullState::UPDATE_DELETED_ERROR:
          throw QueryRuntimeException(
              "RecordDeleted error ocured during PullRemote !");
        case distributed::PullState::RECONSTRUCTION_ERROR:
          throw query::ReconstructionException();
        case distributed::PullState::UNABLE_TO_DELETE_VERTEX_ERROR:
          throw RemoveAttachedVertexException();
        case distributed::PullState::HINTED_ABORT_ERROR:
          throw HintedAbortError();
        case distributed::PullState::QUERY_ERROR:
          throw QueryRuntimeException(
              "Query runtime error occurred during PullRemote !");
      }
    }
  }

  void Reset() {
    worker_ids_ = pull_clients_->GetWorkerIds();
    // Remove master from the worker ids list.
    worker_ids_.erase(std::find(worker_ids_.begin(), worker_ids_.end(), 0));

    // We must clear remote_pulls before reseting cursors to make sure that all
    // outstanding remote pulls are done. Otherwise we might try to reset cursor
    // during its pull.
    remote_pulls_.clear();
    for (auto &worker_id : worker_ids_) {
      pull_clients_->ResetCursor(&db_, worker_id, plan_id_, command_id_);
    }
    remote_results_.clear();
    remote_pulls_initialized_ = false;
  }

  auto Workers() { return worker_ids_; }

  int GetWorkerId(int worker_id_index) { return worker_ids_[worker_id_index]; }

  size_t WorkerCount() { return worker_ids_.size(); }

  void ClearWorkers() { worker_ids_.clear(); }

  bool HasPendingPulls() { return !remote_pulls_.empty(); }

  bool HasPendingPullFromWorker(int worker_id) {
    return remote_pulls_.find(worker_id) != remote_pulls_.end();
  }

  bool HasResultsFromWorker(int worker_id) {
    return !remote_results_[worker_id].empty();
  }

  std::vector<query::TypedValue> PopResultFromWorker(int worker_id) {
    auto result = remote_results_[worker_id].back();
    remote_results_[worker_id].pop_back();

    // Remove the worker if we exhausted all locally stored results and there
    // are no more pending remote pulls for that worker.
    if (remote_results_[worker_id].empty() &&
        remote_pulls_.find(worker_id) == remote_pulls_.end()) {
      worker_ids_.erase(
          std::find(worker_ids_.begin(), worker_ids_.end(), worker_id));
    }

    return result;
  }

 private:
  distributed::PullRpcClients *pull_clients_{nullptr};
  database::GraphDbAccessor &db_;
  std::vector<Symbol> symbols_;
  int64_t plan_id_;
  tx::CommandId command_id_;
  std::unordered_map<int, utils::Future<distributed::PullData>> remote_pulls_;
  std::unordered_map<int, std::vector<std::vector<query::TypedValue>>>
      remote_results_;
  std::vector<int> worker_ids_;
  bool remote_pulls_initialized_ = false;

  void UpdatePullForWorker(int worker_id, Context &context) {
    remote_pulls_[worker_id] = pull_clients_->Pull(
        &db_, worker_id, plan_id_, command_id_, context.parameters_, symbols_,
        context.timestamp_, false);
  }
};

class PullRemoteCursor : public Cursor {
 public:
  PullRemoteCursor(const PullRemote &self, database::GraphDbAccessor &db)
      : self_(self),
        input_cursor_(self.input() ? self.input()->MakeCursor(db) : nullptr),
        command_id_(db.transaction().cid()),
        remote_puller_(
            // TODO: Pass in a Master GraphDb.
            &dynamic_cast<database::Master *>(&db.db())->pull_clients(), db,
            self.symbols(), self.plan_id(), command_id_) {}

  bool Pull(Frame &frame, Context &context) override {
    if (context.db_accessor_.should_abort()) throw HintedAbortError();
    remote_puller_.Initialize(context);

    bool have_remote_results = false;
    while (!have_remote_results && remote_puller_.WorkerCount() > 0) {
      if (context.db_accessor_.should_abort()) throw HintedAbortError();
      remote_puller_.Update(context);

      // Get locally stored results from workers in a round-robin fasion.
      int num_workers = remote_puller_.WorkerCount();
      for (int i = 0; i < num_workers; ++i) {
        int worker_id_index =
            (last_pulled_worker_id_index_ + i + 1) % num_workers;
        int worker_id = remote_puller_.GetWorkerId(worker_id_index);

        if (remote_puller_.HasResultsFromWorker(worker_id)) {
          last_pulled_worker_id_index_ = worker_id_index;
          have_remote_results = true;
          break;
        }
      }

      if (!have_remote_results) {
        if (!remote_puller_.HasPendingPulls()) {
          remote_puller_.ClearWorkers();
          break;
        }

        // If there are no remote results available, try to pull and return
        // local results.
        if (input_cursor_ && input_cursor_->Pull(frame, context)) {
          VLOG(10) << "[PullRemoteCursor] ["
                   << context.db_accessor_.transaction_id() << "] ["
                   << self_.plan_id() << "] [" << command_id_
                   << "] producing local results ";
          return true;
        }

        VLOG(10) << "[PullRemoteCursor] ["
                 << context.db_accessor_.transaction_id() << "] ["
                 << self_.plan_id() << "] [" << command_id_
                 << "] no results available, sleeping ";
        // If there aren't any local/remote results available, sleep.
        std::this_thread::sleep_for(
            std::chrono::microseconds(FLAGS_remote_pull_sleep_micros));
      }
    }

    // No more remote results, make sure local results get exhausted.
    if (!have_remote_results) {
      if (input_cursor_ && input_cursor_->Pull(frame, context)) {
        VLOG(10) << "[PullRemoteCursor] ["
                 << context.db_accessor_.transaction_id() << "] ["
                 << self_.plan_id() << "] [" << command_id_
                 << "] producing local results ";
        return true;
      }
      return false;
    }

    {
      int worker_id = remote_puller_.GetWorkerId(last_pulled_worker_id_index_);
      VLOG(10) << "[PullRemoteCursor] ["
               << context.db_accessor_.transaction_id() << "] ["
               << self_.plan_id() << "] [" << command_id_
               << "] producing results from worker " << worker_id;
      auto result = remote_puller_.PopResultFromWorker(worker_id);
      for (size_t i = 0; i < self_.symbols().size(); ++i) {
        frame[self_.symbols()[i]] = std::move(result[i]);
      }
    }
    return true;
  }

  void Reset() override {
    if (input_cursor_) input_cursor_->Reset();
    remote_puller_.Reset();
    last_pulled_worker_id_index_ = 0;
  }

 private:
  const PullRemote &self_;
  const std::unique_ptr<Cursor> input_cursor_;
  tx::CommandId command_id_;
  RemotePuller remote_puller_;
  int last_pulled_worker_id_index_ = 0;
};

class SynchronizeCursor : public Cursor {
 public:
  SynchronizeCursor(const Synchronize &self, database::GraphDbAccessor &db)
      : self_(self),
        pull_clients_(
            // TODO: Pass in a Master GraphDb.
            &dynamic_cast<database::Master *>(&db.db())->pull_clients()),
        updates_clients_(
            // TODO: Pass in a Master GraphDb.
            &dynamic_cast<database::Master *>(&db.db())->updates_clients()),
        updates_server_(
            // TODO: Pass in a Master GraphDb.
            &dynamic_cast<database::Master *>(&db.db())->updates_server()),
        input_cursor_(self.input()->MakeCursor(db)),
        pull_remote_cursor_(
            self.pull_remote() ? self.pull_remote()->MakeCursor(db) : nullptr),
        command_id_(db.transaction().cid()),
        master_id_(
            // TODO: Pass in a Master GraphDb.
            dynamic_cast<database::Master *>(&db.db())->WorkerId()) {}

  bool Pull(Frame &frame, Context &context) override {
    if (!initial_pull_done_) {
      InitialPull(frame, context);
      initial_pull_done_ = true;
    }
    // Yield local stuff while available.
    if (!local_frames_.empty()) {
      VLOG(10) << "[SynchronizeCursor] ["
               << context.db_accessor_.transaction_id()
               << "] producing local results";
      auto &result = local_frames_.back();
      for (size_t i = 0; i < frame.elems().size(); ++i) {
        if (self_.advance_command()) {
          query::ReconstructTypedValue(result[i]);
        }
        frame.elems()[i] = std::move(result[i]);
      }
      local_frames_.resize(local_frames_.size() - 1);
      return true;
    }

    // We're out of local stuff, yield from pull_remote if available.
    if (pull_remote_cursor_ && pull_remote_cursor_->Pull(frame, context)) {
      VLOG(10) << "[SynchronizeCursor] ["
               << context.db_accessor_.transaction_id()
               << "] producing remote results";
      return true;
    }

    return false;
  }

  void Reset() override {
    input_cursor_->Reset();
    pull_remote_cursor_->Reset();
    initial_pull_done_ = false;
    local_frames_.clear();
  }

 private:
  const Synchronize &self_;
  distributed::PullRpcClients *pull_clients_{nullptr};
  distributed::UpdatesRpcClients *updates_clients_{nullptr};
  distributed::UpdatesRpcServer *updates_server_{nullptr};
  const std::unique_ptr<Cursor> input_cursor_;
  const std::unique_ptr<Cursor> pull_remote_cursor_;
  bool initial_pull_done_{false};
  std::vector<std::vector<TypedValue>> local_frames_;
  tx::CommandId command_id_;
  int master_id_;

  void InitialPull(Frame &frame, Context &context) {
    VLOG(10) << "[SynchronizeCursor] [" << context.db_accessor_.transaction_id()
             << "] initial pull";

    // Tell all workers to accumulate, only if there is a remote pull.
    std::vector<utils::Future<distributed::PullData>> worker_accumulations;
    if (pull_remote_cursor_) {
      for (auto worker_id : pull_clients_->GetWorkerIds()) {
        if (worker_id == master_id_) continue;
        worker_accumulations.emplace_back(pull_clients_->Pull(
            &context.db_accessor_, worker_id, self_.pull_remote()->plan_id(),
            command_id_, context.parameters_, self_.pull_remote()->symbols(),
            context.timestamp_, true, 0));
      }
    }

    // Accumulate local results
    while (input_cursor_->Pull(frame, context)) {
      local_frames_.emplace_back();
      auto &local_frame = local_frames_.back();
      local_frame.reserve(frame.elems().size());
      for (auto &elem : frame.elems()) {
        local_frame.emplace_back(std::move(elem));
      }
    }

    // Wait for all workers to finish accumulation (first sync point).
    for (auto &accu : worker_accumulations) {
      switch (accu.get().pull_state) {
        case distributed::PullState::CURSOR_EXHAUSTED:
          continue;
        case distributed::PullState::CURSOR_IN_PROGRESS:
          throw QueryRuntimeException(
              "Expected exhausted cursor after remote pull accumulate");
        case distributed::PullState::SERIALIZATION_ERROR:
          throw mvcc::SerializationError(
              "Failed to perform remote accumulate due to "
              "SerializationError");
        case distributed::PullState::UPDATE_DELETED_ERROR:
          throw QueryRuntimeException(
              "Failed to perform remote accumulate due to "
              "RecordDeletedError");
        case distributed::PullState::LOCK_TIMEOUT_ERROR:
          throw utils::LockTimeoutException(
              "Failed to perform remote accumulate due to "
              "LockTimeoutException");
        case distributed::PullState::RECONSTRUCTION_ERROR:
          throw QueryRuntimeException(
              "Failed to perform remote accumulate due to "
              "ReconstructionError");
        case distributed::PullState::UNABLE_TO_DELETE_VERTEX_ERROR:
          throw RemoveAttachedVertexException();
        case distributed::PullState::HINTED_ABORT_ERROR:
          throw HintedAbortError();
        case distributed::PullState::QUERY_ERROR:
          throw QueryRuntimeException(
              "Failed to perform remote accumulate due to Query runtime "
              "error");
      }
    }

    if (self_.advance_command()) {
      context.db_accessor_.AdvanceCommand();
    }

    // Make all the workers apply their deltas.
    auto tx_id = context.db_accessor_.transaction_id();
    auto apply_futures = updates_clients_->UpdateApplyAll(master_id_, tx_id);
    updates_server_->Apply(tx_id);
    for (auto &future : apply_futures) {
      switch (future.get()) {
        case distributed::UpdateResult::SERIALIZATION_ERROR:
          throw mvcc::SerializationError(
              "Failed to apply deferred updates due to SerializationError");
        case distributed::UpdateResult::UNABLE_TO_DELETE_VERTEX_ERROR:
          throw RemoveAttachedVertexException();
        case distributed::UpdateResult::UPDATE_DELETED_ERROR:
          throw QueryRuntimeException(
              "Failed to apply deferred updates due to RecordDeletedError");
        case distributed::UpdateResult::LOCK_TIMEOUT_ERROR:
          throw utils::LockTimeoutException(
              "Failed to apply deferred update due to LockTimeoutException");
        case distributed::UpdateResult::DONE:
          break;
      }
    }

    // If the command advanced, let the workers know.
    if (self_.advance_command()) {
      auto futures = pull_clients_->NotifyAllTransactionCommandAdvanced(tx_id);
      for (auto &future : futures) future.wait();
    }
  }
};

class PullRemoteOrderByCursor : public Cursor {
 public:
  PullRemoteOrderByCursor(const PullRemoteOrderBy &self,
                          database::GraphDbAccessor &db)
      : self_(self),
        input_(self.input()->MakeCursor(db)),
        command_id_(db.transaction().cid()),
        remote_puller_(
            // TODO: Pass in a Master GraphDb.
            &dynamic_cast<database::Master *>(&db.db())->pull_clients(), db,
            self.symbols(), self.plan_id(), command_id_) {}

  bool Pull(Frame &frame, Context &context) {
    if (context.db_accessor_.should_abort()) throw HintedAbortError();
    ExpressionEvaluator evaluator(frame, &context, GraphView::OLD);

    auto evaluate_result = [this, &evaluator]() {
      std::vector<TypedValue> order_by;
      order_by.reserve(self_.order_by().size());
      for (auto expression_ptr : self_.order_by()) {
        order_by.emplace_back(expression_ptr->Accept(evaluator));
      }
      return order_by;
    };

    auto restore_frame = [&frame,
                          this](const std::vector<TypedValue> &restore_from) {
      for (size_t i = 0; i < restore_from.size(); ++i) {
        frame[self_.symbols()[i]] = restore_from[i];
      }
    };

    if (!merge_initialized_) {
      VLOG(10) << "[PullRemoteOrderBy] ["
               << context.db_accessor_.transaction_id() << "] ["
               << self_.plan_id() << "] [" << command_id_ << "] initialize";
      remote_puller_.Initialize(context);
      missing_results_from_ = remote_puller_.Workers();
      missing_master_result_ = true;
      merge_initialized_ = true;
    }

    if (missing_master_result_) {
      if (input_->Pull(frame, context)) {
        std::vector<TypedValue> output;
        output.reserve(self_.symbols().size());
        for (const Symbol &symbol : self_.symbols()) {
          output.emplace_back(frame[symbol]);
        }

        merge_.push_back(MergeResultItem{std::experimental::nullopt, output,
                                         evaluate_result()});
      }
      missing_master_result_ = false;
    }

    while (!missing_results_from_.empty()) {
      if (context.db_accessor_.should_abort()) throw HintedAbortError();
      remote_puller_.Update(context);

      bool has_all_result = true;
      for (auto &worker_id : missing_results_from_) {
        if (!remote_puller_.HasResultsFromWorker(worker_id) &&
            remote_puller_.HasPendingPullFromWorker(worker_id)) {
          has_all_result = false;
          break;
        }
      }

      if (!has_all_result) {
        VLOG(10) << "[PullRemoteOrderByCursor] ["
                 << context.db_accessor_.transaction_id() << "] ["
                 << self_.plan_id() << "] [" << command_id_
                 << "] missing results, sleep";
        // If we don't have results from all workers, sleep before continuing.
        std::this_thread::sleep_for(
            std::chrono::microseconds(FLAGS_remote_pull_sleep_micros));
        continue;
      }

      for (auto &worker_id : missing_results_from_) {
        // It is possible that the workers remote pull finished but it didn't
        // return any results. In that case, just skip it.
        if (!remote_puller_.HasResultsFromWorker(worker_id)) continue;
        auto remote_result = remote_puller_.PopResultFromWorker(worker_id);
        restore_frame(remote_result);
        merge_.push_back(
            MergeResultItem{worker_id, remote_result, evaluate_result()});
      }

      missing_results_from_.clear();
    }

    if (merge_.empty()) return false;

    auto result_it = std::min_element(
        merge_.begin(), merge_.end(), [this](const auto &lhs, const auto &rhs) {
          return self_.compare()(lhs.order_by, rhs.order_by);
        });

    restore_frame(result_it->remote_result);

    if (result_it->worker_id) {
      VLOG(10) << "[PullRemoteOrderByCursor] ["
               << context.db_accessor_.transaction_id() << "] ["
               << self_.plan_id() << "] [" << command_id_
               << "] producing results from worker "
               << result_it->worker_id.value();
      missing_results_from_.push_back(result_it->worker_id.value());
    } else {
      VLOG(10) << "[PullRemoteOrderByCursor] ["
               << context.db_accessor_.transaction_id() << "] ["
               << self_.plan_id() << "] [" << command_id_
               << "] producing local results";
      missing_master_result_ = true;
    }

    merge_.erase(result_it);
    return true;
  }

  void Reset() {
    input_->Reset();
    remote_puller_.Reset();
    merge_.clear();
    missing_results_from_.clear();
    missing_master_result_ = false;
    merge_initialized_ = false;
  }

 private:
  struct MergeResultItem {
    std::experimental::optional<int> worker_id;
    std::vector<TypedValue> remote_result;
    std::vector<TypedValue> order_by;
  };

  const PullRemoteOrderBy &self_;
  std::unique_ptr<Cursor> input_;
  tx::CommandId command_id_;
  RemotePuller remote_puller_;
  std::vector<MergeResultItem> merge_;
  std::vector<int> missing_results_from_;
  bool missing_master_result_ = false;
  bool merge_initialized_ = false;
};

}  // namespace

std::unique_ptr<Cursor> PullRemote::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<PullRemoteCursor>(*this, db);
}

std::unique_ptr<Cursor> Synchronize::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<SynchronizeCursor>(*this, db);
}

std::unique_ptr<Cursor> PullRemoteOrderBy::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<PullRemoteOrderByCursor>(*this, db);
}

}  // namespace query::plan
