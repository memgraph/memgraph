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
    auto *distributed_visitor =                                          \
        dynamic_cast<DistributedOperatorVisitor *>(&visitor);            \
    CHECK(distributed_visitor);                                          \
    if (distributed_visitor->PreVisit(*this)) {                          \
      input_->Accept(visitor);                                           \
    }                                                                    \
    return distributed_visitor->PostVisit(*this);                        \
  }

namespace query::plan {

// Create a vertex on this GraphDb and return it. Defined in operator.cpp
VertexAccessor &CreateLocalVertex(NodeAtom *node_atom, Frame &frame,
                                  Context &context);

bool PullRemote::Accept(HierarchicalLogicalOperatorVisitor &visitor) {
  auto *distributed_visitor =
      dynamic_cast<DistributedOperatorVisitor *>(&visitor);
  CHECK(distributed_visitor);
  if (distributed_visitor->PreVisit(*this)) {
    if (input_) input_->Accept(visitor);
  }
  return distributed_visitor->PostVisit(*this);
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
  auto *distributed_visitor =
      dynamic_cast<DistributedOperatorVisitor *>(&visitor);
  CHECK(distributed_visitor);
  if (distributed_visitor->PreVisit(*this)) {
    // pull_remote_ is optional here, so visit it only if we continue visiting
    // and pull_remote_ does exist.
    input_->Accept(visitor) && pull_remote_ && pull_remote_->Accept(visitor);
  }
  return distributed_visitor->PostVisit(*this);
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

ACCEPT_WITH_INPUT(DistributedExpand);

std::vector<Symbol> DistributedExpand::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(node_symbol_);
  symbols.emplace_back(edge_symbol_);
  return symbols;
}

DistributedExpandBfs::DistributedExpandBfs(
    Symbol node_symbol, Symbol edge_symbol, EdgeAtom::Direction direction,
    const std::vector<storage::EdgeType> &edge_types,
    const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
    bool existing_node, GraphView graph_view, Expression *lower_bound,
    Expression *upper_bound, const ExpandVariable::Lambda &filter_lambda)
    : ExpandCommon(node_symbol, edge_symbol, direction, edge_types, input,
                   input_symbol, existing_node, graph_view),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      filter_lambda_(filter_lambda) {}

ACCEPT_WITH_INPUT(DistributedExpandBfs);

std::vector<Symbol> DistributedExpandBfs::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(node_symbol_);
  symbols.emplace_back(edge_symbol_);
  return symbols;
}

DistributedCreateNode::DistributedCreateNode(
    const std::shared_ptr<LogicalOperator> &input, NodeAtom *node_atom,
    bool on_random_worker)
    : input_(input),
      node_atom_(node_atom),
      on_random_worker_(on_random_worker) {}

ACCEPT_WITH_INPUT(DistributedCreateNode);

std::vector<Symbol> DistributedCreateNode::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(table.at(*node_atom_->identifier_));
  return symbols;
}

DistributedCreateExpand::DistributedCreateExpand(
    NodeAtom *node_atom, EdgeAtom *edge_atom,
    const std::shared_ptr<LogicalOperator> &input, Symbol input_symbol,
    bool existing_node)
    : node_atom_(node_atom),
      edge_atom_(edge_atom),
      input_(input ? input : std::make_shared<Once>()),
      input_symbol_(input_symbol),
      existing_node_(existing_node) {}

ACCEPT_WITH_INPUT(DistributedCreateExpand);

std::vector<Symbol> DistributedCreateExpand::ModifiedSymbols(
    const SymbolTable &table) const {
  auto symbols = input_->ModifiedSymbols(table);
  symbols.emplace_back(table.at(*node_atom_->identifier_));
  symbols.emplace_back(table.at(*edge_atom_->identifier_));
  return symbols;
}

//////////////////////////////////////////////////////////////////////
// Cursors
//////////////////////////////////////////////////////////////////////

namespace {

// Helper class that wraps remote pulling for cursors that handle results from
// distributed workers. The command_id should be the command_id at the
// initialization of a cursor.
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
              "Serialization error occured during PullRemote!");
        case distributed::PullState::LOCK_TIMEOUT_ERROR:
          throw utils::LockTimeoutException(
              "LockTimeout error occured during PullRemote!");
        case distributed::PullState::UPDATE_DELETED_ERROR:
          throw QueryRuntimeException(
              "RecordDeleted error ocured during PullRemote!");
        case distributed::PullState::RECONSTRUCTION_ERROR:
          throw query::ReconstructionException();
        case distributed::PullState::UNABLE_TO_DELETE_VERTEX_ERROR:
          throw RemoveAttachedVertexException();
        case distributed::PullState::HINTED_ABORT_ERROR:
          throw HintedAbortError();
        case distributed::PullState::QUERY_ERROR:
          throw QueryRuntimeException(
              "Query runtime error occurred during PullRemote!");
      }
    }
  }

  void Shutdown() {
    // Explicitly get all of the requested RPC futures, so that we register any
    // exceptions.
    for (auto &remote_pull : remote_pulls_) {
      if (remote_pull.second.valid()) remote_pull.second.get();
    }
    remote_pulls_.clear();
  }

  void Reset() {
    worker_ids_ = pull_clients_->GetWorkerIds();
    // Remove master from the worker ids list.
    worker_ids_.erase(std::find(worker_ids_.begin(), worker_ids_.end(), 0));

    // We must clear remote_pulls before reseting cursors to make sure that all
    // outstanding remote pulls are done. Otherwise we might try to reset cursor
    // during its pull.
    for (auto &remote_pull : remote_pulls_) {
      if (remote_pull.second.valid()) remote_pull.second.get();
    }
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
    remote_pulls_[worker_id] =
        pull_clients_->Pull(&db_, worker_id, plan_id_, command_id_,
                            context.evaluation_context_, symbols_, false);
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
            self.symbols_, self.plan_id_, command_id_) {}

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
                   << self_.plan_id_ << "] [" << command_id_
                   << "] producing local results ";
          return true;
        }

        VLOG(10) << "[PullRemoteCursor] ["
                 << context.db_accessor_.transaction_id() << "] ["
                 << self_.plan_id_ << "] [" << command_id_
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
                 << self_.plan_id_ << "] [" << command_id_
                 << "] producing local results ";
        return true;
      }
      return false;
    }

    {
      int worker_id = remote_puller_.GetWorkerId(last_pulled_worker_id_index_);
      VLOG(10) << "[PullRemoteCursor] ["
               << context.db_accessor_.transaction_id() << "] ["
               << self_.plan_id_ << "] [" << command_id_
               << "] producing results from worker " << worker_id;
      auto result = remote_puller_.PopResultFromWorker(worker_id);
      for (size_t i = 0; i < self_.symbols_.size(); ++i) {
        frame[self_.symbols_[i]] = std::move(result[i]);
      }
    }
    return true;
  }

  void Shutdown() override {
    if (input_cursor_) input_cursor_->Reset();
    remote_puller_.Shutdown();
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
            self.pull_remote_ ? self.pull_remote_->MakeCursor(db) : nullptr),
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
        if (self_.advance_command_) {
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

  void Shutdown() override {
    input_cursor_->Shutdown();
    if (pull_remote_cursor_) pull_remote_cursor_->Shutdown();
  }

  void Reset() override {
    input_cursor_->Reset();
    if (pull_remote_cursor_) pull_remote_cursor_->Reset();
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
            &context.db_accessor_, worker_id, self_.pull_remote_->plan_id_,
            command_id_, context.evaluation_context_,
            self_.pull_remote_->symbols_, true, 0));
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
              "Expected exhausted cursor after remote pull accumulate!");
        case distributed::PullState::SERIALIZATION_ERROR:
          throw mvcc::SerializationError(
              "Failed to perform remote accumulate due to "
              "SerializationError!");
        case distributed::PullState::UPDATE_DELETED_ERROR:
          throw QueryRuntimeException(
              "Failed to perform remote accumulate due to "
              "RecordDeletedError!");
        case distributed::PullState::LOCK_TIMEOUT_ERROR:
          throw utils::LockTimeoutException(
              "Failed to perform remote accumulate due to "
              "LockTimeoutException!");
        case distributed::PullState::RECONSTRUCTION_ERROR:
          throw QueryRuntimeException(
              "Failed to perform remote accumulate due to "
              "ReconstructionError!");
        case distributed::PullState::UNABLE_TO_DELETE_VERTEX_ERROR:
          throw RemoveAttachedVertexException();
        case distributed::PullState::HINTED_ABORT_ERROR:
          throw HintedAbortError();
        case distributed::PullState::QUERY_ERROR:
          throw QueryRuntimeException(
              "Failed to perform remote accumulate due to Query runtime "
              "error!");
      }
    }

    if (self_.advance_command_) {
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
              "Failed to apply deferred updates due to SerializationError!");
        case distributed::UpdateResult::UNABLE_TO_DELETE_VERTEX_ERROR:
          throw RemoveAttachedVertexException();
        case distributed::UpdateResult::UPDATE_DELETED_ERROR:
          throw QueryRuntimeException(
              "Failed to apply deferred updates due to RecordDeletedError!");
        case distributed::UpdateResult::LOCK_TIMEOUT_ERROR:
          throw utils::LockTimeoutException(
              "Failed to apply deferred update due to LockTimeoutException!");
        case distributed::UpdateResult::DONE:
          break;
      }
    }

    // If the command advanced, let the workers know.
    if (self_.advance_command_) {
      auto futures = pull_clients_->NotifyAllTransactionCommandAdvanced(tx_id);
      for (auto &future : futures) future.get();
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
            self.symbols_, self.plan_id_, command_id_) {}

  bool Pull(Frame &frame, Context &context) override {
    if (context.db_accessor_.should_abort()) throw HintedAbortError();
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, GraphView::OLD);

    auto evaluate_result = [this, &evaluator]() {
      std::vector<TypedValue> order_by;
      order_by.reserve(self_.order_by_.size());
      for (auto expression_ptr : self_.order_by_) {
        order_by.emplace_back(expression_ptr->Accept(evaluator));
      }
      return order_by;
    };

    auto restore_frame = [&frame,
                          this](const std::vector<TypedValue> &restore_from) {
      for (size_t i = 0; i < restore_from.size(); ++i) {
        frame[self_.symbols_[i]] = restore_from[i];
      }
    };

    if (!merge_initialized_) {
      VLOG(10) << "[PullRemoteOrderBy] ["
               << context.db_accessor_.transaction_id() << "] ["
               << self_.plan_id_ << "] [" << command_id_ << "] initialize";
      remote_puller_.Initialize(context);
      missing_results_from_ = remote_puller_.Workers();
      missing_master_result_ = true;
      merge_initialized_ = true;
    }

    if (missing_master_result_) {
      if (input_->Pull(frame, context)) {
        std::vector<TypedValue> output;
        output.reserve(self_.symbols_.size());
        for (const Symbol &symbol : self_.symbols_) {
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
                 << self_.plan_id_ << "] [" << command_id_
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
          return self_.compare_(lhs.order_by, rhs.order_by);
        });

    restore_frame(result_it->remote_result);

    if (result_it->worker_id) {
      VLOG(10) << "[PullRemoteOrderByCursor] ["
               << context.db_accessor_.transaction_id() << "] ["
               << self_.plan_id_ << "] [" << command_id_
               << "] producing results from worker "
               << result_it->worker_id.value();
      missing_results_from_.push_back(result_it->worker_id.value());
    } else {
      VLOG(10) << "[PullRemoteOrderByCursor] ["
               << context.db_accessor_.transaction_id() << "] ["
               << self_.plan_id_ << "] [" << command_id_
               << "] producing local results";
      missing_master_result_ = true;
    }

    merge_.erase(result_it);
    return true;
  }

  void Shutdown() override {
    input_->Shutdown();
    remote_puller_.Shutdown();
  }

  void Reset() override {
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

class DistributedExpandCursor : public query::plan::Cursor {
 public:
  DistributedExpandCursor(const DistributedExpand *self,
                          database::GraphDbAccessor *db)
      : input_cursor_(self->input()->MakeCursor(*db)), self_(self) {}

  bool Pull(Frame &frame, Context &context) override {
    // A helper function for expanding a node from an edge.
    auto pull_node = [this, &frame](const EdgeAccessor &new_edge,
                                    EdgeAtom::Direction direction) {
      if (self_->existing_node_) return;
      switch (direction) {
        case EdgeAtom::Direction::IN:
          frame[self_->node_symbol_] = new_edge.from();
          break;
        case EdgeAtom::Direction::OUT:
          frame[self_->node_symbol_] = new_edge.to();
          break;
        case EdgeAtom::Direction::BOTH:
          LOG(FATAL) << "Must indicate exact expansion direction here";
      }
    };

    auto push_future_edge = [this, &frame](auto edge, auto direction) {
      auto edge_to = std::async(std::launch::async, [edge, direction]() {
        if (direction == EdgeAtom::Direction::IN)
          return std::make_pair(edge, edge.from());
        if (direction == EdgeAtom::Direction::OUT)
          return std::make_pair(edge, edge.to());
        LOG(FATAL) << "Must indicate exact expansion direction here";
      });
      future_expands_.emplace_back(
          FutureExpand{utils::make_future(std::move(edge_to)), frame.elems()});
    };

    auto find_ready_future = [this]() {
      return std::find_if(
          future_expands_.begin(), future_expands_.end(),
          [](const auto &future) { return future.edge_to.IsReady(); });
    };

    auto put_future_edge_on_frame = [this, &frame](auto &future) {
      auto edge_to = future.edge_to.get();
      frame.elems() = future.frame_elems;
      frame[self_->edge_symbol_] = edge_to.first;
      frame[self_->node_symbol_] = edge_to.second;
    };

    while (true) {
      if (context.db_accessor_.should_abort()) throw HintedAbortError();
      // Try to get any remote edges we may have available first. If we yielded
      // all of the local edges first, we may accumulate large amounts of future
      // edges.
      {
        auto future_it = find_ready_future();
        if (future_it != future_expands_.end()) {
          // Backup the current frame (if we haven't done so already) before
          // putting the future edge.
          if (last_frame_.empty()) last_frame_ = frame.elems();
          put_future_edge_on_frame(*future_it);
          // Erase the future and return true to yield the result.
          future_expands_.erase(future_it);
          return true;
        }
      }
      // In case we have replaced the frame with the one for a future edge,
      // restore it.
      if (!last_frame_.empty()) {
        frame.elems() = last_frame_;
        last_frame_.clear();
      }
      // attempt to get a value from the incoming edges
      if (in_edges_ && *in_edges_it_ != in_edges_->end()) {
        auto edge = *(*in_edges_it_)++;
        if (edge.address().is_local() || self_->existing_node_) {
          frame[self_->edge_symbol_] = edge;
          pull_node(edge, EdgeAtom::Direction::IN);
          return true;
        } else {
          push_future_edge(edge, EdgeAtom::Direction::IN);
          continue;
        }
      }

      // attempt to get a value from the outgoing edges
      if (out_edges_ && *out_edges_it_ != out_edges_->end()) {
        auto edge = *(*out_edges_it_)++;
        // when expanding in EdgeAtom::Direction::BOTH directions
        // we should do only one expansion for cycles, and it was
        // already done in the block above
        if (self_->direction_ == EdgeAtom::Direction::BOTH && edge.is_cycle())
          continue;
        if (edge.address().is_local() || self_->existing_node_) {
          frame[self_->edge_symbol_] = edge;
          pull_node(edge, EdgeAtom::Direction::OUT);
          return true;
        } else {
          push_future_edge(edge, EdgeAtom::Direction::OUT);
          continue;
        }
      }

      // if we are here, either the edges have not been initialized,
      // or they have been exhausted. attempt to initialize the edges,
      // if the input is exhausted
      if (!InitEdges(frame, context)) {
        // We are done with local and remote edges so return false.
        if (future_expands_.empty()) return false;
        // We still need to yield remote edges.
        auto future_it = find_ready_future();
        if (future_it != future_expands_.end()) {
          put_future_edge_on_frame(*future_it);
          // Erase the future and return true to yield the result.
          future_expands_.erase(future_it);
          return true;
        }
        // We are still waiting for future edges, so sleep and fallthrough to
        // continue the loop.
        std::this_thread::sleep_for(
            std::chrono::microseconds(FLAGS_remote_pull_sleep_micros));
      }

      // we have re-initialized the edges, continue with the loop
    }
  }

  void Shutdown() override {
    input_cursor_->Shutdown();
    // Explicitly get all of the requested RPC futures, so that we register any
    // exceptions.
    for (auto &future_expand : future_expands_) {
      if (future_expand.edge_to.valid()) future_expand.edge_to.get();
    }
    future_expands_.clear();
  }

  void Reset() override {
    input_cursor_->Reset();
    in_edges_ = std::experimental::nullopt;
    in_edges_it_ = std::experimental::nullopt;
    out_edges_ = std::experimental::nullopt;
    out_edges_it_ = std::experimental::nullopt;
    // Explicitly get all of the requested RPC futures, so that we register any
    // exceptions.
    for (auto &future_expand : future_expands_) {
      if (future_expand.edge_to.valid()) future_expand.edge_to.get();
    }
    future_expands_.clear();
    last_frame_.clear();
  }

  bool InitEdges(Frame &frame, Context &context) {
    // Input Vertex could be null if it is created by a failed optional match.
    // In those cases we skip that input pull and continue with the next.
    while (true) {
      if (!input_cursor_->Pull(frame, context)) return false;
      TypedValue &vertex_value = frame[self_->input_symbol_];

      // Null check due to possible failed optional match.
      if (vertex_value.IsNull()) continue;

      ExpectType(self_->input_symbol_, vertex_value, TypedValue::Type::Vertex);
      auto &vertex = vertex_value.Value<VertexAccessor>();
      SwitchAccessor(vertex, self_->graph_view_);

      auto direction = self_->direction_;
      if (direction == EdgeAtom::Direction::IN ||
          direction == EdgeAtom::Direction::BOTH) {
        if (self_->existing_node_) {
          TypedValue &existing_node = frame[self_->node_symbol_];
          // old_node_value may be Null when using optional matching
          if (!existing_node.IsNull()) {
            ExpectType(self_->node_symbol_, existing_node,
                       TypedValue::Type::Vertex);
            in_edges_.emplace(
                vertex.in(existing_node.ValueVertex(), &self_->edge_types_));
          }
        } else {
          in_edges_.emplace(vertex.in(&self_->edge_types_));
        }
        in_edges_it_.emplace(in_edges_->begin());
      }

      if (direction == EdgeAtom::Direction::OUT ||
          direction == EdgeAtom::Direction::BOTH) {
        if (self_->existing_node_) {
          TypedValue &existing_node = frame[self_->node_symbol_];
          // old_node_value may be Null when using optional matching
          if (!existing_node.IsNull()) {
            ExpectType(self_->node_symbol_, existing_node,
                       TypedValue::Type::Vertex);
            out_edges_.emplace(
                vertex.out(existing_node.ValueVertex(), &self_->edge_types_));
          }
        } else {
          out_edges_.emplace(vertex.out(&self_->edge_types_));
        }
        out_edges_it_.emplace(out_edges_->begin());
      }

      return true;
    }
  }

 private:
  struct FutureExpand {
    utils::Future<std::pair<EdgeAccessor, VertexAccessor>> edge_to;
    std::vector<TypedValue> frame_elems;
  };

  std::unique_ptr<query::plan::Cursor> input_cursor_;
  const DistributedExpand *self_{nullptr};
  // The iterable over edges and the current edge iterator are referenced via
  // optional because they can not be initialized in the constructor of
  // this class. They are initialized once for each pull from the input.
  std::experimental::optional<DistributedExpand::InEdgeT> in_edges_;
  std::experimental::optional<DistributedExpand::InEdgeIteratorT> in_edges_it_;
  std::experimental::optional<DistributedExpand::OutEdgeT> out_edges_;
  std::experimental::optional<DistributedExpand::OutEdgeIteratorT>
      out_edges_it_;
  // Stores the last frame before we yield the frame for future edge. It needs
  // to be restored afterward.
  std::vector<TypedValue> last_frame_;
  // Edges which are being asynchronously fetched from a remote worker.
  // NOTE: This should be destructed first to ensure that no invalid
  // references or pointers exists to other objects of this class.
  std::vector<FutureExpand> future_expands_;
};

class DistributedExpandBfsCursor : public query::plan::Cursor {
 public:
  DistributedExpandBfsCursor(const DistributedExpandBfs &self,
                             database::GraphDbAccessor &db)
      : self_(self), db_(db), input_cursor_(self_.input()->MakeCursor(db)) {
    // TODO: Pass in a DistributedGraphDb.
    if (auto *distributed_db =
            dynamic_cast<database::DistributedGraphDb *>(&db.db())) {
      bfs_subcursor_clients_ = &distributed_db->bfs_subcursor_clients();
    }
    CHECK(bfs_subcursor_clients_);
    subcursor_ids_ = bfs_subcursor_clients_->CreateBfsSubcursors(
        db_.transaction_id(), self_.direction_, self_.edge_types_,
        self_.graph_view_);
    bfs_subcursor_clients_->RegisterSubcursors(subcursor_ids_);
    VLOG(10) << "BFS subcursors initialized";
    pull_pos_ = subcursor_ids_.end();
  }

  ~DistributedExpandBfsCursor() {
    VLOG(10) << "Removing BFS subcursors";
    bfs_subcursor_clients_->RemoveBfsSubcursors(subcursor_ids_);
  }

  bool Pull(Frame &frame, Context &context) override {
    // TODO(mtomic): lambda filtering in distributed
    if (self_.filter_lambda_.expression) {
      throw utils::NotYetImplemented("lambda filtering in distributed BFS");
    }

    // Evaluator for the filtering condition and expansion depth.
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, self_.graph_view_);

    while (true) {
      if (context.db_accessor_.should_abort()) throw HintedAbortError();
      TypedValue last_vertex;

      if (!skip_rest_) {
        if (current_depth_ >= lower_bound_) {
          for (; pull_pos_ != subcursor_ids_.end(); ++pull_pos_) {
            auto vertex = bfs_subcursor_clients_->Pull(pull_pos_->first,
                                                       pull_pos_->second, &db_);
            if (vertex) {
              last_vertex = *vertex;
              SwitchAccessor(last_vertex.ValueVertex(), self_.graph_view_);
              break;
            }
            VLOG(10) << "Nothing to pull from " << pull_pos_->first;
          }
        }

        if (last_vertex.IsVertex()) {
          // Handle existence flag
          if (self_.existing_node_) {
            TypedValue &node = frame[self_.node_symbol_];
            // Due to optional matching the existing node could be null
            if (node.IsNull() || (node != last_vertex).ValueBool()) continue;
            // There is no point in traversing the rest of the graph because BFS
            // can find only one path to a certain node.
            skip_rest_ = true;
          } else {
            frame[self_.node_symbol_] = last_vertex;
          }

          VLOG(10) << "Expanded to vertex: " << last_vertex;

          // Reconstruct path
          std::vector<TypedValue> edges;

          // During path reconstruction, edges crossing worker boundary are
          // obtained from edge owner to reduce network traffic. If the last
          // worker queried for its path segment owned the crossing edge,
          // `current_vertex_addr` will be set. Otherwise, `current_edge_addr`
          // will be set.
          std::experimental::optional<storage::VertexAddress>
              current_vertex_addr = last_vertex.ValueVertex().GlobalAddress();
          std::experimental::optional<storage::EdgeAddress> current_edge_addr;

          while (true) {
            DCHECK(static_cast<bool>(current_edge_addr) ^
                   static_cast<bool>(current_vertex_addr))
                << "Exactly one of `current_edge_addr` or "
                   "`current_vertex_addr` "
                   "should be set during path reconstruction";
            auto ret = current_edge_addr
                           ? bfs_subcursor_clients_->ReconstructPath(
                                 subcursor_ids_, *current_edge_addr, &db_)
                           : bfs_subcursor_clients_->ReconstructPath(
                                 subcursor_ids_, *current_vertex_addr, &db_);
            edges.insert(edges.end(), ret.edges.begin(), ret.edges.end());
            current_vertex_addr = ret.next_vertex;
            current_edge_addr = ret.next_edge;
            if (!current_vertex_addr && !current_edge_addr) break;
          }
          std::reverse(edges.begin(), edges.end());
          for (auto &edge : edges)
            SwitchAccessor(edge.ValueEdge(), self_.graph_view_);
          frame[self_.edge_symbol_] = std::move(edges);
          return true;
        }

        // We're done pulling for this level
        pull_pos_ = subcursor_ids_.begin();

        // Try to expand again
        if (current_depth_ < upper_bound_) {
          VLOG(10) << "Trying to expand again...";
          current_depth_++;
          bfs_subcursor_clients_->PrepareForExpand(subcursor_ids_, false);
          if (bfs_subcursor_clients_->ExpandLevel(subcursor_ids_)) {
            continue;
          }
        }
      }

      VLOG(10) << "Trying to get a new source...";
      // We're done with this source, try getting a new one
      if (!input_cursor_->Pull(frame, context)) return false;

      auto vertex_value = frame[self_.input_symbol_];

      // It is possible that the vertex is Null due to optional matching.
      if (vertex_value.IsNull()) continue;

      auto vertex = vertex_value.ValueVertex();
      lower_bound_ = self_.lower_bound_
                         ? EvaluateInt(&evaluator, self_.lower_bound_,
                                       "Min depth in breadth-first expansion")
                         : 1;
      upper_bound_ = self_.upper_bound_
                         ? EvaluateInt(&evaluator, self_.upper_bound_,
                                       "Max depth in breadth-first expansion")
                         : std::numeric_limits<int64_t>::max();
      skip_rest_ = false;

      if (upper_bound_ < 1) {
        throw QueryRuntimeException(
            "Max depth in breadth-first expansion must be at least 1");
      }

      VLOG(10) << "Starting BFS from " << vertex << " with limits "
               << lower_bound_ << ".." << upper_bound_;
      bfs_subcursor_clients_->PrepareForExpand(subcursor_ids_, true);
      bfs_subcursor_clients_->SetSource(subcursor_ids_, vertex.GlobalAddress());
      current_depth_ = 1;
    }
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override {
    input_cursor_->Reset();
    bfs_subcursor_clients_->ResetSubcursors(subcursor_ids_);
    pull_pos_ = subcursor_ids_.end();
  }

 private:
  const DistributedExpandBfs &self_;
  database::GraphDbAccessor &db_;
  distributed::BfsRpcClients *bfs_subcursor_clients_{nullptr};
  std::unique_ptr<query::plan::Cursor> input_cursor_;

  // Depth bounds. Calculated on each pull from the input, the initial value
  // is irrelevant.
  int64_t lower_bound_{-1};
  int64_t upper_bound_{-1};

  // When set to true, expansion is restarted from a new source.
  bool skip_rest_{false};

  // Current depth. Reset for each new expansion, the initial value is
  // irrelevant.
  int current_depth_{-1};

  // Map from worker IDs to their corresponding subcursors.
  std::unordered_map<int16_t, int64_t> subcursor_ids_;

  // Next worker master should try pulling from.
  std::unordered_map<int16_t, int64_t>::iterator pull_pos_;
};

// Returns a random worker id. Worker ID is obtained from the Db.
int RandomWorkerId(const database::DistributedGraphDb &db) {
  thread_local std::mt19937 gen_{std::random_device{}()};
  thread_local std::uniform_int_distribution<int> rand_;

  auto worker_ids = db.GetWorkerIds();
  return worker_ids[rand_(gen_) % worker_ids.size()];
}

// Creates a vertex on the GraphDb with the given worker_id. Can be this worker.
VertexAccessor &CreateVertexOnWorker(int worker_id, NodeAtom *node_atom,
                                     Frame &frame, Context &context) {
  auto &dba = context.db_accessor_;

  int current_worker_id = 0;
  // TODO: Figure out a better solution.
  if (auto *distributed_db =
          dynamic_cast<database::DistributedGraphDb *>(&dba.db())) {
    current_worker_id = distributed_db->WorkerId();
  } else {
    CHECK(dynamic_cast<database::SingleNode *>(&dba.db()));
  }

  if (worker_id == current_worker_id)
    return CreateLocalVertex(node_atom, frame, context);

  std::unordered_map<storage::Property, PropertyValue> properties;

  // Evaluator should use the latest accessors, as modified in this query, when
  // setting properties on new nodes.
  ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                context.evaluation_context_,
                                &context.db_accessor_, GraphView::NEW);
  for (auto &kv : node_atom->properties_) {
    auto value = kv.second->Accept(evaluator);
    if (!value.IsPropertyValue()) {
      throw QueryRuntimeException("'{}' cannot be used as a property value.",
                                  value.type());
    }
    properties.emplace(kv.first.second, std::move(value));
  }

  auto new_node =
      database::InsertVertexIntoRemote(&dba, worker_id, node_atom->labels_,
                                       properties, std::experimental::nullopt);
  frame[context.symbol_table_.at(*node_atom->identifier_)] = new_node;
  return frame[context.symbol_table_.at(*node_atom->identifier_)].ValueVertex();
}

class DistributedCreateNodeCursor : public query::plan::Cursor {
 public:
  DistributedCreateNodeCursor(const DistributedCreateNode *self,
                              database::GraphDbAccessor *dba)
      : input_cursor_(self->input()->MakeCursor(*dba)),
        // TODO: Replace this with some other mechanism
        db_(dynamic_cast<database::DistributedGraphDb *>(&dba->db())),
        node_atom_(self->node_atom_),
        on_random_worker_(self->on_random_worker_) {
    CHECK(db_);
    CHECK(node_atom_);
  }

  bool Pull(Frame &frame, Context &context) override {
    if (input_cursor_->Pull(frame, context)) {
      if (on_random_worker_) {
        CreateVertexOnWorker(RandomWorkerId(*db_), node_atom_, frame, context);
      } else {
        CreateLocalVertex(node_atom_, frame, context);
      }
      return true;
    }
    return false;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override { input_cursor_->Reset(); }

 private:
  std::unique_ptr<query::plan::Cursor> input_cursor_;
  database::DistributedGraphDb *db_{nullptr};
  NodeAtom *node_atom_{nullptr};
  bool on_random_worker_{false};
};

class DistributedCreateExpandCursor : public query::plan::Cursor {
 public:
  DistributedCreateExpandCursor(const DistributedCreateExpand *self,
                                database::GraphDbAccessor *dba)
      : input_cursor_(self->input()->MakeCursor(*dba)),
        self_(self),
        // TODO: Replace this with some other mechanism
        db_(dynamic_cast<database::DistributedGraphDb *>(&dba->db())) {
    CHECK(db_);
  }

  bool Pull(Frame &frame, Context &context) override {
    if (!input_cursor_->Pull(frame, context)) return false;

    // get the origin vertex
    TypedValue &vertex_value = frame[self_->input_symbol_];
    ExpectType(self_->input_symbol_, vertex_value, TypedValue::Type::Vertex);
    auto &v1 = vertex_value.Value<VertexAccessor>();

    // Similarly to CreateNode, newly created edges and nodes should use the
    // latest accesors.
    ExpressionEvaluator evaluator(&frame, context.symbol_table_,
                                  context.evaluation_context_,
                                  &context.db_accessor_, GraphView::NEW);
    // E.g. we pickup new properties: `CREATE (n {p: 42}) -[:r {ep: n.p}]-> ()`
    v1.SwitchNew();

    // get the destination vertex (possibly an existing node)
    auto &v2 = OtherVertex(v1.GlobalAddress().worker_id(), frame, context);
    v2.SwitchNew();

    auto *dba = &context.db_accessor_;
    // create an edge between the two nodes
    switch (self_->edge_atom_->direction_) {
      case EdgeAtom::Direction::IN:
        CreateEdge(&v2, &v1, &frame, context.symbol_table_, &evaluator, dba);
        break;
      case EdgeAtom::Direction::OUT:
        CreateEdge(&v1, &v2, &frame, context.symbol_table_, &evaluator, dba);
        break;
      case EdgeAtom::Direction::BOTH:
        // in the case of an undirected CreateExpand we choose an arbitrary
        // direction. this is used in the MERGE clause
        // it is not allowed in the CREATE clause, and the semantic
        // checker needs to ensure it doesn't reach this point
        CreateEdge(&v1, &v2, &frame, context.symbol_table_, &evaluator, dba);
    }

    return true;
  }

  void Shutdown() override { input_cursor_->Shutdown(); }

  void Reset() override { input_cursor_->Reset(); }

  VertexAccessor &OtherVertex(int worker_id, Frame &frame, Context &context) {
    if (self_->existing_node_) {
      const auto &dest_node_symbol =
          context.symbol_table_.at(*self_->node_atom_->identifier_);
      TypedValue &dest_node_value = frame[dest_node_symbol];
      ExpectType(dest_node_symbol, dest_node_value, TypedValue::Type::Vertex);
      return dest_node_value.Value<VertexAccessor>();
    } else {
      return CreateVertexOnWorker(worker_id, self_->node_atom_, frame,
                                  context);
    }
  }

  void CreateEdge(VertexAccessor *from, VertexAccessor *to, Frame *frame,
                  const SymbolTable &symbol_table,
                  ExpressionEvaluator *evaluator,
                  database::GraphDbAccessor *dba) {
    EdgeAccessor edge =
        dba->InsertEdge(*from, *to, self_->edge_atom_->edge_types_[0]);
    for (auto kv : self_->edge_atom_->properties_)
      PropsSetChecked(&edge, kv.first.second, kv.second->Accept(*evaluator));
    (*frame)[symbol_table.at(*self_->edge_atom_->identifier_)] = edge;
  }

 private:
  std::unique_ptr<query::plan::Cursor> input_cursor_;
  const DistributedCreateExpand *self_{nullptr};
  database::DistributedGraphDb *db_{nullptr};
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

std::unique_ptr<Cursor> DistributedExpand::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<DistributedExpandCursor>(this, &db);
}

std::unique_ptr<Cursor> DistributedExpandBfs::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<DistributedExpandBfsCursor>(*this, db);
}

std::unique_ptr<Cursor> DistributedCreateNode::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<DistributedCreateNodeCursor>(this, &db);
}

std::unique_ptr<Cursor> DistributedCreateExpand::MakeCursor(
    database::GraphDbAccessor &db) const {
  return std::make_unique<DistributedCreateExpandCursor>(this, &db);
}

}  // namespace query::plan
