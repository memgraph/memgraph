#pragma once

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "utils/likely.hpp"
#include "utils/string.hpp"

namespace query {

class TransactionEngine final {
 public:
  TransactionEngine(database::GraphDb *db, Interpreter *interpreter)
      : db_(db), interpreter_(interpreter) {}

  ~TransactionEngine() { Abort(); }

  std::pair<std::vector<std::string>, std::vector<query::AuthQuery::Privilege>>
  Interpret(const std::string &query,
            const std::map<std::string, PropertyValue> &params) {
    // Clear pending results.
    results_ = std::experimental::nullopt;

    // Check the query for transaction commands.
    auto query_upper = utils::Trim(utils::ToUpperCase(query));
    if (query_upper == "BEGIN") {
      if (in_explicit_transaction_) {
        throw QueryException("Nested transactions are not supported.");
      }
      in_explicit_transaction_ = true;
      expect_rollback_ = false;
      return {};
    } else if (query_upper == "COMMIT") {
      if (!in_explicit_transaction_) {
        throw QueryException("No current transaction to commit.");
      }
      if (expect_rollback_) {
        throw QueryException(
            "Transaction can't be committed because there was a previous "
            "error. Please invoke a rollback instead.");
      }
      Commit();
      expect_rollback_ = false;
      in_explicit_transaction_ = false;
      return {};
    } else if (query_upper == "ROLLBACK") {
      if (!in_explicit_transaction_) {
        throw QueryException("No current transaction to rollback.");
      }
      Abort();
      expect_rollback_ = false;
      in_explicit_transaction_ = false;
      return {};
    }

    // Any other query in an explicit transaction block advances the command.
    if (in_explicit_transaction_ && db_accessor_) AdvanceCommand();

    // Create a DB accessor if we don't yet have one.
    if (!db_accessor_) db_accessor_ = db_->Access();

    // Interpret the query and return the headers.
    try {
      results_.emplace((*interpreter_)(query, *db_accessor_, params,
                                       in_explicit_transaction_));
      return {results_->header(), results_->privileges()};
    } catch (const utils::BasicException &) {
      AbortCommand();
      throw;
    }
  }

  template <typename TStream>
  std::map<std::string, TypedValue> PullAll(TStream *result_stream) {
    // If we don't have any results (eg. a transaction command preceeded),
    // return an empty summary.
    if (UNLIKELY(!results_)) return {};

    // Stream all results and return the summary.
    try {
      results_->PullAll(*result_stream);
      // Make a copy of the summary because the `Commit` call will destroy the
      // `results_` object.
      auto summary = results_->summary();
      if (!in_explicit_transaction_) Commit();
      return summary;
    } catch (const utils::BasicException &) {
      AbortCommand();
      throw;
    }
  }

  void Abort() {
    results_ = std::experimental::nullopt;
    expect_rollback_ = false;
    in_explicit_transaction_ = false;
    if (!db_accessor_) return;
    db_accessor_->Abort();
    db_accessor_ = nullptr;
  }

 private:
  database::GraphDb *db_{nullptr};
  Interpreter *interpreter_{nullptr};
  std::unique_ptr<database::GraphDbAccessor> db_accessor_;
  // The `query::Interpreter::Results` object MUST be destroyed before the
  // `database::GraphDbAccessor` is destroyed because the `Results` object holds
  // references to the `GraphDb` object and will crash the database when
  // destructed if you are not careful.
  std::experimental::optional<query::Interpreter::Results> results_;
  bool in_explicit_transaction_{false};
  bool expect_rollback_{false};

  void Commit() {
    results_ = std::experimental::nullopt;
    if (!db_accessor_) return;
    db_accessor_->Commit();
    db_accessor_ = nullptr;
  }

  void AdvanceCommand() {
    results_ = std::experimental::nullopt;
    if (!db_accessor_) return;
    db_accessor_->AdvanceCommand();
  }

  void AbortCommand() {
    results_ = std::experimental::nullopt;
    if (in_explicit_transaction_) {
      expect_rollback_ = true;
    } else {
      Abort();
    }
  }
};

}  // namespace query
