// Copyright 2024 Memgraph Ltd.
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

#include "utils/exceptions.hpp"
#include "utils/message.hpp"

#include <fmt/core.h>
#include <fmt/format.h>

namespace memgraph::query {

/**
 * @brief Base class of all query language related exceptions. All exceptions
 * derived from this one will be interpreted as ClientError-s, i. e. if client
 * executes same query again without making modifications to the database data,
 * query will fail again.
 */
class QueryException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(QueryException)
};

/**
 * @brief Base class of all query language related exceptions which can be retried.
 * All exceptions derived from this one will be interpreted as TransientError-s,
 * i.e. client will be encouraged to retry the queries.
 */
class RetryBasicException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(RetryBasicException)
};

class LexingException : public QueryException {
 public:
  using QueryException::QueryException;
  LexingException() : QueryException("") {}
  SPECIALIZE_GET_EXCEPTION_NAME(LexingException)
};

class SyntaxException : public QueryException {
 public:
  using QueryException::QueryException;
  SyntaxException() : QueryException("") {}
  SPECIALIZE_GET_EXCEPTION_NAME(SyntaxException)
};

// TODO: Figure out what information to put in exception.
// Error reporting is tricky since we get stripped query and position of error
// in original query is not same as position of error in stripped query. Most
// correct approach would be to do semantic analysis with original query even
// for already hashed queries, but that has obvious performance issues. Other
// approach would be to report some of the semantic errors in runtime of the
// query and only report line numbers of semantic errors (not position in the
// line) if multiple line strings are not allowed by grammar. We could also
// print whole line that contains error instead of specifying line number.
class SemanticException : public QueryException {
 public:
  using QueryException::QueryException;
  SemanticException() : QueryException("") {}
  SPECIALIZE_GET_EXCEPTION_NAME(SemanticException)
};

class UnboundVariableError : public SemanticException {
 public:
  explicit UnboundVariableError(const std::string &name) : SemanticException("Unbound variable: " + name + ".") {}
  SPECIALIZE_GET_EXCEPTION_NAME(UnboundVariableError)
};

class RedeclareVariableError : public SemanticException {
 public:
  explicit RedeclareVariableError(const std::string &name) : SemanticException("Redeclaring variable: " + name + ".") {}
  SPECIALIZE_GET_EXCEPTION_NAME(RedeclareVariableError)
};

class TypeMismatchError : public SemanticException {
 public:
  TypeMismatchError(const std::string &name, const std::string &datum, const std::string &expected)
      : SemanticException(fmt::format("Type mismatch: {} already defined as {}, expected {}.", name, datum, expected)) {
  }
  SPECIALIZE_GET_EXCEPTION_NAME(TypeMismatchError)
};

class UnprovidedParameterError : public QueryException {
 public:
  using QueryException::QueryException;
  SPECIALIZE_GET_EXCEPTION_NAME(UnprovidedParameterError)
};

class MulticommandTxException : public QueryException {
 public:
  explicit MulticommandTxException(std::string_view query)
      : QueryException(memgraph::utils::MessageWithLink(
            "{} is not allowed in multicommand transactions. A multicommand transaction, also known as an "
            "explicit transaction, groups multiple commands into a single atomic operation. Instead, please use an "
            "implicit transaction, also knwon as an auto committing transaction, in order to execute this particular "
            "query.",
            query, "https://memgraph.com/docs")) {}
  SPECIALIZE_GET_EXCEPTION_NAME(MulticommandTxException)
};

class ProfileInMulticommandTxException : public MulticommandTxException {
 public:
  ProfileInMulticommandTxException() : MulticommandTxException("Query profiling") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ProfileInMulticommandTxException)
};

class IndexInMulticommandTxException : public MulticommandTxException {
 public:
  IndexInMulticommandTxException() : MulticommandTxException("Index manipulation") {}
  SPECIALIZE_GET_EXCEPTION_NAME(IndexInMulticommandTxException)
};

class EdgeIndexDisabledPropertiesOnEdgesException : public QueryException {
 public:
  EdgeIndexDisabledPropertiesOnEdgesException()
      : QueryException("Edge indices are allowed only if properties are allowed on edges.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(EdgeIndexDisabledPropertiesOnEdgesException)
};

class SchemaAssertInMulticommandTxException : public MulticommandTxException {
 public:
  SchemaAssertInMulticommandTxException() : MulticommandTxException("Schema-related procedures call") {}
  SPECIALIZE_GET_EXCEPTION_NAME(SchemaAssertInMulticommandTxException)
};

class ConstraintInMulticommandTxException : public MulticommandTxException {
 public:
  ConstraintInMulticommandTxException() : MulticommandTxException("Constraint manipulation") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConstraintInMulticommandTxException)
};

class InfoInMulticommandTxException : public MulticommandTxException {
 public:
  InfoInMulticommandTxException() : MulticommandTxException("Storage information query") {}
  SPECIALIZE_GET_EXCEPTION_NAME(InfoInMulticommandTxException)
};

class UserAlreadyExistsException : public QueryException {
 public:
  using QueryException::QueryException;
  SPECIALIZE_GET_EXCEPTION_NAME(UserAlreadyExistsException)
};

/**
 * An exception for an illegal operation that can not be detected
 * before the query starts executing over data.
 */
class QueryRuntimeException : public QueryException {
 public:
  using QueryException::QueryException;
  SPECIALIZE_GET_EXCEPTION_NAME(QueryRuntimeException)
};

enum class AbortReason : uint8_t {
  NO_ABORT = 0,

  // transaction has been requested to terminate, ie. "TERMINATE TRANSACTIONS ..."
  TERMINATED = 1,

  // server is gracefully shutting down
  SHUTDOWN = 2,

  // the transaction timeout has been reached. Either via "--query-execution-timeout-sec", or a per-transaction timeout
  TIMEOUT = 3,
};

// This one is inherited from RetryBasicException and will be treated as
// TransientError, i. e. client will be encouraged to retry execution because it
// could succeed if executed again.
class HintedAbortError : public RetryBasicException {
 public:
  explicit HintedAbortError(AbortReason reason) : RetryBasicException(AsMsg(reason)), reason_{reason} {}
  SPECIALIZE_GET_EXCEPTION_NAME(HintedAbortError)

  auto Reason() const -> AbortReason { return reason_; }

 private:
  static auto AsMsg(AbortReason reason) -> std::string_view {
    using namespace std::string_view_literals;
    switch (reason) {
      case AbortReason::TERMINATED:
        return "Transaction was asked to abort by another user."sv;
      case AbortReason::SHUTDOWN:
        return "Transaction was asked to abort because of database shutdown."sv;
      case AbortReason::TIMEOUT:
        return "Transaction was asked to abort because of transaction timeout."sv;
      default:
        // should never happen
        return "Transaction was asked to abort for an unknown reason."sv;
    }
  }
  AbortReason reason_;
};

class ExplicitTransactionUsageException : public QueryRuntimeException {
 public:
  using QueryRuntimeException::QueryRuntimeException;
  SPECIALIZE_GET_EXCEPTION_NAME(ExplicitTransactionUsageException)
};

class DatabaseContextRequiredException : public QueryRuntimeException {
 public:
  using QueryRuntimeException::QueryRuntimeException;
  SPECIALIZE_GET_EXCEPTION_NAME(DatabaseContextRequiredException)
};

class ConcurrentSystemQueriesException : public QueryRuntimeException {
 public:
  using QueryRuntimeException::QueryRuntimeException;
  SPECIALIZE_GET_EXCEPTION_NAME(ConcurrentSystemQueriesException)
};

class WriteVertexOperationInEdgeImportModeException : public QueryException {
 public:
  WriteVertexOperationInEdgeImportModeException()
      : QueryException("Write operations on vertices are forbidden while the edge import mode is active.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(WriteVertexOperationInEdgeImportModeException)
};

// This one is inherited from BasicException and will be treated as
// TransientError, i. e. client will be encouraged to retry execution because it
// could succeed if executed again.
class TransactionSerializationException : public RetryBasicException {
 public:
  TransactionSerializationException()
      : RetryBasicException(
            "Cannot resolve conflicting transactions. You can retry this transaction when the conflicting transaction "
            "is finished") {}
  SPECIALIZE_GET_EXCEPTION_NAME(TransactionSerializationException)
};

class ReconstructionException : public QueryException {
 public:
  ReconstructionException()
      : QueryException(
            "Record invalid after WITH clause. Most likely deleted by a "
            "preceeding DELETE.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ReconstructionException)
};

class RemoveAttachedVertexException : public QueryRuntimeException {
 public:
  RemoveAttachedVertexException()
      : QueryRuntimeException(
            "Failed to remove node because of it's existing "
            "connections. Consider using DETACH DELETE.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(RemoveAttachedVertexException)
};

class UserModificationInMulticommandTxException : public MulticommandTxException {
 public:
  UserModificationInMulticommandTxException() : MulticommandTxException("Managing users") {}
  SPECIALIZE_GET_EXCEPTION_NAME(UserModificationInMulticommandTxException)
};

class InvalidArgumentsException : public QueryException {
 public:
  InvalidArgumentsException(const std::string &argument_name, const std::string &message)
      : QueryException(fmt::format("Invalid arguments sent: {} - {}", argument_name, message)) {}
  SPECIALIZE_GET_EXCEPTION_NAME(InvalidArgumentsException)
};

class ReplicationModificationInMulticommandTxException : public MulticommandTxException {
 public:
  ReplicationModificationInMulticommandTxException() : MulticommandTxException("Managing replication") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ReplicationModificationInMulticommandTxException)
};

class CoordinatorModificationInMulticommandTxException : public MulticommandTxException {
 public:
  CoordinatorModificationInMulticommandTxException() : MulticommandTxException("Managing coordinators") {}
  SPECIALIZE_GET_EXCEPTION_NAME(CoordinatorModificationInMulticommandTxException)
};

class ReplicationDisabledOnDiskStorage : public QueryException {
 public:
  ReplicationDisabledOnDiskStorage() : QueryException("Replication is not supported while in on-disk storage mode.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ReplicationDisabledOnDiskStorage)
};

class LockPathModificationInMulticommandTxException : public MulticommandTxException {
 public:
  LockPathModificationInMulticommandTxException() : MulticommandTxException("Locking paths") {}
  SPECIALIZE_GET_EXCEPTION_NAME(LockPathModificationInMulticommandTxException)
};

class LockPathDisabledOnDiskStorage : public QueryException {
 public:
  LockPathDisabledOnDiskStorage()
      : QueryException("Lock path disabled on disk storage since all data is already persisted. ") {}
  SPECIALIZE_GET_EXCEPTION_NAME(LockPathDisabledOnDiskStorage)
};

class FreeMemoryModificationInMulticommandTxException : public MulticommandTxException {
 public:
  FreeMemoryModificationInMulticommandTxException() : MulticommandTxException("Free memory query") {}
  SPECIALIZE_GET_EXCEPTION_NAME(FreeMemoryModificationInMulticommandTxException)
};

class FreeMemoryDisabledOnDiskStorage : public QueryException {
 public:
  FreeMemoryDisabledOnDiskStorage() : QueryException("Free memory does nothing when using disk storage. ") {}
  SPECIALIZE_GET_EXCEPTION_NAME(FreeMemoryDisabledOnDiskStorage)
};

class ShowConfigModificationInMulticommandTxException : public MulticommandTxException {
 public:
  ShowConfigModificationInMulticommandTxException() : MulticommandTxException("Configuration information query") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ShowConfigModificationInMulticommandTxException)
};

class TriggerModificationInMulticommandTxException : public MulticommandTxException {
 public:
  TriggerModificationInMulticommandTxException() : MulticommandTxException("Managing triggers") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ShowConfigModificationInMulticommandTxException)
};

class StreamQueryInMulticommandTxException : public MulticommandTxException {
 public:
  StreamQueryInMulticommandTxException() : MulticommandTxException("Managing streams") {}
  SPECIALIZE_GET_EXCEPTION_NAME(StreamQueryInMulticommandTxException)
};

class IsolationLevelModificationInMulticommandTxException : public MulticommandTxException {
 public:
  IsolationLevelModificationInMulticommandTxException() : MulticommandTxException("Modifying isolation levels") {}
  SPECIALIZE_GET_EXCEPTION_NAME(IsolationLevelModificationInMulticommandTxException)
};

class IsolationLevelModificationInAnalyticsException : public QueryException {
 public:
  IsolationLevelModificationInAnalyticsException()
      : QueryException(
            "Isolation level cannot be modified when storage mode is set to IN_MEMORY_ANALYTICAL."
            "IN_MEMORY_ANALYTICAL mode doesn't provide any isolation guarantees, "
            "you can think about it as an equivalent to READ_UNCOMMITED.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(IsolationLevelModificationInAnalyticsException)
};

class IsolationLevelModificationInDiskTransactionalException : public QueryException {
 public:
  IsolationLevelModificationInDiskTransactionalException()
      : QueryException("Snapshot isolation level is the only supported isolation level for disk storage.") {}
};

class StorageModeModificationInMulticommandTxException : public MulticommandTxException {
 public:
  StorageModeModificationInMulticommandTxException() : MulticommandTxException("Modifying storage modes") {}
  SPECIALIZE_GET_EXCEPTION_NAME(StorageModeModificationInMulticommandTxException)
};

class EdgeImportModeModificationInMulticommandTxException : public MulticommandTxException {
 public:
  EdgeImportModeModificationInMulticommandTxException() : MulticommandTxException("Changing the edge import mode") {}
  SPECIALIZE_GET_EXCEPTION_NAME(EdgeImportModeModificationInMulticommandTxException)
};

class CreateSnapshotInMulticommandTxException final : public MulticommandTxException {
 public:
  CreateSnapshotInMulticommandTxException() : MulticommandTxException("Creating snapshots") {}
  SPECIALIZE_GET_EXCEPTION_NAME(CreateSnapshotInMulticommandTxException)
};

class CreateSnapshotDisabledOnDiskStorage final : public QueryException {
 public:
  CreateSnapshotDisabledOnDiskStorage() : QueryException("In the on-disk storage mode data is already persistent.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(CreateSnapshotDisabledOnDiskStorage)
};

class RecoverSnapshotInMulticommandTxException final : public MulticommandTxException {
 public:
  RecoverSnapshotInMulticommandTxException() : MulticommandTxException("Recovering from snapshot") {}
  SPECIALIZE_GET_EXCEPTION_NAME(RecoverSnapshotInMulticommandTxException)
};

class RecoverSnapshotDisabledOnDiskStorage final : public QueryException {
 public:
  RecoverSnapshotDisabledOnDiskStorage() : QueryException("Snapshot recovery is not supported for on-disk") {}
  SPECIALIZE_GET_EXCEPTION_NAME(RecoverSnapshotDisabledOnDiskStorage)
};

class ShowSnapshotsInMulticommandTxException final : public MulticommandTxException {
 public:
  ShowSnapshotsInMulticommandTxException() : MulticommandTxException("Snapshots listing") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ShowSnapshotsInMulticommandTxException)
};

class ShowSnapshotsDisabledOnDiskStorage final : public QueryException {
 public:
  ShowSnapshotsDisabledOnDiskStorage() : QueryException("SHOW SNAPSHOTS is not supported for on-disk") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ShowSnapshotsDisabledOnDiskStorage)
};

class EdgeImportModeQueryDisabledOnDiskStorage final : public QueryException {
 public:
  EdgeImportModeQueryDisabledOnDiskStorage()
      : QueryException("Edge import mode is only allowed for on-disk storage mode.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(EdgeImportModeQueryDisabledOnDiskStorage)
};

class SettingConfigInMulticommandTxException final : public MulticommandTxException {
 public:
  SettingConfigInMulticommandTxException() : MulticommandTxException("Updating or fetching settings") {}
  SPECIALIZE_GET_EXCEPTION_NAME(SettingConfigInMulticommandTxException)
};

class VersionInfoInMulticommandTxException : public MulticommandTxException {
 public:
  VersionInfoInMulticommandTxException() : MulticommandTxException("Version information query") {}
  SPECIALIZE_GET_EXCEPTION_NAME(VersionInfoInMulticommandTxException)
};

class AnalyzeGraphInMulticommandTxException : public MulticommandTxException {
 public:
  AnalyzeGraphInMulticommandTxException() : MulticommandTxException("Analyzing graph") {}
  SPECIALIZE_GET_EXCEPTION_NAME(AnalyzeGraphInMulticommandTxException)
};

class ReplicationException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
  explicit ReplicationException(const std::string &message)
      : utils::BasicException("Replication Exception: {} Check the status of the replicas using 'SHOW REPLICAS' query.",
                              message) {}
  SPECIALIZE_GET_EXCEPTION_NAME(ReplicationException)
};

class WriteQueryOnReplicaException : public QueryException {
 public:
  WriteQueryOnReplicaException() : QueryException("Write query forbidden on the replica!") {}
  SPECIALIZE_GET_EXCEPTION_NAME(WriteQueryOnReplicaException)
};

class WriteQueryOnMainException : public QueryException {
 public:
  WriteQueryOnMainException()
      : QueryException(
            "Write query forbidden on the main! Coordinator needs to enable writing on main by sending RPC message.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(WriteQueryOnMainException)
};

class TransactionQueueInMulticommandTxException : public MulticommandTxException {
 public:
  TransactionQueueInMulticommandTxException() : MulticommandTxException("Querying transaction status") {}
  SPECIALIZE_GET_EXCEPTION_NAME(TransactionQueueInMulticommandTxException)
};

class IndexPersistenceException : public QueryException {
 public:
  IndexPersistenceException() : QueryException("Persisting index on disk failed.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(IndexPersistenceException)
};

class ConstraintsPersistenceException : public QueryException {
 public:
  ConstraintsPersistenceException() : QueryException("Persisting constraints on disk failed.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConstraintsPersistenceException)
};

class MultiDatabaseQueryInMulticommandTxException : public MulticommandTxException {
 public:
  MultiDatabaseQueryInMulticommandTxException()
      : MulticommandTxException("Creating/dropping databases or switching the currently active database") {}
  SPECIALIZE_GET_EXCEPTION_NAME(MultiDatabaseQueryInMulticommandTxException)
};

class DropGraphInMulticommandTxException : public MulticommandTxException {
 public:
  DropGraphInMulticommandTxException() : MulticommandTxException("Dropping the graph") {}
  SPECIALIZE_GET_EXCEPTION_NAME(DropGraphInMulticommandTxException)
};

class TextSearchException : public QueryException {
  using QueryException::QueryException;
  SPECIALIZE_GET_EXCEPTION_NAME(TextSearchException)
};

class TextSearchDisabledException : public TextSearchException {
 public:
  TextSearchDisabledException()
      : TextSearchException(
            "To use text indices and text search, start Memgraph with the experimental text search feature enabled.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(TextSearchDisabledException)
};

class VectorSearchDisabledException : public QueryException {
 public:
  VectorSearchDisabledException()
      : QueryException(
            "To use vector indices and vector search, start Memgraph with the experimental vector search feature "
            "flag.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(VectorSearchDisabledException)
};

class EnumModificationInMulticommandTxException : public MulticommandTxException {
 public:
  EnumModificationInMulticommandTxException() : MulticommandTxException("Creating or modifying enums") {}
  SPECIALIZE_GET_EXCEPTION_NAME(EnumModificationInMulticommandTxException)
};

class TtlInMulticommandTxException : public MulticommandTxException {
 public:
  TtlInMulticommandTxException() : MulticommandTxException("Configuring TTL") {}
  SPECIALIZE_GET_EXCEPTION_NAME(TtlInMulticommandTxException)
};

class ShowSchemaInfoOnDiskException : public QueryException {
 public:
  ShowSchemaInfoOnDiskException() : QueryException("Show schema info is not supported for OnDisk.") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ShowSchemaInfoOnDiskException)
};

class ShowSchemaInfoInMulticommandTxException : public MulticommandTxException {
 public:
  ShowSchemaInfoInMulticommandTxException() : MulticommandTxException("Show schema info query") {}
  SPECIALIZE_GET_EXCEPTION_NAME(ShowSchemaInfoInMulticommandTxException)
};

}  // namespace memgraph::query
