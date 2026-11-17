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

#include "storage/v2/inmemory/wal_apply.hpp"

#include "slk/streams.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/variant_helpers.hpp"

#include <spdlog/spdlog.h>
#include <optional>
#include <range/v3/algorithm/find_if.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>
#include <unordered_map>

import memgraph.utils.fnv;

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

std::pair<uint64_t, durability::WalDeltaData> ReadWalDelta(durability::BaseDecoder *decoder, const uint64_t version) {
  try {
    auto timestamp = durability::ReadWalDeltaHeader(decoder);
    auto delta = durability::ReadWalDeltaData(decoder, version);
    return {timestamp, delta};
  } catch (const slk::SlkReaderException &) {
    throw utils::BasicException("Missing data!");
  } catch (const durability::RecoveryFailure &) {
    throw utils::BasicException("Invalid data!");
  }
}

// The number of applied deltas also includes skipped deltas.
std::optional<SingleTxnDeltasProcessingResult> InMemoryStorage::ApplyWalTransaction(durability::BaseDecoder *decoder,
                                                                                    const uint64_t version,
                                                                                    const WalApplyOptions &options) {
  bool const two_phase_commit = options.mode == WalApplyMode::kStreamTwoPhase;
  bool const loading_wal = options.mode == WalApplyMode::kWalFile;

  constexpr auto kSharedAccess = StorageAccessType::WRITE;
  constexpr auto kUniqueAccess = StorageAccessType::UNIQUE;

  uint64_t commit_timestamp{0};
  std::unique_ptr<ReplicationAccessor> commit_accessor;

  bool should_commit{true};
  // Replica will use the same storage access type as main did when doing the transaction
  // It is passed through the WalTransactionStart delta
  std::optional<StorageAccessType> access_type;

  auto translate_access_type = [](durability::TransactionAccessType access_type) {
    switch (access_type) {
      case durability::TransactionAccessType::UNIQUE:
        return StorageAccessType::UNIQUE;
      case durability::TransactionAccessType::WRITE:
        return StorageAccessType::WRITE;
      case durability::TransactionAccessType::READ:
        return StorageAccessType::READ;
      case durability::TransactionAccessType::READ_ONLY:
        return StorageAccessType::READ_ONLY;
      default:
        throw std::runtime_error("Unrecognized access type!");
    }
  };

  auto const get_replication_accessor = [&, this](uint64_t const local_commit_timestamp,
                                                  StorageAccessType acc_hint = kSharedAccess) -> ReplicationAccessor * {
    if (!commit_accessor) {
      std::unique_ptr<Storage::Accessor> acc = nullptr;
      // acc_hint only gets used if we are using an older version of WAL (before v3.5.0)
      switch (auto const true_access_type = access_type.value_or(acc_hint)) {
        case StorageAccessType::READ:
          [[fallthrough]];
        case StorageAccessType::WRITE:
          acc = Access(true_access_type);
          break;
        case StorageAccessType::UNIQUE:
          acc = UniqueAccess();
          break;
        case StorageAccessType::READ_ONLY:
          acc = ReadOnlyAccess();
          break;
        default:
          throw utils::BasicException("Replica failed to gain storage access! Unknown accessor type.");
      }

      commit_timestamp = local_commit_timestamp;
      commit_accessor.reset(static_cast<ReplicationAccessor *>(acc.release()));

    } else if (commit_timestamp != local_commit_timestamp) {
      throw utils::BasicException("Received more than one transaction!");
    }
    return commit_accessor.get();
  };

  uint64_t num_committed_txns{0};
  uint64_t current_delta_idx{0};  // tracks over how many deltas we iterated, includes also skipped deltas.
  uint64_t applied_deltas{0};     // Non-skipped deltas
  uint64_t skipped_deltas{0};     // Already present on the replica
  auto max_delta_timestamp = repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;

  auto current_durable_commit_timestamp = max_delta_timestamp;
  spdlog::trace("Current durable commit timestamp: {}", current_durable_commit_timestamp);

  uint64_t prev_printed_timestamp = 0;

  // Cache (edge_gid, delta_timestamp, edge_type) -> EdgeAccessor. Filled on EdgeCreate or on first SET_PROPERTY
  // resolution; reused for subsequent SET_PROPERTY.
  struct EdgeSetPropertyCacheKey {
    uint64_t edge_gid{};
    uint64_t delta_timestamp{};
    bool operator==(const EdgeSetPropertyCacheKey &o) const = default;
  };

  struct EdgeSetPropertyCacheKeyHash {
    size_t operator()(const EdgeSetPropertyCacheKey &k) const {
      return utils::HashCombine<uint64_t, uint64_t>{}(k.edge_gid, k.delta_timestamp);
    }
  };

  std::unordered_map<EdgeSetPropertyCacheKey, EdgeAccessor, EdgeSetPropertyCacheKeyHash> edge_set_property_cache;

  // Edge deletions are buffered and applied as one batch. DeleteEdgesEx resolves the whole batch from gids and
  // hands it to a single DetachDelete, which groups by endpoint vertex and scans each vertex's adjacency once —
  // so the batch costs O(degree) per vertex where a delta-at-a-time loop costs O(degree) per edge. Main is
  // already on the batched path (the Delete operator buffers, then calls DetachDelete once), which is why
  // deleting a dense vertex's edges is linear there and was quadratic here.
  //
  // The WAL encoder emits deltas in fixed passes (vertex writes, edge creates, edge property sets, edge deletes,
  // vertex deletes), so every edge delete of a transaction arrives as one contiguous run and a single batch
  // absorbs all of them.
  std::vector<InMemoryStorage::EdgeDeleteSpec> pending_edge_deletes;

  // Bounds the buffer's memory: main may split one transaction's deletions across several DetachDelete calls
  // (the Delete operator's buffer size), so a transaction can carry far more edge deletes than main ever held
  // at once. Flushing mid-run only costs one extra adjacency scan per vertex per chunk.
  constexpr size_t kMaxPendingEdgeDeletes = 100'000;

  auto flush_pending_edge_deletes = [&]() {
    if (pending_edge_deletes.empty()) return;
    // A non-empty buffer implies the accessor was created when the first edge delete was buffered.
    DMG_ASSERT(commit_accessor, "Buffered edge deletions without a commit accessor");
    auto const deleted = commit_accessor->DeleteEdgesEx(pending_edge_deletes);
    // Each buffered edge was already traced with its gid above, so the timestamp is enough to correlate.
    if (!deleted.has_value()) {
      throw utils::BasicException(
          "Failed to delete a batch of {} edges at timestamp {}.", pending_edge_deletes.size(), commit_timestamp);
    }
    // Resolving by gid no longer visits the edge through a visibility-checked lookup, so the count is what
    // catches a WAL naming an edge the replica does not have.
    if (*deleted != pending_edge_deletes.size()) {
      throw utils::BasicException("Deleted {} of {} edges in the batch at timestamp {}.",
                                  *deleted,
                                  pending_edge_deletes.size(),
                                  commit_timestamp);
    }
    pending_edge_deletes.clear();
  };

  decoder->ResetCrcAcc();
  // A DDL delta below can occupy the handler for minutes on its own, so index population and constraint validation
  // report progress per vertex through this. Returning true also abandons that work once the main is gone, instead of
  // finishing a build whose result can no longer be delivered.
  auto const schema_progress = [&options]() {
    if (options.on_progress) options.on_progress();
    return options.should_cancel && options.should_cancel();
  };

  // The text, point and vector builders report progress but have no cancellation hook, so they get a progress-only
  // callback rather than schema_progress -- passing that would silently discard its cancel answer.
  auto const report_progress = [&options]() {
    if (options.on_progress) options.on_progress();
  };

  for (bool transaction_complete = false; !transaction_complete; ++current_delta_idx) {
    report_progress();
    auto const [delta_timestamp, delta] = ReadWalDelta(decoder, version);
    if (delta_timestamp != prev_printed_timestamp) {
      spdlog::trace("Timestamp: {}", delta_timestamp);
      prev_printed_timestamp = delta_timestamp;
    }

    max_delta_timestamp = std::max(max_delta_timestamp, delta_timestamp);

    transaction_complete = IsWalDeltaDataTransactionEnd(delta, version);

    if (delta_timestamp <= current_durable_commit_timestamp) {
      ++skipped_deltas;
      continue;
    }

    // NOLINTNEXTLINE (google-build-using-namespace)
    using namespace durability;
    auto *mapper = name_id_mapper_.get();
    auto delta_apply = utils::Overloaded{
        [&](WalVertexCreate const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("  Delta {}. Create vertex {}", current_delta_idx, gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          if (!transaction->CreateVertexEx(data.gid)) {
            throw utils::BasicException("Vertex with gid {} already exists at replica.", gid);
          }
        },
        [&](WalVertexDelete const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("  Delta {}. Delete vertex {}", current_delta_idx, gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) {
            throw utils::BasicException("Vertex with gid {} couldn't be found while trying to delete vertex.", gid);
          }
          auto ret = transaction->DeleteVertex(&*vertex);
          if (!ret || !ret.value()) {
            throw utils::BasicException("Deleting vertex with gid {} failed.", gid);
          }
        },
        [&](WalVertexAddLabel const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("   Delta {}. Vertex {} add label {}", current_delta_idx, gid, data.label);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) {
            throw utils::BasicException("Couldn't find vertex {} when adding label.", gid);
          }
          auto ret = vertex->AddLabel(transaction->NameToLabel(data.label));
          if (!ret || !ret.value()) {
            throw utils::BasicException("Failed to add label to vertex {}.", gid);
          }
        },
        [&](WalVertexRemoveLabel const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("   Delta {}. Vertex {} remove label {}", current_delta_idx, gid, data.label);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) throw utils::BasicException("Failed to find vertex {} when removing label.", gid);
          auto ret = vertex->RemoveLabel(transaction->NameToLabel(data.label));
          if (!ret || !ret.value()) {
            throw utils::BasicException("Failed to remove label from vertex {}.", gid);
          }
        },
        [&](WalVertexSetProperty const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("   Delta {}. Vertex {} set property", current_delta_idx, gid);
          // NOLINTNEXTLINE
          auto *transaction = get_replication_accessor(delta_timestamp);
          // NOLINTNEXTLINE
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) {
            throw utils::BasicException("Failed to find vertex {} when setting property.", gid);
          }
          auto ret =
              vertex->SetProperty(transaction->NameToProperty(data.property), ToPropertyValue(data.value, mapper));
          if (!ret) {
            throw utils::BasicException("Failed to set property label from vertex {}.", gid);
          }
        },
        [&](WalEdgeCreate const &data) {
          auto const edge_gid = data.gid.AsUint();
          auto const from_vertex_gid = data.from_vertex.AsUint();
          auto const to_vertex_gid = data.to_vertex.AsUint();
          spdlog::trace("   Delta {}. Create edge {} of type {} from vertex {} to vertex {}",
                        current_delta_idx,
                        edge_gid,
                        data.edge_type,
                        from_vertex_gid,
                        to_vertex_gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto from_vertex = transaction->FindVertex(data.from_vertex, View::NEW);
          if (!from_vertex) {
            throw utils::BasicException("Failed to find vertex {} when adding edge {}.", from_vertex_gid, edge_gid);
          }
          auto to_vertex = transaction->FindVertex(data.to_vertex, View::NEW);
          if (!to_vertex) {
            throw utils::BasicException("Failed to find vertex {} when adding edge {}.", to_vertex_gid, edge_gid);
          }
          auto edge_result = transaction->CreateEdgeEx(
              &*from_vertex, &*to_vertex, transaction->NameToEdgeType(data.edge_type), data.gid);
          if (!edge_result) {
            throw utils::BasicException(
                "Failed to add edge {} between vertices {} and {}.", edge_gid, from_vertex_gid, to_vertex_gid);
          }
          // Pre-fill cache for subsequent SET_PROPERTY on this edge in the same transaction.
          auto &edge = *edge_result;
          EdgeSetPropertyCacheKey key{.edge_gid = edge_gid, .delta_timestamp = delta_timestamp};
          edge_set_property_cache.emplace(key, edge);
        },
        [&](WalEdgeDelete const &data) {
          auto const edge_gid = data.gid.AsUint();
          auto const from_vertex_gid = data.from_vertex.AsUint();
          auto const to_vertex_gid = data.to_vertex.AsUint();
          spdlog::trace("   Delta {}. Delete edge {} of type {} from vertex {} to vertex {}",
                        current_delta_idx,
                        edge_gid,
                        data.edge_type,
                        from_vertex_gid,
                        to_vertex_gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          // The record already says everything needed to delete the edge, so nothing is resolved here; the
          // batch is flushed by the next non-edge-delete delta and resolves its edges together.
          pending_edge_deletes.push_back(
              InMemoryStorage::EdgeDeleteSpec{.edge_gid = data.gid,
                                              .from_gid = data.from_vertex,
                                              .to_gid = data.to_vertex,
                                              .edge_type = transaction->NameToEdgeType(data.edge_type)});
          if (pending_edge_deletes.size() >= kMaxPendingEdgeDeletes) {
            flush_pending_edge_deletes();
          }
        },
        [&](WalEdgeSetProperty const &data) {
          auto const edge_gid = data.gid.AsUint();
          spdlog::trace("   Delta {}. Edge {} set property (from_gid={} to_gid={})",
                        current_delta_idx,
                        edge_gid,
                        data.from_gid.has_value() ? static_cast<int64_t>(data.from_gid->AsUint()) : -1,
                        data.to_gid.has_value() ? static_cast<int64_t>(data.to_gid->AsUint()) : -1);
          if (!config_.salient.items.properties_on_edges)
            throw utils::BasicException(
                "Can't set properties on edges because properties on edges "
                "are disabled!");

          auto *transaction = get_replication_accessor(delta_timestamp);
          EdgeSetPropertyCacheKey const cache_key{.edge_gid = edge_gid, .delta_timestamp = delta_timestamp};

          // Fast path: use cached edge accessor.
          auto it = edge_set_property_cache.find(cache_key);
          if (it != edge_set_property_cache.end()) {
            auto ret =
                it->second.SetProperty(transaction->NameToProperty(data.property), ToPropertyValue(data.value, mapper));
            if (!ret) {
              throw utils::BasicException("Setting property on edge {} failed.", edge_gid);
            }
            return;
          }

          // Resolve EdgeInfo using the best available WAL data (newest to oldest format).
          // Edge is alive by WAL ordering — a SET_PROPERTY delta proves it was not GC-collectable
          // at main-side commit.
          // Case 1 (newest WAL): from_gid + to_gid + edge_type → type-filtered out_edges scan via
          //                      transaction->FindEdge, fastest. Light and heavy edges both supported.
          // Case 2:              from_gid only → FindEdge(gid, from_gid) handles both light
          //                      and heavy edges internally; no special-casing needed. O(log V + deg).
          // Case 3 (oldest WAL): gid only → FindEdge(gid) handles both light and heavy edges
          //                      internally; no special-casing needed. Full scan fallback.
          const auto cached_edge_info = std::invoke([&]() -> EdgeInfo {
            if (data.from_gid.has_value() && data.to_gid.has_value() && data.edge_type.has_value() &&
                *data.to_gid != kInvalidGid && !data.edge_type->empty()) {
              auto to_v = transaction->FindVertex(*data.to_gid, View::NEW);
              if (!to_v)
                throw utils::BasicException("Failed to find to vertex {} when setting edge property.",
                                            data.to_gid->AsUint());
              auto from_v = transaction->FindVertex(*data.from_gid, View::NEW);
              if (!from_v)
                throw utils::BasicException("Failed to find from vertex {} when setting edge property.",
                                            data.from_gid->AsUint());
              auto const edge_type_id = transaction->NameToEdgeType(*data.edge_type);
              auto found = transaction->FindEdge(data.gid, View::NEW, edge_type_id, &*from_v, &*to_v);
              if (!found) {
                throw utils::BasicException("Failed to find edge {} when setting edge property.", edge_gid);
              }
              return EdgeInfo{std::in_place, found->edge_, found->edge_type_, found->from_vertex_, found->to_vertex_};
            } else if (data.from_gid.has_value()) {
              auto info = FindEdge(data.gid, *data.from_gid);
              if (!info)
                throw utils::BasicException("Failed to find edge {} from vertex {} when setting edge property.",
                                            edge_gid,
                                            data.from_gid->AsUint());
              return info;
            } else {
              auto info = FindEdge(data.gid);
              if (!info) throw utils::BasicException("Failed to find edge {} when setting edge property.", edge_gid);
              return info;
            }
          });

          auto const &[er, et, fv, tv] = *cached_edge_info;
          auto *edge_raw = er.ptr;
          {
            bool is_visible = true;
            Delta *local_delta = nullptr;
            {
              auto guard = std::shared_lock{edge_raw->lock};
              is_visible = !edge_raw->deleted();
              local_delta = edge_raw->delta();
            }
            ApplyDeltasForRead(
                &transaction->GetTransaction(), local_delta, View::NEW, [&is_visible](const Delta &delta) {
                  switch (delta.action) {
                    case Delta::Action::ADD_LABEL:
                    case Delta::Action::REMOVE_LABEL:
                    case Delta::Action::SET_PROPERTY:
                    case Delta::Action::ADD_IN_EDGE:
                    case Delta::Action::ADD_OUT_EDGE:
                    case Delta::Action::REMOVE_IN_EDGE:
                    case Delta::Action::REMOVE_OUT_EDGE:
                      break;
                    case Delta::Action::RECREATE_OBJECT: {
                      is_visible = true;
                      break;
                    }
                    case Delta::Action::DELETE_DESERIALIZED_OBJECT:
                    case Delta::Action::DELETE_OBJECT: {
                      is_visible = false;
                      break;
                    }
                  }
                });
            if (!is_visible) {
              throw utils::BasicException("Edge {} isn't visible when setting edge property.", edge_gid);
            }
          }
          EdgeAccessor ea{er, et, fv, tv, this, &transaction->GetTransaction()};
          edge_set_property_cache.emplace(cache_key, ea);  // Fast edge accessor lookup cache
          auto ret = ea.SetProperty(transaction->NameToProperty(data.property), ToPropertyValue(data.value, mapper));
          if (!ret) {
            throw utils::BasicException("Setting property on edge {} failed.", edge_gid);
          }
        },
        [&](WalTransactionStart const &data) {
          spdlog::trace("   Delta {}. Transaction start. Commit txn: {}, Access type: {}",
                        current_delta_idx,
                        data.commit,
                        data.access_type ? static_cast<uint64_t>(*data.access_type) : -1);

          if (loading_wal) {
            // This only gets used when loading a WAL from main
            // Otherwise it doesn't matter what gets sent
            should_commit = data.commit.value_or(true);
          }
          access_type = data.access_type ? std::optional(translate_access_type(*data.access_type)) : std::nullopt;
        },
        [&](WalTransactionEnd const &txn_end) {
          spdlog::trace("   Delta {}. Transaction end", current_delta_idx);
          if (!commit_accessor || commit_timestamp != delta_timestamp) {
            throw utils::BasicException("Invalid commit data!");
          }
          // We don't do CRC verification on PrepareCommitRpc because we are already using TCP sockets
          if (loading_wal && txn_end.txn_crc.has_value() && !utils::CrcAccumulator::Verify(decoder->CrcAccValue())) {
            throw utils::BasicException(
                "Replication WAL CRC mismatch (stored {}, residue {}).", *txn_end.txn_crc, decoder->CrcAccValue());
          }
          decoder->ResetCrcAcc();

          // Durability could take some time on replica
          auto in_progress_cb = report_progress;

          auto const ret = commit_accessor->PrepareForCommitPhase(
              CommitArgs::make_replica_write(commit_timestamp, two_phase_commit, std::move(in_progress_cb)));

          if (!ret) {
            throw utils::BasicException("Committing failed while trying to prepare for commit on replica.");
          }
          // If not STRICT SYNC replica, reset the commit accessor immediately because the txn is considered committed
          if (!two_phase_commit) {
            commit_accessor.reset();
          }
          // Used to return info to MAIN about how many txns were committed
          if (loading_wal) {
            num_committed_txns++;
          }
        },
        [&](WalLabelIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create label index on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->CreateIndex(NameToLabel(data.label), schema_progress))
            throw utils::BasicException("Failed to create label index on :{}.", data.label);
        },
        [&](WalLabelIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop label index on :{}", current_delta_idx, data.label);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->DropIndex(NameToLabel(data.label), AbsentIndex::kIsRecorded))
            throw utils::BasicException("Failed to drop label index on :{}.", data.label);
        },
        [&](WalLabelIndexStatsSet const &data) {
          spdlog::trace("   Delta {}. Set label index statistics on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          const auto label = NameToLabel(data.label);
          LabelIndexStats stats{};
          if (!FromJson(data.json_stats, stats)) {
            throw utils::BasicException("Failed to read statistics!");
          }
          transaction->SetIndexStats(label, stats);
        },
        [&](WalLabelIndexStatsClear const &data) {
          spdlog::trace("   Delta {}. Clear label index statistics on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          if (!transaction->DeleteLabelIndexStats(NameToLabel(data.label))) {
            throw utils::BasicException("Failed to clear label index statistics on :{}.", data.label);
          }
        },
        [&](WalLabelPropertyIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create label+property index on :{} ({})",
                        current_delta_idx,
                        data.label,
                        data.composite_property_paths);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto property_paths = data.composite_property_paths.convert(mapper);
          if (!transaction->CreateIndex(
                  NameToLabel(data.label), std::move(property_paths), IndexOrder::ASC, schema_progress))
            throw utils::BasicException(
                "Failed to create label+property index on :{} ({}).", data.label, data.composite_property_paths);
        },
        [&](WalLabelPropertyIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop label+property index on :{} ({})",
                        current_delta_idx,
                        data.label,
                        data.composite_property_paths);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto property_paths = data.composite_property_paths.convert(mapper);

          // A WAL written before the order was recorded means the only order there was, ASC.
          if (!transaction->DropIndex(NameToLabel(data.label),
                                      std::move(property_paths),
                                      data.order.value_or(IndexOrder::ASC),
                                      AbsentIndex::kIsRecorded)) {
            throw utils::BasicException(
                "Failed to drop label+property index on :{} ({}).", data.label, data.composite_property_paths);
          }
        },
        [&](WalLabelPropertyIndexStatsSet const &data) {
          spdlog::trace("   Delta {}. Set label-property index statistics on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          const auto label = NameToLabel(data.label);
          auto property_paths = data.composite_property_paths.convert(mapper);
          LabelPropertyIndexStats stats{};
          if (!FromJson(data.json_stats, stats)) {
            throw utils::BasicException("Failed to read statistics!");
          }
          transaction->SetIndexStats(label, std::move(property_paths), stats);
        },
        [&](WalLabelPropertyIndexStatsClear const &data) {
          spdlog::trace("   Delta {}. Clear label-property index statistics on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          transaction->DeleteLabelPropertyIndexStats(NameToLabel(data.label));
        },
        [&](WalEdgeTypeIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create edge index on :{}", current_delta_idx, data.edge_type);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->CreateIndex(NameToEdgeType(data.edge_type), schema_progress)) {
            throw utils::BasicException("Failed to create edge index on :{}.", data.edge_type);
          }
        },
        [&](WalEdgeTypeIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop edge index on :{}", current_delta_idx, data.edge_type);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->DropIndex(NameToEdgeType(data.edge_type), AbsentIndex::kIsRecorded)) {
            throw utils::BasicException("Failed to drop edge index on :{}.", data.edge_type);
          }
        },
        [&](WalEdgeTypePropertyIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create edge index on :{}({})", current_delta_idx, data.edge_type, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->CreateIndex(NameToEdgeType(data.edge_type), NameToProperty(data.property), schema_progress)
                   .has_value()) {
            throw utils::BasicException(
                "Failed to create edge property index on :{}({}).", data.edge_type, data.property);
          }
        },
        [&](WalEdgeTypePropertyIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop edge index on :{}({})", current_delta_idx, data.edge_type, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->DropIndex(
                  NameToEdgeType(data.edge_type), NameToProperty(data.property), AbsentIndex::kIsRecorded)) {
            throw utils::BasicException(
                "Failed to drop edge property index on :{}({}).", data.edge_type, data.property);
          }
        },
        [&](WalEdgePropertyIndexCreate const &data) {
          spdlog::trace("       Create global edge index on ({})", data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->CreateGlobalEdgeIndex(NameToProperty(data.property), schema_progress)) {
            throw utils::BasicException("Failed to create global edge property index on ({}).", data.property);
          }
        },
        [&](WalEdgePropertyIndexDrop const &data) {
          spdlog::trace("       Drop global edge index on ({})", data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->DropGlobalEdgeIndex(NameToProperty(data.property), AbsentIndex::kIsRecorded)) {
            throw utils::BasicException("Failed to drop global edge property index on ({}).", data.property);
          }
        },
        [&](WalVertexPropertyIndexCreate const &data) {
          spdlog::trace("       Create global vertex property index on ({})", data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->CreateGlobalVertexIndex(NameToProperty(data.property), schema_progress)) {
            throw utils::BasicException("Failed to create global vertex property index on ({}).", data.property);
          }
        },
        [&](WalVertexPropertyIndexDrop const &data) {
          spdlog::trace("       Drop global vertex property index on ({})", data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->DropGlobalVertexIndex(NameToProperty(data.property), AbsentIndex::kIsRecorded)) {
            throw utils::BasicException("Failed to drop global vertex property index on ({}).", data.property);
          }
        },
        [&](WalTextIndexCreate const &data) {
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto label_id = NameToLabel(data.label);
          const auto properties_str = std::invoke([&]() -> std::string {
            if (data.properties && !data.properties->empty()) {
              return fmt::format(" ({})", rv::join(*data.properties, ", ") | r::to<std::string>);
            }
            return {};
          });
          spdlog::trace("   Delta {}. Create text search index {} on :{}{}",
                        current_delta_idx,
                        data.index_name,
                        data.label,
                        properties_str);
          auto prop_ids = std::invoke([&]() -> std::vector<PropertyId> {
            if (!data.properties) {
              return {};
            }
            return *data.properties | rv::transform([&](const auto &prop_name) { return NameToProperty(prop_name); }) |
                   r::to_vector;
          });
          auto ret = transaction->CreateTextIndex(
              TextIndexSpec{.index_name = data.index_name, .label = label_id, .properties = prop_ids}, report_progress);
          if (!ret) {
            throw utils::BasicException("Failed to create text search index {} on {}.", data.index_name, data.label);
          }
        },
        [&](WalTextEdgeIndexCreate const &data) {
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          const auto edge_type = NameToEdgeType(data.edge_type);
          const auto properties_str = std::invoke([&]() -> std::string {
            if (!data.properties.empty()) {
              return fmt::format(" ({})", rv::join(data.properties, ", ") | r::to<std::string>);
            }
            return {};
          });
          spdlog::trace("   Delta {}. Create text search index {} on :{}{}",
                        current_delta_idx,
                        data.index_name,
                        data.edge_type,
                        properties_str);
          auto prop_ids = data.properties |
                          rv::transform([&](const auto &prop_name) { return NameToProperty(prop_name); }) |
                          r::to_vector;
          const auto ret = transaction->CreateTextEdgeIndex(
              TextEdgeIndexSpec{.index_name = data.index_name, .edge_type = edge_type, .properties = prop_ids},
              report_progress);
          if (!ret) {
            throw utils::BasicException(
                "Failed to create text search index {} on {}.", data.index_name, data.edge_type);
          }
        },
        [&](WalTextIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop text search index {}.", current_delta_idx, data.index_name);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->DropTextIndex(data.index_name)) {
            throw utils::BasicException("Failed to drop text search index {}.", data.index_name);
          }
        },
        [&](WalExistenceConstraintCreate const &data) {
          spdlog::trace(
              "   Delta {}. Create existence constraint on :{} ({})", current_delta_idx, data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->CreateExistenceConstraint(
              NameToLabel(data.label), NameToProperty(data.property), schema_progress);
          if (!ret) {
            throw utils::BasicException(
                "Failed to create existence constraint on :{} ({}).", data.label, data.property);
          }
        },
        [&](WalExistenceConstraintDrop const &data) {
          spdlog::trace(
              "   Delta {}. Drop existence constraint on :{} ({})", current_delta_idx, data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (!transaction->DropExistenceConstraint(NameToLabel(data.label), NameToProperty(data.property))
                   .has_value()) {
            throw utils::BasicException("Failed to drop existence constraint on :{} ({}).", data.label, data.property);
          }
        },
        [&](WalUniqueConstraintCreate const &data) {
          std::stringstream ss;
          utils::PrintIterable(ss, data.properties);
          spdlog::trace("   Delta {}. Create unique constraint on :{} ({})", current_delta_idx, data.label, ss.str());
          std::set<PropertyId> properties;
          for (const auto &prop : data.properties) {
            properties.emplace(NameToProperty(prop));
          }
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->CreateUniqueConstraint(NameToLabel(data.label), properties, schema_progress);
          if (!ret || ret.value() != UniqueConstraints::CreationStatus::SUCCESS) {
            throw utils::BasicException("Failed to create unique constraint on :{} ({}).", data.label, ss.str());
          }
        },
        [&](WalUniqueConstraintDrop const &data) {
          std::stringstream ss;
          utils::PrintIterable(ss, data.properties);
          spdlog::trace("   Delta {}. Drop unique constraint on :{} ({})", current_delta_idx, data.label, ss.str());
          std::set<PropertyId> properties;
          for (const auto &prop : data.properties) {
            properties.emplace(NameToProperty(prop));
          }
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->DropUniqueConstraint(NameToLabel(data.label), properties);
          if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
            throw utils::BasicException("Failed to create unique constraint on :{} ({}).", data.label, ss.str());
          }
        },
        [&](WalTypeConstraintCreate const &data) {
          spdlog::trace("   Delta {}. Create IS TYPED {} constraint on :{} ({})",
                        current_delta_idx,
                        TypeConstraintKindToString(data.kind),
                        data.label,
                        data.property);

          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->CreateTypeConstraint(
              NameToLabel(data.label), NameToProperty(data.property), data.kind, schema_progress);
          if (!ret) {
            throw utils::BasicException("Failed to create IS TYPED {} constraint on :{} ({}).",
                                        TypeConstraintKindToString(data.kind),
                                        data.label,
                                        data.property);
          }
        },
        [&](WalTypeConstraintDrop const &data) {
          spdlog::trace("   Delta {}. Drop IS TYPED {} constraint on :{} ({})",
                        current_delta_idx,
                        TypeConstraintKindToString(data.kind),
                        data.label,
                        data.property);

          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->DropTypeConstraint(NameToLabel(data.label), NameToProperty(data.property), data.kind);
          if (!ret) {
            throw utils::BasicException("Failed to drop IS TYPED {} constraint on :{} ({}).",
                                        TypeConstraintKindToString(data.kind),
                                        data.label,
                                        data.property);
          }
        },
        [&](WalEnumCreate const &data) {
          std::stringstream ss;
          utils::PrintIterable(ss, data.evalues);
          spdlog::trace("   Delta {}. Create enum {} with values {}", current_delta_idx, data.etype, ss.str());
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->CreateEnum(data.etype, data.evalues);
          if (!res) {
            throw utils::BasicException("Failed to create enum {} with values {}.", data.etype, ss.str());
          }
        },
        [&](WalEnumAlterAdd const &data) {
          spdlog::trace("   Delta {}. Alter enum {} add value {}", current_delta_idx, data.etype, data.evalue);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->EnumAlterAdd(data.etype, data.evalue);
          if (!res) {
            throw utils::BasicException("Failed to alter enum {} add value {}.", data.etype, data.evalue);
          }
        },
        [&](WalEnumAlterUpdate const &data) {
          spdlog::trace("   Delta {}. Alter enum {} update {} to {}",
                        current_delta_idx,
                        data.etype,
                        data.evalue_old,
                        data.evalue_new);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->EnumAlterUpdate(data.etype, data.evalue_old, data.evalue_new);
          if (!res) {
            throw utils::BasicException(
                "Failed to alter enum {} update {} to {}.", data.etype, data.evalue_old, data.evalue_new);
          }
        },
        [&](WalPointIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create point index on :{}({})", current_delta_idx, data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto labelId = NameToLabel(data.label);
          auto propId = NameToProperty(data.property);
          auto res = transaction->CreatePointIndex(labelId, propId, report_progress);
          if (!res) {
            throw utils::BasicException("Failed to create point index on :{}({})", data.label, data.property);
          }
        },
        [&](WalPointIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop point index on :{}({})", current_delta_idx, data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto labelId = NameToLabel(data.label);
          auto propId = NameToProperty(data.property);
          auto res = transaction->DropPointIndex(labelId, propId);
          if (!res) {
            throw utils::BasicException("Failed to drop point index on :{}({})", data.label, data.property);
          }
        },
        [&](WalVectorIndexCreate const &data) {
          spdlog::trace(
              "   Delta {}. Create vector index {} on property {}", current_delta_idx, data.index_name, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto propId = NameToProperty(data.property);
          auto metric_kind = MetricFromName(data.metric_kind);
          auto scalar_kind = data.scalar_kind ? static_cast<unum::usearch::scalar_kind_t>(*data.scalar_kind)
                                              : unum::usearch::scalar_kind_t::f32_k;

          std::vector<LabelId> label_ids;
          label_ids.reserve(data.label_filter.ids.size());
          for (const auto &name : data.label_filter.ids) label_ids.push_back(NameToLabel(name));

          auto res = transaction->CreateVectorIndex(
              VectorIndexSpec{
                  .index_name = data.index_name,
                  .label_filter = VectorLabelFilter{.mode = static_cast<VectorMatchMode>(data.label_filter.mode),
                                                    .ids = std::move(label_ids)},
                  .property = propId,
                  .metric_kind = metric_kind,
                  .dimension = data.dimension,
                  .resize_coefficient = data.resize_coefficient,
                  .capacity = data.capacity,
                  .scalar_kind = scalar_kind,
              },
              report_progress);
          if (!res) {
            throw utils::BasicException(
                "Failed to create vector index {} on property {}", data.index_name, data.property);
          }
        },
        [&](WalVectorEdgeIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create vector edge index {} on property {}",
                        current_delta_idx,
                        data.index_name,
                        data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto propId = NameToProperty(data.property);
          auto metric_kind = MetricFromName(data.metric_kind);

          std::vector<EdgeTypeId> edge_type_ids;
          edge_type_ids.reserve(data.edge_type_filter.ids.size());
          for (const auto &name : data.edge_type_filter.ids) edge_type_ids.push_back(NameToEdgeType(name));

          auto res = transaction->CreateVectorEdgeIndex(
              VectorEdgeIndexSpec{
                  .index_name = data.index_name,
                  .edge_type_filter =
                      VectorEdgeTypeFilter{.mode = static_cast<VectorMatchMode>(data.edge_type_filter.mode),
                                           .ids = std::move(edge_type_ids)},
                  .property = propId,
                  .metric_kind = metric_kind,
                  .dimension = data.dimension,
                  .resize_coefficient = data.resize_coefficient,
                  .capacity = data.capacity,
                  .scalar_kind = static_cast<unum::usearch::scalar_kind_t>(data.scalar_kind),
              },
              report_progress);
          if (!res) {
            throw utils::BasicException(
                "Failed to create vector edge index {} on property {}", data.index_name, data.property);
          }
        },
        [&](WalVectorIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop vector index {} ", current_delta_idx, data.index_name);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->DropVectorIndex(data.index_name, report_progress);
          if (!res) {
            throw utils::BasicException("Failed to drop vector index {}", data.index_name);
          }
        },
        [&]([[maybe_unused]] WalTtlOperation const &data) {
#ifdef MG_ENTERPRISE
          spdlog::trace("   Delta {}. TTL operation type {}", current_delta_idx, static_cast<int>(data.operation_type));
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          switch (data.operation_type) {
            case durability::TtlOperationType::ENABLE:
              transaction->StartTtl({.is_main = false});
              break;
            case durability::TtlOperationType::DISABLE:
              transaction->DisableTtl({.is_main = false});
              break;
            case durability::TtlOperationType::CONFIGURE:
              transaction->ConfigureTtl(ttl::TtlInfo{data.period, data.start_time, data.should_run_edge_ttl},
                                        {.is_main = false});
              // Configuration will leave it paused; replicas should not run ttl
              break;
            case durability::TtlOperationType::STOP:
              transaction->StopTtl();
              break;
            default:
              throw utils::BasicException("Invalid TTL operation type: {}", static_cast<int>(data.operation_type));
          }
#else
          spdlog::trace("TTL operation is not supported in community edition");
#endif
        },
        [&](WalDescriptionSet const &data) {
          spdlog::trace("   Delta {}. Set description (kind={})", current_delta_idx, static_cast<int>(data.kind));
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          switch (data.kind) {
            case DescriptionTargetKind::DATABASE:
              transaction->SetDatabaseDescription(data.description);
              break;
            case DescriptionTargetKind::LABEL:
              transaction->SetLabelDescription(data.labels, data.description);
              break;
            case DescriptionTargetKind::EDGE_TYPE:
              transaction->SetEdgeTypeDescription(data.edge_type, data.description);
              break;
            case DescriptionTargetKind::LABEL_PROPERTY:
              transaction->SetLabelPropertyDescription(data.labels, data.property, data.description);
              break;
            case DescriptionTargetKind::EDGE_TYPE_PROPERTY:
              transaction->SetEdgeTypePropertyDescription(data.edge_type, data.property, data.description);
              break;
            case DescriptionTargetKind::PROPERTY:
              transaction->SetPropertyDescription(data.property, data.description);
              break;
            case DescriptionTargetKind::PROPERTY_VALUE:
              transaction->SetPropertyValueDescription(data.property, data.value, data.description);
              break;
            case DescriptionTargetKind::EDGE_TYPE_PATTERN:
              transaction->SetEdgeTypePatternDescription(
                  data.from_labels, data.edge_type, data.to_labels, data.description);
              break;
            case DescriptionTargetKind::EDGE_TYPE_PATTERN_PROPERTY:
              transaction->SetEdgeTypePatternPropertyDescription(
                  data.from_labels, data.edge_type, data.to_labels, data.property, data.description);
              break;
            default:
              break;
          }
        },
        [&](WalDescriptionDelete const &data) {
          spdlog::trace("   Delta {}. Delete description (kind={})", current_delta_idx, static_cast<int>(data.kind));
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          switch (data.kind) {
            case DescriptionTargetKind::DATABASE:
              transaction->DeleteDatabaseDescription();
              break;
            case DescriptionTargetKind::LABEL:
              transaction->DeleteLabelDescription(data.labels);
              break;
            case DescriptionTargetKind::EDGE_TYPE:
              transaction->DeleteEdgeTypeDescription(data.edge_type);
              break;
            case DescriptionTargetKind::LABEL_PROPERTY:
              transaction->DeleteLabelPropertyDescription(data.labels, data.property);
              break;
            case DescriptionTargetKind::EDGE_TYPE_PROPERTY:
              transaction->DeleteEdgeTypePropertyDescription(data.edge_type, data.property);
              break;
            case DescriptionTargetKind::PROPERTY:
              transaction->DeletePropertyDescription(data.property);
              break;
            case DescriptionTargetKind::PROPERTY_VALUE:
              transaction->DeletePropertyValueDescription(data.property, data.value);
              break;
            case DescriptionTargetKind::EDGE_TYPE_PATTERN:
              transaction->DeleteEdgeTypePatternDescription(data.from_labels, data.edge_type, data.to_labels);
              break;
            case DescriptionTargetKind::EDGE_TYPE_PATTERN_PROPERTY:
              transaction->DeleteEdgeTypePatternPropertyDescription(
                  data.from_labels, data.edge_type, data.to_labels, data.property);
              break;
            default:
              break;
          }
        },
    };
    // If I received PrepareCommitRpc, deltas should be applied (loading_wal will be false)
    // If loading WAL file, WalTransactionStart is decision-maker whether to load the txn from WAL or not
    if (loading_wal && !should_commit) continue;

    try {
      // Any other delta may observe the effect of the buffered deletions, so the batch is flushed before it runs.
      // Guarding here rather than inside each handler keeps the invariant in one place: the transaction-end delta
      // (which commits) and the vertex-delete deltas (which require detached vertices) are both covered, as is
      // every handler added later.
      if (!std::holds_alternative<WalEdgeDelete>(delta.data_)) {
        flush_pending_edge_deletes();
      }
      std::visit(delta_apply, delta.data_);
    } catch (const std::exception &e) {
      spdlog::error("Applying deltas failed because of {}", e.what());
      return std::nullopt;
    }
    applied_deltas++;
  }

  // The transaction-end delta goes through the flush guard before it commits, so the buffer is always drained
  // by the time the loop exits.
  MG_ASSERT(pending_edge_deletes.empty(), "Buffered edge deletions were never applied");

  spdlog::debug("Applied {} deltas, skipped {} already applied deltas. Committed {} txns.",
                applied_deltas,
                skipped_deltas,
                num_committed_txns);

  return SingleTxnDeltasProcessingResult{.commit_acc = std::move(commit_accessor),
                                         .current_delta_idx = current_delta_idx,
                                         .num_txns_committed = num_committed_txns};
}

}  // namespace memgraph::storage
