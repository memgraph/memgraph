#include "durability/recovery.hpp"

#include <experimental/filesystem>
#include <limits>
#include <unordered_map>

#include "database/graph_db_accessor.hpp"
#include "database/indexes/label_property_index.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/paths.hpp"
#include "durability/snapshot_decoder.hpp"
#include "durability/snapshot_value.hpp"
#include "durability/version.hpp"
#include "durability/wal.hpp"
#include "glue/conversion.hpp"
#include "query/typed_value.hpp"
#include "storage/address_types.hpp"
#include "transactions/type.hpp"
#include "utils/algorithm.hpp"
#include "utils/file.hpp"

namespace fs = std::experimental::filesystem;

namespace durability {

using communication::bolt::Value;
bool ReadSnapshotSummary(HashedFileReader &buffer, int64_t &vertex_count,
                         int64_t &edge_count, uint64_t &hash) {
  auto pos = buffer.Tellg();
  auto offset = sizeof(vertex_count) + sizeof(edge_count) + sizeof(hash);
  buffer.Seek(-offset, std::ios_base::end);
  bool r_val = buffer.ReadType(vertex_count, false) &&
               buffer.ReadType(edge_count, false) &&
               buffer.ReadType(hash, false);
  buffer.Seek(pos);
  return r_val;
}

bool VersionConsistency(const fs::path &durability_dir) {
  const auto snapshot_dir = durability_dir / kSnapshotDir;
  if (fs::exists(snapshot_dir) && fs::is_directory(snapshot_dir)) {
    for (auto &file : fs::directory_iterator(snapshot_dir)) {
      HashedFileReader reader;
      SnapshotDecoder<HashedFileReader> decoder(reader);

      // This is ok because we are only trying to detect version
      // inconsistencies.
      if (!reader.Open(fs::path(file))) continue;

      auto magic_number = durability::kMagicNumber;
      if (!reader.Read(magic_number.data(), magic_number.size())) continue;

      Value dv;
      if (!decoder.ReadValue(&dv, Value::Type::Int) ||
          dv.ValueInt() != durability::kVersion)
        return false;
    }
  }

  const auto wal_dir = durability_dir / kWalDir;
  if (fs::exists(snapshot_dir) && fs::is_directory(wal_dir)) {
    for (auto &file : fs::directory_iterator(wal_dir)) {
      HashedFileReader reader;
      communication::bolt::Decoder<HashedFileReader> decoder(reader);
      if (!reader.Open(fs::path(file))) continue;
      Value dv;
      if (!decoder.ReadValue(&dv, Value::Type::Int) ||
          dv.ValueInt() != durability::kVersion)
        return false;
    }
  }

  return true;
}

bool ContainsDurabilityFiles(const fs::path &durability_dir) {
  for (auto &durability_type : {kSnapshotDir, kWalDir}) {
    const auto dtype_dir = durability_dir / durability_type;
    if (fs::exists(dtype_dir) && fs::is_directory(dtype_dir) &&
        !fs::is_empty(dtype_dir))
      return true;
  }
  return false;
}

void MoveToBackup(const fs::path &durability_dir) {
  const auto backup_dir = durability_dir / kBackupDir;
  utils::CheckDir(backup_dir);
  utils::CheckDir(backup_dir / kSnapshotDir);
  utils::CheckDir(backup_dir / kWalDir);
  for (auto &durability_type : {kSnapshotDir, kWalDir}) {
    const auto dtype_dir = durability_dir / durability_type;
    if (!fs::exists(dtype_dir) || !fs::is_directory(dtype_dir)) continue;
    for (auto &file : fs::directory_iterator(dtype_dir)) {
      const auto filename = fs::path(file).filename();
      fs::rename(file, backup_dir / durability_type / filename);
    }
  }
}

namespace {
using communication::bolt::Value;

#define RETURN_IF_NOT(condition) \
  if (!(condition)) {            \
    reader.Close();              \
    return false;                \
  }

bool RecoverSnapshot(const fs::path &snapshot_file, database::GraphDb *db,
                     RecoveryData *recovery_data, int worker_id) {
  HashedFileReader reader;
  SnapshotDecoder<HashedFileReader> decoder(reader);

  RETURN_IF_NOT(reader.Open(snapshot_file));

  auto magic_number = durability::kMagicNumber;
  reader.Read(magic_number.data(), magic_number.size());
  RETURN_IF_NOT(magic_number == durability::kMagicNumber);

  // Read the vertex and edge count, and the hash, from the end of the snapshot.
  int64_t vertex_count;
  int64_t edge_count;
  uint64_t hash;
  RETURN_IF_NOT(
      durability::ReadSnapshotSummary(reader, vertex_count, edge_count, hash));

  Value dv;
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::Int) &&
                dv.ValueInt() == durability::kVersion);

  // Checks worker id was set correctly
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::Int) &&
                dv.ValueInt() == worker_id);

  // Vertex and edge generator ids
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::Int));
  uint64_t vertex_generator_cnt = dv.ValueInt();
  db->storage().VertexGenerator().SetId(std::max(
      db->storage().VertexGenerator().LocalCount(), vertex_generator_cnt));
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::Int));
  uint64_t edge_generator_cnt = dv.ValueInt();
  db->storage().EdgeGenerator().SetId(
      std::max(db->storage().EdgeGenerator().LocalCount(), edge_generator_cnt));

  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::Int));
  recovery_data->snapshooter_tx_id = dv.ValueInt();
  // Transaction snapshot of the transaction that created the snapshot.
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::List));
  for (const auto &value : dv.ValueList()) {
    RETURN_IF_NOT(value.IsInt());
    recovery_data->snapshooter_tx_snapshot.emplace_back(value.ValueInt());
  }

  // A list of label+property indexes.
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::List));
  auto index_value = dv.ValueList();
  for (auto it = index_value.begin(); it != index_value.end();) {
    auto label = *it++;
    RETURN_IF_NOT(it != index_value.end());
    auto property = *it++;
    RETURN_IF_NOT(label.IsString() && property.IsString());
    recovery_data->indexes.emplace_back(label.ValueString(),
                                        property.ValueString());
  }

  auto dba = db->Access();
  std::unordered_map<gid::Gid,
                     std::pair<storage::VertexAddress, storage::VertexAddress>>
      edge_gid_endpoints_mapping;

  for (int64_t i = 0; i < vertex_count; ++i) {
    auto vertex = decoder.ReadSnapshotVertex();
    RETURN_IF_NOT(vertex);

    auto vertex_accessor = dba->InsertVertex(vertex->gid, vertex->cypher_id);
    for (const auto &label : vertex->labels) {
      vertex_accessor.add_label(dba->Label(label));
    }
    for (const auto &property_pair : vertex->properties) {
      vertex_accessor.PropsSet(dba->Property(property_pair.first),
                               glue::ToTypedValue(property_pair.second));
    }
    auto vertex_record = vertex_accessor.GetNew();
    for (const auto &edge : vertex->in) {
      vertex_record->in_.emplace(edge.vertex, edge.address,
                                 dba->EdgeType(edge.type));
      edge_gid_endpoints_mapping[edge.address.gid()] = {
          edge.vertex, vertex_accessor.GlobalAddress()};
    }
    for (const auto &edge : vertex->out) {
      vertex_record->out_.emplace(edge.vertex, edge.address,
                                  dba->EdgeType(edge.type));
      edge_gid_endpoints_mapping[edge.address.gid()] = {
          vertex_accessor.GlobalAddress(), edge.vertex};
    }
  }

  auto vertex_transform_to_local_if_possible =
      [&dba, worker_id](storage::VertexAddress &address) {
        if (address.is_local()) return;
        // If the worker id matches it should be a local apperance
        if (address.worker_id() == worker_id) {
          address = storage::VertexAddress(
              dba->db().storage().LocalAddress<Vertex>(address.gid()));
          CHECK(address.is_local()) << "Address should be local but isn't";
        }
      };

  auto edge_transform_to_local_if_possible =
      [&dba, worker_id](storage::EdgeAddress &address) {
        if (address.is_local()) return;
        // If the worker id matches it should be a local apperance
        if (address.worker_id() == worker_id) {
          address = storage::EdgeAddress(
              dba->db().storage().LocalAddress<Edge>(address.gid()));
          CHECK(address.is_local()) << "Address should be local but isn't";
        }
      };

  Value dv_cypher_id;

  for (int64_t i = 0; i < edge_count; ++i) {
    RETURN_IF_NOT(
        decoder.ReadValue(&dv, communication::bolt::Value::Type::Edge));
    auto &edge = dv.ValueEdge();

    // Read cypher_id
    RETURN_IF_NOT(decoder.ReadValue(&dv_cypher_id,
                                    communication::bolt::Value::Type::Int));
    auto cypher_id = dv_cypher_id.ValueInt();

    // We have to take full edge endpoints from vertices since the endpoints
    // found here don't containt worker_id, and this can't be changed since this
    // edges must be bolt-compliant
    auto &edge_endpoints = edge_gid_endpoints_mapping[edge.id.AsUint()];

    storage::VertexAddress from;
    storage::VertexAddress to;
    std::tie(from, to) = edge_endpoints;

    // From and to are written in the global_address format and we should
    // convert them back to local format for speedup - if possible
    vertex_transform_to_local_if_possible(from);
    vertex_transform_to_local_if_possible(to);

    auto edge_accessor = dba->InsertOnlyEdge(from, to, dba->EdgeType(edge.type),
                                             edge.id.AsUint(), cypher_id);

    for (const auto &property_pair : edge.properties)
      edge_accessor.PropsSet(dba->Property(property_pair.first),
                             glue::ToTypedValue(property_pair.second));
  }

  // Vertex and edge counts are included in the hash. Re-read them to update the
  // hash.
  reader.ReadType(vertex_count);
  reader.ReadType(edge_count);
  if (!reader.Close() || reader.hash() != hash) {
    dba->Abort();
    return false;
  }

  // We have to replace global_ids with local ids where possible for all edges
  // in every vertex and this can only be done after we inserted the edges; this
  // is to speedup execution
  for (auto &vertex_accessor : dba->Vertices(true)) {
    auto vertex = vertex_accessor.GetNew();
    auto iterate_and_transform =
        [vertex_transform_to_local_if_possible,
         edge_transform_to_local_if_possible](Edges &edges) {
          Edges transformed;
          for (auto &element : edges) {
            auto vertex = element.vertex;
            vertex_transform_to_local_if_possible(vertex);

            auto edge = element.edge;
            edge_transform_to_local_if_possible(edge);

            transformed.emplace(vertex, edge, element.edge_type);
          }

          return transformed;
        };

    vertex->in_ = iterate_and_transform(vertex->in_);
    vertex->out_ = iterate_and_transform(vertex->out_);
  }

  // Ensure that the next transaction ID in the recovered DB will be greater
  // than the latest one we have recovered. Do this to make sure that
  // subsequently created snapshots and WAL files will have transactional info
  // that does not interfere with that found in previous snapshots and WAL.
  tx::TransactionId max_id = recovery_data->snapshooter_tx_id;
  auto &snap = recovery_data->snapshooter_tx_snapshot;
  if (!snap.empty()) max_id = *std::max_element(snap.begin(), snap.end());
  dba->db().tx_engine().EnsureNextIdGreater(max_id);
  dba->Commit();
  return true;
}

#undef RETURN_IF_NOT

std::vector<fs::path> GetWalFiles(const fs::path &wal_dir) {
  // Get paths to all the WAL files and sort them (on date).
  std::vector<fs::path> wal_files;
  if (!fs::exists(wal_dir)) return {};
  for (auto &wal_file : fs::directory_iterator(wal_dir))
    wal_files.emplace_back(wal_file);
  std::sort(wal_files.begin(), wal_files.end());
  return wal_files;
}

bool ApplyOverDeltas(
    const std::vector<fs::path> &wal_files, tx::TransactionId first_to_recover,
    const std::function<void(const database::StateDelta &)> &f) {
  for (auto &wal_file : wal_files) {
    auto wal_file_max_tx_id = TransactionIdFromWalFilename(wal_file.filename());
    if (!wal_file_max_tx_id || *wal_file_max_tx_id < first_to_recover) continue;

    HashedFileReader wal_reader;
    if (!wal_reader.Open(wal_file)) return false;

    communication::bolt::Decoder<HashedFileReader> decoder(wal_reader);

    Value dv;
    if (!decoder.ReadValue(&dv, Value::Type::Int) ||
        dv.ValueInt() != durability::kVersion)
      return false;

    while (true) {
      auto delta = database::StateDelta::Decode(wal_reader, decoder);
      if (!delta) break;
      f(*delta);
    }
  }

  return true;
}

auto FirstWalTxToRecover(const RecoveryData &recovery_data) {
  auto &tx_sn = recovery_data.snapshooter_tx_snapshot;
  auto first_to_recover = tx_sn.empty() ? recovery_data.snapshooter_tx_id + 1
                                        : *std::min(tx_sn.begin(), tx_sn.end());
  return first_to_recover;
}

std::vector<tx::TransactionId> ReadWalRecoverableTransactions(
    const fs::path &wal_dir, database::GraphDb *db,
    const RecoveryData &recovery_data) {
  auto wal_files = GetWalFiles(wal_dir);

  std::unordered_set<tx::TransactionId> committed_set;
  auto first_to_recover = FirstWalTxToRecover(recovery_data);
  ApplyOverDeltas(
      wal_files, first_to_recover, [&](const database::StateDelta &delta) {
        if (delta.transaction_id >= first_to_recover &&
            delta.type == database::StateDelta::Type::TRANSACTION_COMMIT) {
          committed_set.insert(delta.transaction_id);
        }
      });

  std::vector<tx::TransactionId> committed_tx_ids(committed_set.size());
  for (auto id : committed_set) committed_tx_ids.push_back(id);
  return committed_tx_ids;
}

// TODO - finer-grained recovery feedback could be useful here.
bool RecoverWal(const fs::path &wal_dir, database::GraphDb *db,
                RecoveryData *recovery_data,
                RecoveryTransactions *transactions) {
  auto wal_files = GetWalFiles(wal_dir);
  // Track which transaction should be recovered first, and define logic for
  // which transactions should be skipped in recovery.
  auto &tx_sn = recovery_data->snapshooter_tx_snapshot;
  auto first_to_recover = FirstWalTxToRecover(*recovery_data);

  // Set of transactions which can be recovered, since not every transaction in
  // wal can be recovered because it might not be present on some workers (there
  // wasn't enough time for it to flush to disk or similar)
  std::unordered_set<tx::TransactionId> common_wal_tx;
  for (auto tx_id : recovery_data->wal_tx_to_recover)
    common_wal_tx.insert(tx_id);

  auto should_skip = [&tx_sn, recovery_data, &common_wal_tx,
                      first_to_recover](tx::TransactionId tx_id) {
    return tx_id < first_to_recover ||
           (tx_id < recovery_data->snapshooter_tx_id &&
            !utils::Contains(tx_sn, tx_id)) ||
           !utils::Contains(common_wal_tx, tx_id);
  };

  // Ensure that the next transaction ID in the recovered DB will be greater
  // than the latest one we have recovered. Do this to make sure that
  // subsequently created snapshots and WAL files will have transactional info
  // that does not interfere with that found in previous snapshots and WAL.
  tx::TransactionId max_observed_tx_id{0};

  // Read all the WAL files whose max_tx_id is not smaller than
  // min_tx_to_recover.
  ApplyOverDeltas(
      wal_files, first_to_recover, [&](const database::StateDelta &delta) {
        max_observed_tx_id = std::max(max_observed_tx_id, delta.transaction_id);
        if (should_skip(delta.transaction_id)) return;
        switch (delta.type) {
          case database::StateDelta::Type::TRANSACTION_BEGIN:
            transactions->Begin(delta.transaction_id);
            break;
          case database::StateDelta::Type::TRANSACTION_ABORT:
            transactions->Abort(delta.transaction_id);
            break;
          case database::StateDelta::Type::TRANSACTION_COMMIT:
            transactions->Commit(delta.transaction_id);
            break;
          case database::StateDelta::Type::BUILD_INDEX:
            // TODO index building might still be problematic in HA
            recovery_data->indexes.emplace_back(delta.label_name,
                                                delta.property_name);
            break;
          default:
            transactions->Apply(delta);
        }
      });

  // TODO when implementing proper error handling return one of the following:
  // - WAL fully recovered
  // - WAL partially recovered
  // - WAL recovery error

  db->tx_engine().EnsureNextIdGreater(max_observed_tx_id);
  return true;
}

}  // anonymous namespace

RecoveryInfo RecoverOnlySnapshot(
    const fs::path &durability_dir, database::GraphDb *db,
    RecoveryData *recovery_data,
    std::experimental::optional<tx::TransactionId> required_snapshot_tx_id,
    int worker_id) {
  // Attempt to recover from snapshot files in reverse order (from newest
  // backwards).
  const auto snapshot_dir = durability_dir / kSnapshotDir;
  std::vector<fs::path> snapshot_files;
  if (fs::exists(snapshot_dir) && fs::is_directory(snapshot_dir))
    for (auto &file : fs::directory_iterator(snapshot_dir))
      snapshot_files.emplace_back(file);
  std::sort(snapshot_files.rbegin(), snapshot_files.rend());
  for (auto &snapshot_file : snapshot_files) {
    if (required_snapshot_tx_id) {
      auto snapshot_file_tx_id =
          TransactionIdFromSnapshotFilename(snapshot_file);
      if (!snapshot_file_tx_id ||
          snapshot_file_tx_id.value() != *required_snapshot_tx_id) {
        LOG(INFO) << "Skipping snapshot file '" << snapshot_file
                  << "' because it does not match the required snapshot tx id: "
                  << *required_snapshot_tx_id;
        continue;
      }
    }
    LOG(INFO) << "Starting snapshot recovery from: " << snapshot_file;
    if (!RecoverSnapshot(snapshot_file, db, recovery_data, worker_id)) {
      db->ReinitializeStorage();
      recovery_data->Clear();
      LOG(WARNING) << "Snapshot recovery failed, trying older snapshot...";
      continue;
    } else {
      LOG(INFO) << "Snapshot recovery successful.";
      break;
    }
  }

  // If snapshot recovery is required, and we failed, don't even deal with
  // the WAL recovery.
  if (required_snapshot_tx_id &&
      recovery_data->snapshooter_tx_id != *required_snapshot_tx_id)
    return {recovery_data->snapshooter_tx_id, {}};

  return {recovery_data->snapshooter_tx_id,
          ReadWalRecoverableTransactions(durability_dir / kWalDir, db,
                                         *recovery_data)};
}

void RecoverWalAndIndexes(const fs::path &durability_dir, database::GraphDb *db,
                          RecoveryData *recovery_data,
                          RecoveryTransactions *transactions) {
  // Write-ahead-log recovery.
  // WAL recovery does not have to be complete for the recovery to be
  // considered successful. For the time being ignore the return value,
  // consider a better system.
  RecoverWal(durability_dir / kWalDir, db, recovery_data, transactions);

  // Index recovery.
  auto db_accessor_indices = db->Access();
  for (const auto &label_prop : recovery_data->indexes) {
    const database::LabelPropertyIndex::Key key{
        db_accessor_indices->Label(label_prop.first),
        db_accessor_indices->Property(label_prop.second)};
    db_accessor_indices->db().storage().label_property_index().CreateIndex(key);
    db_accessor_indices->PopulateIndex(key);
    db_accessor_indices->EnableIndex(key);
  }
  db_accessor_indices->Commit();
}

}  // namespace durability
