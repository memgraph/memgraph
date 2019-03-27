#include "durability/single_node/recovery.hpp"

#include <experimental/filesystem>
#include <experimental/optional>
#include <limits>
#include <unordered_map>

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/single_node/paths.hpp"
#include "durability/single_node/version.hpp"
#include "durability/single_node/wal.hpp"
#include "glue/communication.hpp"
#include "storage/single_node/indexes/label_property_index.hpp"
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
  for (const auto &durability_type : {kSnapshotDir, kWalDir}) {
    auto recovery_dir = durability_dir / durability_type;
    if (!fs::exists(recovery_dir) || !fs::is_directory(recovery_dir)) continue;

    for (const auto &file : fs::directory_iterator(recovery_dir)) {
      HashedFileReader reader;
      communication::bolt::Decoder<HashedFileReader> decoder(reader);

      // The following checks are ok because we are only trying to detect
      // version inconsistencies.
      if (!reader.Open(fs::path(file))) continue;

      std::array<uint8_t, 4> target_magic_number =
          (durability_type == kSnapshotDir) ? durability::kSnapshotMagic
                                            : durability::kWalMagic;
      std::array<uint8_t, 4> magic_number;
      if (!reader.Read(magic_number.data(), magic_number.size())) continue;
      if (magic_number != target_magic_number) continue;

      if (reader.EndOfFile()) continue;

      Value dv;
      if (!decoder.ReadValue(&dv, Value::Type::Int) ||
          dv.ValueInt() != durability::kVersion)
        return false;
    }
  }

  return true;
}

bool DistributedVersionConsistency(const int64_t master_version) {
  return durability::kVersion == master_version;
}

bool ContainsDurabilityFiles(const fs::path &durability_dir) {
  for (const auto &durability_type : {kSnapshotDir, kWalDir}) {
    auto recovery_dir = durability_dir / durability_type;
    if (fs::exists(recovery_dir) && fs::is_directory(recovery_dir) &&
        !fs::is_empty(recovery_dir))
      return true;
  }
  return false;
}

void MoveToBackup(const fs::path &durability_dir) {
  auto backup_dir = durability_dir / kBackupDir;
  utils::EnsureDirOrDie(backup_dir);
  utils::EnsureDirOrDie(backup_dir / kSnapshotDir);
  utils::EnsureDirOrDie(backup_dir / kWalDir);
  for (const auto &durability_type : {kSnapshotDir, kWalDir}) {
    auto recovery_dir = durability_dir / durability_type;
    if (!fs::exists(recovery_dir) || !fs::is_directory(recovery_dir)) continue;
    for (const auto &file : fs::directory_iterator(recovery_dir)) {
      auto filename = fs::path(file).filename();
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
                     RecoveryData *recovery_data) {
  HashedFileReader reader;
  communication::bolt::Decoder<HashedFileReader> decoder(reader);

  RETURN_IF_NOT(reader.Open(snapshot_file));

  auto magic_number = durability::kSnapshotMagic;
  reader.Read(magic_number.data(), magic_number.size());
  RETURN_IF_NOT(magic_number == durability::kSnapshotMagic);

  // Read the vertex and edge count, and the hash, from the end of the snapshot.
  int64_t vertex_count;
  int64_t edge_count;
  uint64_t hash;
  RETURN_IF_NOT(
      durability::ReadSnapshotSummary(reader, vertex_count, edge_count, hash));

  Value dv;
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::Int) &&
                dv.ValueInt() == durability::kVersion);

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
    RETURN_IF_NOT(it != index_value.end());
    auto unique = *it++;
    RETURN_IF_NOT(label.IsString() && property.IsString() && unique.IsBool());
    recovery_data->indexes.emplace_back(
        IndexRecoveryData{label.ValueString(), property.ValueString(),
                          /*create = */ true, unique.ValueBool()});
  }

  // Read a list of existence constraints
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::List));
  auto existence_constraints = dv.ValueList();
  for (auto it = existence_constraints.begin();
       it != existence_constraints.end();) {
    RETURN_IF_NOT(it->IsString());
    auto label = it->ValueString();
    ++it;
    RETURN_IF_NOT(it != existence_constraints.end());
    RETURN_IF_NOT(it->IsInt());
    auto prop_size = it->ValueInt();
    ++it;
    std::vector<std::string> properties;
    properties.reserve(prop_size);
    for (size_t i = 0; i < prop_size; ++i) {
      RETURN_IF_NOT(it != existence_constraints.end());
      RETURN_IF_NOT(it->IsString());
      properties.emplace_back(it->ValueString());
      ++it;
    }

    recovery_data->existence_constraints.emplace_back(
        ExistenceConstraintRecoveryData{label, std::move(properties), true});
  }

  // Read a list of unique constraints
  RETURN_IF_NOT(decoder.ReadValue(&dv, Value::Type::List));
  auto unique_constraints = dv.ValueList();
  for (auto it = unique_constraints.begin();
       it != unique_constraints.end();) {
    RETURN_IF_NOT(it->IsString());
    auto label = it->ValueString();
    ++it;
    RETURN_IF_NOT(it != unique_constraints.end());
    RETURN_IF_NOT(it->IsInt());
    auto prop_size = it->ValueInt();
    ++it;
    std::vector<std::string> properties;
    properties.reserve(prop_size);
    for (size_t i = 0; i < prop_size; ++i) {
      RETURN_IF_NOT(it != unique_constraints.end());
      RETURN_IF_NOT(it->IsString());
      properties.emplace_back(it->ValueString());
      ++it;
    }

    recovery_data->unique_constraints.emplace_back(
        UniqueConstraintRecoveryData{label, std::move(properties), true});
  }

  auto dba = db->Access();
  std::unordered_map<uint64_t, VertexAccessor> vertices;
  for (int64_t i = 0; i < vertex_count; ++i) {
    Value vertex_dv;
    RETURN_IF_NOT(decoder.ReadValue(&vertex_dv, Value::Type::Vertex));
    auto &vertex = vertex_dv.ValueVertex();
    auto vertex_accessor = dba->InsertVertex(vertex.id.AsUint());

    for (const auto &label : vertex.labels) {
      vertex_accessor.add_label(dba->Label(label));
    }
    for (const auto &property_pair : vertex.properties) {
      vertex_accessor.PropsSet(dba->Property(property_pair.first),
                               glue::ToPropertyValue(property_pair.second));
    }
    vertices.insert({vertex.id.AsUint(), vertex_accessor});
  }

  for (int64_t i = 0; i < edge_count; ++i) {
    Value edge_dv;
    RETURN_IF_NOT(decoder.ReadValue(&edge_dv, Value::Type::Edge));
    auto &edge = edge_dv.ValueEdge();
    auto it_from = vertices.find(edge.from.AsUint());
    auto it_to = vertices.find(edge.to.AsUint());
    RETURN_IF_NOT(it_from != vertices.end() && it_to != vertices.end());
    auto edge_accessor =
        dba->InsertEdge(it_from->second, it_to->second,
                        dba->EdgeType(edge.type), edge.id.AsUint());

    for (const auto &property_pair : edge.properties)
      edge_accessor.PropsSet(dba->Property(property_pair.first),
                             glue::ToPropertyValue(property_pair.second));
  }

  // Vertex and edge counts are included in the hash. Re-read them to update the
  // hash.
  reader.ReadType(vertex_count);
  reader.ReadType(edge_count);
  if (!reader.Close() || reader.hash() != hash) {
    dba->Abort();
    return false;
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

bool ApplyOverDeltas(const std::vector<fs::path> &wal_files,
                     tx::TransactionId first_to_recover,
                     const std::function<void(const database::StateDelta &)> &f,
                     bool report_to_user) {
  for (auto &wal_file : wal_files) {
    auto wal_file_max_tx_id = TransactionIdFromWalFilename(wal_file.filename());
    if (!wal_file_max_tx_id || *wal_file_max_tx_id < first_to_recover) continue;

    HashedFileReader wal_reader;
    if (!wal_reader.Open(wal_file)) return false;

    communication::bolt::Decoder<HashedFileReader> decoder(wal_reader);

    auto magic_number = durability::kWalMagic;
    wal_reader.Read(magic_number.data(), magic_number.size());
    if (magic_number != durability::kWalMagic) return false;

    Value dv;
    if (!decoder.ReadValue(&dv, Value::Type::Int) ||
        dv.ValueInt() != durability::kVersion)
      return false;

    while (true) {
      auto delta = database::StateDelta::Decode(wal_reader, decoder);
      if (!delta) break;
      f(*delta);
    }

    LOG_IF(INFO, report_to_user) << "Applied WAL deltas from: " << wal_file;
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
      wal_files, first_to_recover,
      [&](const database::StateDelta &delta) {
        if (delta.transaction_id >= first_to_recover &&
            delta.type == database::StateDelta::Type::TRANSACTION_COMMIT) {
          committed_set.insert(delta.transaction_id);
        }
      },
      false);

  std::vector<tx::TransactionId> committed_tx_ids(committed_set.size());
  for (auto id : committed_set) committed_tx_ids.push_back(id);
  return committed_tx_ids;
}

}  // anonymous namespace

RecoveryInfo RecoverOnlySnapshot(
    const fs::path &durability_dir, database::GraphDb *db,
    RecoveryData *recovery_data,
    std::experimental::optional<tx::TransactionId> required_snapshot_tx_id) {
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
    if (!RecoverSnapshot(snapshot_file, db, recovery_data)) {
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
    return {durability::kVersion, recovery_data->snapshooter_tx_id, {}};

  return {durability::kVersion, recovery_data->snapshooter_tx_id,
          ReadWalRecoverableTransactions(durability_dir / kWalDir, db,
                                         *recovery_data)};
}

RecoveryTransactions::RecoveryTransactions(database::GraphDb *db) : db_(db) {}

void RecoveryTransactions::Begin(const tx::TransactionId &tx_id) {
  CHECK(accessors_.find(tx_id) == accessors_.end())
      << "Double transaction start";
  accessors_.emplace(tx_id, db_->Access());
}

void RecoveryTransactions::Abort(const tx::TransactionId &tx_id) {
  GetAccessor(tx_id)->Abort();
  accessors_.erase(accessors_.find(tx_id));
}

void RecoveryTransactions::Commit(const tx::TransactionId &tx_id) {
  GetAccessor(tx_id)->Commit();
  accessors_.erase(accessors_.find(tx_id));
}

void RecoveryTransactions::Apply(const database::StateDelta &delta) {
  delta.Apply(*GetAccessor(delta.transaction_id));
}

database::GraphDbAccessor *RecoveryTransactions::GetAccessor(
    const tx::TransactionId &tx_id) {
  auto found = accessors_.find(tx_id);
  CHECK(found != accessors_.end())
      << "Accessor does not exist for transaction: " << tx_id;
  return found->second.get();
}

// TODO - finer-grained recovery feedback could be useful here.
void RecoverWal(const fs::path &durability_dir, database::GraphDb *db,
                RecoveryData *recovery_data,
                RecoveryTransactions *transactions) {
  auto wal_dir = durability_dir / kWalDir;
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
          case database::StateDelta::Type::BUILD_INDEX: {
            auto drop_it = std::find_if(
                recovery_data->indexes.begin(), recovery_data->indexes.end(),
                [label = delta.label_name, property = delta.property_name](
                    const IndexRecoveryData &other) {
                  return other.label == label && other.property == property &&
                         other.create == false;
                });

            // If we already have a drop index in the recovery data, just erase
            // the drop index action. Otherwise add the build index action.
            if (drop_it != recovery_data->indexes.end()) {
              recovery_data->indexes.erase(drop_it);
            } else {
              recovery_data->indexes.emplace_back(
                  IndexRecoveryData{delta.label_name, delta.property_name,
                                    /*create = */ true, delta.unique});
            }
            break;
          }
          case database::StateDelta::Type::DROP_INDEX: {
            auto build_it = std::find_if(
                recovery_data->indexes.begin(), recovery_data->indexes.end(),
                [label = delta.label_name, property = delta.property_name](
                    const IndexRecoveryData &other) {
                  return other.label == label && other.property == property &&
                         other.create == true;
                });

            // If we already have a build index in the recovery data, just erase
            // the build index action. Otherwise add the drop index action.
            if (build_it != recovery_data->indexes.end()) {
              recovery_data->indexes.erase(build_it);
            } else {
              recovery_data->indexes.emplace_back(
                  IndexRecoveryData{delta.label_name, delta.property_name,
                                    /*create = */ false});
            }
            break;
          }
          case database::StateDelta::Type::BUILD_EXISTENCE_CONSTRAINT: {
            auto drop_it = std::find_if(
                recovery_data->existence_constraints.begin(),
                recovery_data->existence_constraints.end(),
                [&delta](const ExistenceConstraintRecoveryData &data) {
                  return data.label == delta.label_name &&
                         std::is_permutation(data.properties.begin(),
                                             data.properties.end(),
                                             delta.property_names.begin()) &&
                         data.create == false;
                });

            if (drop_it != recovery_data->existence_constraints.end()) {
              recovery_data->existence_constraints.erase(drop_it);
            } else {
              recovery_data->existence_constraints.emplace_back(
                  ExistenceConstraintRecoveryData{delta.label_name,
                                                  delta.property_names, true});
            }
            break;
          }
          case database::StateDelta::Type::DROP_EXISTENCE_CONSTRAINT: {
            auto build_it = std::find_if(
                recovery_data->existence_constraints.begin(),
                recovery_data->existence_constraints.end(),
                [&delta](const ExistenceConstraintRecoveryData &data) {
                  return data.label == delta.label_name &&
                         std::is_permutation(data.properties.begin(),
                                             data.properties.end(),
                                             delta.property_names.begin()) &&
                         data.create == true;
                });

            if (build_it != recovery_data->existence_constraints.end()) {
              recovery_data->existence_constraints.erase(build_it);
            } else {
              recovery_data->existence_constraints.emplace_back(
                  ExistenceConstraintRecoveryData{delta.label_name,
                                                  delta.property_names, false});
            }
            break;
          }
          case database::StateDelta::Type::BUILD_UNIQUE_CONSTRAINT: {
            auto drop_it = std::find_if(
                recovery_data->unique_constraints.begin(),
                recovery_data->unique_constraints.end(),
                [&delta](const UniqueConstraintRecoveryData &data) {
                  return data.label == delta.label_name &&
                         std::is_permutation(data.properties.begin(),
                                             data.properties.end(),
                                             delta.property_names.begin()) &&
                         data.create == false;
                });

            if (drop_it != recovery_data->unique_constraints.end()) {
              recovery_data->unique_constraints.erase(drop_it);
            } else {
              recovery_data->unique_constraints.emplace_back(
                  UniqueConstraintRecoveryData{delta.label_name,
                                               delta.property_names, true});
            }
            break;
          }
          case database::StateDelta::Type::DROP_UNIQUE_CONSTRAINT: {
            auto build_it = std::find_if(
                recovery_data->unique_constraints.begin(),
                recovery_data->unique_constraints.end(),
                [&delta](const UniqueConstraintRecoveryData &data) {
                  return data.label == delta.label_name &&
                         std::is_permutation(data.properties.begin(),
                                             data.properties.end(),
                                             delta.property_names.begin()) &&
                         data.create == true;
                });

            if (build_it != recovery_data->unique_constraints.end()) {
              recovery_data->unique_constraints.erase(build_it);
            } else {
              recovery_data->unique_constraints.emplace_back(
                  UniqueConstraintRecoveryData{delta.label_name,
                                               delta.property_names, false});
            }
            break;
          }
          default:
            transactions->Apply(delta);
        }
      },
      true);

  // TODO when implementing proper error handling return one of the following:
  // - WAL fully recovered
  // - WAL partially recovered
  // - WAL recovery error

  db->tx_engine().EnsureNextIdGreater(max_observed_tx_id);
}

void RecoverIndexes(database::GraphDb *db,
                    const std::vector<IndexRecoveryData> &indexes) {
  auto dba = db->Access();
  for (const auto &index : indexes) {
    auto label = dba->Label(index.label);
    auto property = dba->Property(index.property);
    if (index.create) {
      dba->BuildIndex(label, property, index.unique);
    } else {
      dba->DeleteIndex(label, property);
    }
  }
  dba->Commit();
}

void RecoverExistenceConstraints(
    database::GraphDb *db,
    const std::vector<ExistenceConstraintRecoveryData> &constraints) {
  auto dba = db->Access();
  for (auto &constraint : constraints) {
    auto label = dba->Label(constraint.label);
    std::vector<storage::Property> properties;
    properties.reserve(constraint.properties.size());
    for (auto &prop : constraint.properties) {
      properties.push_back(dba->Property(prop));
    }

    if (constraint.create) {
      dba->BuildExistenceConstraint(label, properties);
    } else {
      dba->DeleteExistenceConstraint(label, properties);
    }
  }
}

void RecoverUniqueConstraints(
    database::GraphDb *db,
    const std::vector<UniqueConstraintRecoveryData> &constraints) {
  auto dba = db->Access();
  for (auto &constraint : constraints) {
    auto label = dba->Label(constraint.label);
    std::vector<storage::Property> properties;
    properties.reserve(constraint.properties.size());
    for (auto &prop : constraint.properties) {
      properties.push_back(dba->Property(prop));
    }

    DCHECK(properties.size() == 1)
        << "Unique constraint with multiple properties is not supported";
    if (constraint.create) {
      dba->BuildUniqueConstraint(label, properties[0]);
    } else {
      dba->DeleteUniqueConstraint(label, properties[0]);
    }
  }
}
}  // namespace durability
