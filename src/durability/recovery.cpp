#include "durability/recovery.hpp"

#include <limits>
#include <unordered_map>

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/paths.hpp"
#include "durability/version.hpp"
#include "durability/wal.hpp"
#include "query/typed_value.hpp"
#include "transactions/type.hpp"
#include "utils/string.hpp"

namespace durability {

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

namespace {
using communication::bolt::DecodedValue;

// A data structure for exchanging info between main recovery function and
// snapshot and WAL recovery functions.
struct RecoveryData {
  tx::transaction_id_t snapshooter_tx_id{0};
  std::vector<tx::transaction_id_t> snapshooter_tx_snapshot;
  // A collection into which the indexes should be added so they
  // can be rebuilt at the end of the recovery transaction.
  std::vector<std::pair<std::string, std::string>> indexes;

  void Clear() {
    snapshooter_tx_id = 0;
    snapshooter_tx_snapshot.clear();
    indexes.clear();
  }
};

#define RETURN_IF_NOT(condition) \
  if (!(condition)) {            \
    reader.Close();              \
    return false;                \
  }

bool RecoverSnapshot(const fs::path &snapshot_file, GraphDb &db,
                     RecoveryData &recovery_data) {
  HashedFileReader reader;
  communication::bolt::Decoder<HashedFileReader> decoder(reader);

  RETURN_IF_NOT(reader.Open(snapshot_file));
  std::unordered_map<uint64_t, VertexAccessor> vertices;

  auto magic_number = durability::kMagicNumber;
  reader.Read(magic_number.data(), magic_number.size());
  RETURN_IF_NOT(magic_number == durability::kMagicNumber);

  // Read the vertex and edge count, and the hash, from the end of the snapshot.
  int64_t vertex_count;
  int64_t edge_count;
  uint64_t hash;
  RETURN_IF_NOT(
      durability::ReadSnapshotSummary(reader, vertex_count, edge_count, hash));

  DecodedValue dv;

  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::Int) &&
                dv.ValueInt() == durability::kVersion);

  // Vertex and edge generator ids
  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::Int));
  uint64_t vertex_generator_cnt = dv.ValueInt();
  db.VertexGenerator().SetId(
      std::max(db.VertexGenerator().LocalCount(), vertex_generator_cnt));
  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::Int));
  uint64_t edge_generator_cnt = dv.ValueInt();
  db.EdgeGenerator().SetId(
      std::max(db.EdgeGenerator().LocalCount(), edge_generator_cnt));

  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::Int));
  recovery_data.snapshooter_tx_id = dv.ValueInt();
  // Transaction snapshot of the transaction that created the snapshot.
  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::List));
  for (const auto &value : dv.ValueList()) {
    RETURN_IF_NOT(value.IsInt());
    recovery_data.snapshooter_tx_snapshot.emplace_back(value.ValueInt());
  }

  // A list of label+property indexes.
  RETURN_IF_NOT(decoder.ReadValue(&dv, DecodedValue::Type::List));
  auto index_value = dv.ValueList();
  for (auto it = index_value.begin(); it != index_value.end();) {
    auto label = *it++;
    RETURN_IF_NOT(it != index_value.end());
    auto property = *it++;
    RETURN_IF_NOT(label.IsString() && property.IsString());
    recovery_data.indexes.emplace_back(label.ValueString(),
                                       property.ValueString());
  }

  GraphDbAccessor dba(db);
  for (int64_t i = 0; i < vertex_count; ++i) {
    DecodedValue vertex_dv;
    RETURN_IF_NOT(decoder.ReadValue(&vertex_dv, DecodedValue::Type::Vertex));
    auto &vertex = vertex_dv.ValueVertex();
    auto vertex_accessor = dba.InsertVertex(vertex.id);
    for (const auto &label : vertex.labels) {
      vertex_accessor.add_label(dba.Label(label));
    }
    for (const auto &property_pair : vertex.properties) {
      vertex_accessor.PropsSet(dba.Property(property_pair.first),
                               query::TypedValue(property_pair.second));
    }
    vertices.insert({vertex.id, vertex_accessor});
  }
  for (int64_t i = 0; i < edge_count; ++i) {
    DecodedValue edge_dv;
    RETURN_IF_NOT(decoder.ReadValue(&edge_dv, DecodedValue::Type::Edge));
    auto &edge = edge_dv.ValueEdge();
    auto it_from = vertices.find(edge.from);
    auto it_to = vertices.find(edge.to);
    RETURN_IF_NOT(it_from != vertices.end() && it_to != vertices.end());
    auto edge_accessor = dba.InsertEdge(it_from->second, it_to->second,
                                        dba.EdgeType(edge.type), edge.id);

    for (const auto &property_pair : edge.properties)
      edge_accessor.PropsSet(dba.Property(property_pair.first),
                             query::TypedValue(property_pair.second));
  }

  // Vertex and edge counts are included in the hash. Re-read them to update the
  // hash.
  reader.ReadType(vertex_count);
  reader.ReadType(edge_count);
  if (!reader.Close() || reader.hash() != hash) {
    dba.Abort();
    return false;
  }
  dba.Commit();
  return true;
}

#undef RETURN_IF_NOT

// TODO - finer-grained recovery feedback could be useful here.
bool RecoverWal(const fs::path &wal_dir, GraphDbAccessor &db_accessor,
                RecoveryData &recovery_data) {
  // Get paths to all the WAL files and sort them (on date).
  std::vector<fs::path> wal_files;
  if (!fs::exists(wal_dir)) return true;
  for (auto &wal_file : fs::directory_iterator(wal_dir))
    wal_files.emplace_back(wal_file);
  std::sort(wal_files.begin(), wal_files.end());

  // Track which transaction should be recovered next.
  tx::transaction_id_t next_to_recover = recovery_data.snapshooter_tx_id + 1;

  // Some transactions that come after the first to recover need to be skipped
  // (if they committed before the snapshot, and are not in the snapshot's tx
  // snapshot).
  std::set<tx::transaction_id_t> to_skip;

  if (!recovery_data.snapshooter_tx_snapshot.empty()) {
    std::set<tx::transaction_id_t> txs{
        recovery_data.snapshooter_tx_snapshot.begin(),
        recovery_data.snapshooter_tx_snapshot.end()};
    next_to_recover = *txs.begin();
    for (tx::transaction_id_t i = next_to_recover;
         i < recovery_data.snapshooter_tx_id; ++i)
      if (txs.find(i) == txs.end()) to_skip.emplace(i);

    // We don't try to recover the snapshooter transaction.
    to_skip.emplace(recovery_data.snapshooter_tx_id);
  }

  // A buffer for the WAL transaction deltas. Accumulate and apply them in the
  // right transactional sequence.
  std::map<tx::transaction_id_t, std::vector<database::StateDelta>> deltas;
  // Track which transactions were aborted/committed in the WAL.
  std::set<tx::transaction_id_t> aborted;
  std::set<tx::transaction_id_t> committed;

  auto apply_all_possible = [&]() {
    while (true) {
      // Remove old deltas from memory.
      for (auto it = deltas.begin(); it != deltas.end();) {
        if (it->first < next_to_recover)
          it = deltas.erase(it);
        else
          ++it;
      }

      // Check if we can apply skip/apply the next transaction.
      if (to_skip.find(next_to_recover) != to_skip.end())
        next_to_recover++;
      else if (utils::Contains(aborted, next_to_recover)) {
        next_to_recover++;
      } else if (utils::Contains(committed, next_to_recover)) {
        auto found = deltas.find(next_to_recover);
        if (found != deltas.end())
          for (const auto &delta : found->second) delta.Apply(db_accessor);
        next_to_recover++;
      } else
        break;
    }
  };

  // Read all the WAL files whose max_tx_id is not smaller then
  // min_tx_to_recover
  for (auto &wal_file : wal_files) {
    auto wal_file_tx_id = TransactionIdFromWalFilename(wal_file.filename());
    if (!wal_file_tx_id || *wal_file_tx_id < next_to_recover) continue;

    HashedFileReader wal_reader;
    if (!wal_reader.Open(wal_file)) return false;
    communication::bolt::Decoder<HashedFileReader> decoder(wal_reader);
    while (true) {
      auto delta = database::StateDelta::Decode(wal_reader, decoder);
      if (!delta) break;
      switch (delta->type()) {
        case database::StateDelta::Type::TRANSACTION_BEGIN:
          DCHECK(deltas.find(delta->transaction_id()) == deltas.end())
              << "Double transaction start";
          if (to_skip.find(delta->transaction_id()) == to_skip.end())
            deltas.emplace(delta->transaction_id(),
                           std::vector<database::StateDelta>{});
          break;
        case database::StateDelta::Type::TRANSACTION_ABORT: {
          auto it = deltas.find(delta->transaction_id());
          if (it != deltas.end()) deltas.erase(it);
          aborted.emplace(delta->transaction_id());
          apply_all_possible();
          break;
        }
        case database::StateDelta::Type::TRANSACTION_COMMIT:
          committed.emplace(delta->transaction_id());
          apply_all_possible();
          break;
        case database::StateDelta::Type::BUILD_INDEX: {
          recovery_data.indexes.emplace_back(delta->IndexName());
          break;
        }
        default: {
          auto it = deltas.find(delta->transaction_id());
          if (it != deltas.end()) it->second.emplace_back(*delta);
        }
      }
    }  // reading all deltas in a single wal file
  }    // reading all wal files

  apply_all_possible();

  // TODO when implementing proper error handling return one of the following:
  // - WAL fully recovered
  // - WAL partially recovered
  // - WAL recovery error
  return true;
}
}  // anonymous namespace

bool Recover(const fs::path &durability_dir, GraphDb &db) {
  RecoveryData recovery_data;

  // Attempt to recover from snapshot files in reverse order (from newest
  // backwards).
  const auto snapshot_dir = durability_dir / kSnapshotDir;
  std::vector<fs::path> snapshot_files;
  if (fs::exists(snapshot_dir) && fs::is_directory(snapshot_dir))
    for (auto &file : fs::directory_iterator(snapshot_dir))
      snapshot_files.emplace_back(file);
  std::sort(snapshot_files.rbegin(), snapshot_files.rend());
  for (auto &snapshot_file : snapshot_files) {
    LOG(INFO) << "Starting snapshot recovery from: " << snapshot_file;
    if (!RecoverSnapshot(snapshot_file, db, recovery_data)) {
      recovery_data.Clear();
      LOG(WARNING) << "Snapshot recovery failed, trying older snapshot...";
      continue;
    } else {
      LOG(INFO) << "Snapshot recovery successful.";
      break;
    }
  }

  // Write-ahead-log recovery.
  GraphDbAccessor db_accessor(db);
  // WAL recovery does not have to be complete for the recovery to be
  // considered successful. For the time being ignore the return value,
  // consider a better system.
  RecoverWal(durability_dir / kWalDir, db_accessor, recovery_data);
  db_accessor.Commit();

  // Index recovery.
  GraphDbAccessor db_accessor_indices{db};
  for (const auto &label_prop : recovery_data.indexes)
    db_accessor_indices.BuildIndex(
        db_accessor_indices.Label(label_prop.first),
        db_accessor_indices.Property(label_prop.second));
  db_accessor_indices.Commit();
  return true;
}
}  // namespace durability
