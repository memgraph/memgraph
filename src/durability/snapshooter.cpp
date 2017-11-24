#include <algorithm>

#include <glog/logging.h>

#include "durability/snapshooter.hpp"

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "database/graph_db_accessor.hpp"
#include "durability/hashed_file_writer.hpp"
#include "durability/paths.hpp"
#include "durability/version.hpp"
#include "utils/datetime/timestamp.hpp"

namespace fs = std::experimental::filesystem;

namespace durability {

namespace {
bool Encode(const fs::path &snapshot_file, GraphDbAccessor &db_accessor_) {
  try {
    HashedFileWriter buffer(snapshot_file);
    communication::bolt::BaseEncoder<HashedFileWriter> encoder(buffer);
    int64_t vertex_num = 0, edge_num = 0;

    encoder.WriteRAW(durability::kMagicNumber.data(),
                     durability::kMagicNumber.size());
    encoder.WriteInt(durability::kVersion);

    // Write the ID of the transaction doing the snapshot.
    encoder.WriteInt(db_accessor_.transaction_id());

    // Write the transaction snapshot into the snapshot. It's used when
    // recovering from the combination of snapshot and write-ahead-log.
    {
      std::vector<query::TypedValue> tx_snapshot;
      for (int64_t tx : db_accessor_.transaction().snapshot())
        tx_snapshot.emplace_back(tx);
      encoder.WriteList(tx_snapshot);
    }

    // Write label+property indexes as list ["label", "property", ...]
    {
      std::vector<query::TypedValue> index_vec;
      for (const auto &key : db_accessor_.GetIndicesKeys()) {
        index_vec.emplace_back(db_accessor_.LabelName(key.label_));
        index_vec.emplace_back(db_accessor_.PropertyName(key.property_));
      }
      encoder.WriteList(index_vec);
    }

    for (const auto &vertex : db_accessor_.Vertices(false)) {
      encoder.WriteVertex(vertex);
      vertex_num++;
    }
    for (const auto &edge : db_accessor_.Edges(false)) {
      encoder.WriteEdge(edge);
      edge_num++;
    }
    buffer.WriteValue(vertex_num);
    buffer.WriteValue(edge_num);
    buffer.WriteValue(buffer.hash());
    buffer.Close();
  } catch (const std::ifstream::failure &) {
    if (fs::exists(snapshot_file) && !fs::remove(snapshot_file)) {
      LOG(ERROR) << "Error while removing corrupted snapshot file: "
                 << snapshot_file;
    }
    return false;
  }
  return true;
}

// Removes snaposhot files so that only `max_retained` latest ones are kept. If
// `max_retained == -1`, all the snapshots are retained.
void RemoveOldSnapshots(const fs::path &snapshot_dir, int max_retained) {
  if (max_retained == -1) return;
  std::vector<fs::path> files;
  for (auto &file : fs::directory_iterator(snapshot_dir))
    files.push_back(file.path());
  if (static_cast<int>(files.size()) <= max_retained) return;
  sort(files.begin(), files.end());
  for (int i = 0; i < static_cast<int>(files.size()) - max_retained; ++i) {
    if (!fs::remove(files[i])) {
      LOG(ERROR) << "Error while removing file: " << files[i];
    }
  }
}

// Removes write-ahead log files that are no longer necessary (they don't get
// used when recovering from the latest snapshot.
void RemoveOldWals(const fs::path &wal_dir,
                   const tx::Transaction &snapshot_transaction) {
  if (!fs::exists(wal_dir)) return;
  // We can remove all the WAL files that will not be used when restoring from
  // the snapshot created in the given transaction.
  auto min_trans_id = snapshot_transaction.snapshot().empty()
                          ? snapshot_transaction.id_
                          : snapshot_transaction.snapshot().front();
  for (auto &wal_file : fs::directory_iterator(wal_dir)) {
    auto tx_id = TransactionIdFromWalFilename(wal_file.path().filename());
    if (tx_id && tx_id.value() < min_trans_id) fs::remove(wal_file);
  }
}
}  // annonnymous namespace

fs::path MakeSnapshotPath(const fs::path &durability_dir) {
  std::string date_str =
      Timestamp(Timestamp::now())
          .to_string("{:04d}_{:02d}_{:02d}__{:02d}_{:02d}_{:02d}_{:05d}");
  return durability_dir / kSnapshotDir / date_str;
}

bool MakeSnapshot(GraphDbAccessor &db_accessor_, const fs::path &durability_dir,
                  const int snapshot_max_retained) {
  auto ensure_dir = [](const auto &dir) {
    if (!fs::exists(dir) && !fs::create_directories(dir)) {
      LOG(ERROR) << "Error while creating directory " << dir;
      return false;
    }
    return true;
  };
  if (!ensure_dir(durability_dir)) return false;
  if (!ensure_dir(durability_dir / kSnapshotDir)) return false;
  const auto snapshot_file = MakeSnapshotPath(durability_dir);
  if (fs::exists(snapshot_file)) return false;
  if (Encode(snapshot_file, db_accessor_)) {
    RemoveOldSnapshots(durability_dir / kSnapshotDir, snapshot_max_retained);
    RemoveOldWals(durability_dir / kWalDir, db_accessor_.transaction());
    return true;
  } else {
    std::error_code error_code;  // Just for exception suppression.
    fs::remove(snapshot_file, error_code);
    return false;
  }
}
}  // namespace durability
