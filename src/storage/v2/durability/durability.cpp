#include "storage/v2/durability/durability.hpp"

#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <algorithm>
#include <tuple>
#include <utility>
#include <vector>

#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/wal.hpp"

namespace storage::durability {

void VerifyStorageDirectoryOwnerAndProcessUserOrDie(
    const std::filesystem::path &storage_directory) {
  // Get the process user ID.
  auto process_euid = geteuid();

  // Get the data directory owner ID.
  struct stat statbuf;
  auto ret = stat(storage_directory.c_str(), &statbuf);
  if (ret != 0 && errno == ENOENT) {
    // The directory doesn't currently exist.
    return;
  }
  CHECK(ret == 0) << "Couldn't get stat for '" << storage_directory
                  << "' because of: " << strerror(errno) << " (" << errno
                  << ")";
  auto directory_owner = statbuf.st_uid;

  auto get_username = [](auto uid) {
    auto info = getpwuid(uid);
    if (!info) return std::to_string(uid);
    return std::string(info->pw_name);
  };

  auto user_process = get_username(process_euid);
  auto user_directory = get_username(directory_owner);
  CHECK(process_euid == directory_owner)
      << "The process is running as user " << user_process
      << ", but the data directory is owned by user " << user_directory
      << ". Please start the process as user " << user_directory << "!";
}

std::vector<SnapshotDurabilityInfo> GetSnapshotFiles(
    const std::filesystem::path &snapshot_directory,
    const std::string_view uuid) {
  std::vector<SnapshotDurabilityInfo> snapshot_files;
  std::error_code error_code;
  if (utils::DirExists(snapshot_directory)) {
    for (const auto &item :
         std::filesystem::directory_iterator(snapshot_directory, error_code)) {
      if (!item.is_regular_file()) continue;
      try {
        auto info = ReadSnapshotInfo(item.path());
        if (uuid.empty() || info.uuid == uuid) {
          snapshot_files.emplace_back(item.path(), std::move(info.uuid),
                                      info.start_timestamp);
        }
      } catch (const RecoveryFailure &) {
        continue;
      }
    }
    CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                       << error_code.message() << "!";
  }

  return snapshot_files;
}

std::optional<std::vector<WalDurabilityInfo>> GetWalFiles(
    const std::filesystem::path &wal_directory, const std::string_view uuid,
    const std::optional<size_t> current_seq_num) {
  if (!utils::DirExists(wal_directory)) return std::nullopt;

  std::vector<WalDurabilityInfo> wal_files;
  std::error_code error_code;
  for (const auto &item :
       std::filesystem::directory_iterator(wal_directory, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      auto info = ReadWalInfo(item.path());
      if ((uuid.empty() || info.uuid == uuid) &&
          (!current_seq_num || info.seq_num < current_seq_num))
        wal_files.emplace_back(info.seq_num, info.from_timestamp,
                               info.to_timestamp, std::move(info.uuid),
                               item.path());
    } catch (const RecoveryFailure &e) {
      DLOG(WARNING) << "Failed to read " << item.path();
      continue;
    }
  }
  CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                     << error_code.message() << "!";

  std::sort(wal_files.begin(), wal_files.end());
  return std::move(wal_files);
}

// Function used to recover all discovered indices and constraints. The
// indices and constraints must be recovered after the data recovery is done
// to ensure that the indices and constraints are consistent at the end of the
// recovery process.
void RecoverIndicesAndConstraints(
    const RecoveredIndicesAndConstraints &indices_constraints, Indices *indices,
    Constraints *constraints, utils::SkipList<Vertex> *vertices) {
  // Recover label indices.
  for (const auto &item : indices_constraints.indices.label) {
    if (!indices->label_index.CreateIndex(item, vertices->access()))
      throw RecoveryFailure("The label index must be created here!");
  }

  // Recover label+property indices.
  for (const auto &item : indices_constraints.indices.label_property) {
    if (!indices->label_property_index.CreateIndex(item.first, item.second,
                                                   vertices->access()))
      throw RecoveryFailure("The label+property index must be created here!");
  }

  // Recover existence constraints.
  for (const auto &item : indices_constraints.constraints.existence) {
    auto ret = CreateExistenceConstraint(constraints, item.first, item.second,
                                         vertices->access());
    if (ret.HasError() || !ret.GetValue())
      throw RecoveryFailure("The existence constraint must be created here!");
  }

  // Recover unique constraints.
  for (const auto &item : indices_constraints.constraints.unique) {
    auto ret = constraints->unique_constraints.CreateConstraint(
        item.first, item.second, vertices->access());
    if (ret.HasError() ||
        ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS)
      throw RecoveryFailure("The unique constraint must be created here!");
  }
}

std::optional<RecoveryInfo> RecoverData(
    const std::filesystem::path &snapshot_directory,
    const std::filesystem::path &wal_directory, std::string *uuid,
    utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
    std::atomic<uint64_t> *edge_count, NameIdMapper *name_id_mapper,
    Indices *indices, Constraints *constraints, Config::Items items,
    uint64_t *wal_seq_num) {
  if (!utils::DirExists(snapshot_directory) && !utils::DirExists(wal_directory))
    return std::nullopt;

  auto snapshot_files = GetSnapshotFiles(snapshot_directory);

  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;
  std::optional<uint64_t> snapshot_timestamp;
  if (!snapshot_files.empty()) {
    // Order the files by name
    std::sort(snapshot_files.begin(), snapshot_files.end());

    // UUID used for durability is the UUID of the last snapshot file.
    *uuid = snapshot_files.back().uuid;
    std::optional<RecoveredSnapshot> recovered_snapshot;
    for (auto it = snapshot_files.rbegin(); it != snapshot_files.rend(); ++it) {
      const auto &[path, file_uuid, _] = *it;
      if (file_uuid != *uuid) {
        LOG(WARNING) << "The snapshot file " << path
                     << " isn't related to the latest snapshot file!";
        continue;
      }
      LOG(INFO) << "Starting snapshot recovery from " << path;
      try {
        recovered_snapshot = LoadSnapshot(path, vertices, edges, name_id_mapper,
                                          edge_count, items);
        LOG(INFO) << "Snapshot recovery successful!";
        break;
      } catch (const RecoveryFailure &e) {
        LOG(WARNING) << "Couldn't recover snapshot from " << path
                     << " because of: " << e.what();
        continue;
      }
    }
    CHECK(recovered_snapshot)
        << "The database is configured to recover on startup, but couldn't "
           "recover using any of the specified snapshots! Please inspect them "
           "and restart the database.";
    recovery_info = recovered_snapshot->recovery_info;
    indices_constraints = std::move(recovered_snapshot->indices_constraints);
    snapshot_timestamp = recovered_snapshot->snapshot_info.start_timestamp;
    if (!utils::DirExists(wal_directory)) {
      RecoverIndicesAndConstraints(indices_constraints, indices, constraints,
                                   vertices);
      return recovered_snapshot->recovery_info;
    }
  } else {
    std::error_code error_code;
    if (!utils::DirExists(wal_directory)) return std::nullopt;
    // Array of all discovered WAL files, ordered by name.
    std::vector<std::pair<std::filesystem::path, std::string>> wal_files;
    for (const auto &item :
         std::filesystem::directory_iterator(wal_directory, error_code)) {
      if (!item.is_regular_file()) continue;
      try {
        auto info = ReadWalInfo(item.path());
        wal_files.emplace_back(item.path(), info.uuid);
      } catch (const RecoveryFailure &e) {
        continue;
      }
    }
    CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                       << error_code.message() << "!";
    if (wal_files.empty()) return std::nullopt;
    std::sort(wal_files.begin(), wal_files.end());
    // UUID used for durability is the UUID of the last WAL file.
    *uuid = wal_files.back().second;
  }

  auto maybe_wal_files = GetWalFiles(wal_directory, *uuid);
  if (!maybe_wal_files) return std::nullopt;

  // Array of all discovered WAL files, ordered by sequence number.
  auto &wal_files = *maybe_wal_files;

  // By this point we should have recovered from a snapshot, or we should have
  // found some WAL files to recover from in the above `else`. This is just a
  // sanity check to circumvent the following case: The database didn't recover
  // from a snapshot, the above `else` triggered to find the recovery UUID from
  // a WAL file. The above `else` has an early exit in case there are no WAL
  // files. Because we reached this point there must have been some WAL files
  // and we must have some WAL files after this second WAL directory iteration.
  CHECK(snapshot_timestamp || !wal_files.empty())
      << "The database didn't recover from a snapshot and didn't find any WAL "
         "files that match the last WAL file!";

  if (!wal_files.empty()) {
    {
      const auto &[seq_num, from_timestamp, to_timestamp, _, path] =
          wal_files[0];
      if (seq_num != 0) {
        // We don't have all WAL files. We need to see whether we need them all.
        if (!snapshot_timestamp) {
          // We didn't recover from a snapshot and we must have all WAL files
          // starting from the first one (seq_num == 0) to be able to recover
          // data from them.
          LOG(FATAL) << "There are missing prefix WAL files and data can't be "
                        "recovered without them!";
        } else if (to_timestamp >= *snapshot_timestamp) {
          // We recovered from a snapshot and we must have at least one WAL file
          // whose all deltas were created before the snapshot in order to
          // verify that nothing is missing from the beginning of the WAL chain.
          LOG(FATAL) << "You must have at least one WAL file that contains "
                        "deltas that were created before the snapshot file!";
        }
      }
    }
    std::optional<uint64_t> previous_seq_num;
    auto last_loaded_timestamp = snapshot_timestamp;
    for (const auto &[seq_num, from_timestamp, to_timestamp, _, path] :
         wal_files) {
      if (previous_seq_num && *previous_seq_num + 1 != seq_num &&
          *previous_seq_num != seq_num) {
        LOG(FATAL) << "You are missing a WAL file with the sequence number "
                   << *previous_seq_num + 1 << "!";
      }
      previous_seq_num = seq_num;
      try {
        auto info = LoadWal(path, &indices_constraints, last_loaded_timestamp,
                            vertices, edges, name_id_mapper, edge_count, items);
        recovery_info.next_vertex_id =
            std::max(recovery_info.next_vertex_id, info.next_vertex_id);
        recovery_info.next_edge_id =
            std::max(recovery_info.next_edge_id, info.next_edge_id);
        recovery_info.next_timestamp =
            std::max(recovery_info.next_timestamp, info.next_timestamp);
        last_loaded_timestamp.emplace(recovery_info.next_timestamp - 1);
      } catch (const RecoveryFailure &e) {
        LOG(FATAL) << "Couldn't recover WAL deltas from " << path
                   << " because of: " << e.what();
      }
    }
    // The sequence number needs to be recovered even though `LoadWal` didn't
    // load any deltas from that file.
    *wal_seq_num = *previous_seq_num + 1;
  }

  RecoverIndicesAndConstraints(indices_constraints, indices, constraints,
                               vertices);
  return recovery_info;
}

}  // namespace storage::durability
