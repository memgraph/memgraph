#include "storage/v2/durability/durability.hpp"

#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include <algorithm>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "storage/v2/durability/paths.hpp"
#include "utils/uuid.hpp"

namespace storage::durability {

namespace {

/// Verifies that the owner of the storage directory is the same user that
/// started the current process.
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

}  // namespace

Durability::Durability(Config::Durability config,
                       utils::SkipList<Vertex> *vertices,
                       utils::SkipList<Edge> *edges,
                       NameIdMapper *name_id_mapper,
                       std::atomic<uint64_t> *edge_count, Indices *indices,
                       Constraints *constraints, Config::Items items)
    : config_(config),
      vertices_(vertices),
      edges_(edges),
      name_id_mapper_(name_id_mapper),
      edge_count_(edge_count),
      indices_(indices),
      constraints_(constraints),
      items_(items),
      storage_directory_(config_.storage_directory),
      snapshot_directory_(config_.storage_directory / kSnapshotDirectory),
      wal_directory_(config_.storage_directory / kWalDirectory),
      lock_file_path_(config_.storage_directory / ".lock"),
      uuid_(utils::GenerateUUID()) {}

std::optional<RecoveryInfo> Durability::Initialize(
    std::function<void(std::function<void(Transaction *)>)>
        execute_with_transaction) {
  execute_with_transaction_ = execute_with_transaction;
  if (config_.snapshot_wal_mode !=
          Config::Durability::SnapshotWalMode::DISABLED ||
      config_.snapshot_on_exit || config_.recover_on_startup) {
    // Create the directory initially to crash the database in case of
    // permission errors. This is done early to crash the database on startup
    // instead of crashing the database for the first time during runtime (which
    // could be an unpleasant surprise).
    utils::EnsureDirOrDie(snapshot_directory_);
    // Same reasoning as above.
    utils::EnsureDirOrDie(wal_directory_);

    // Verify that the user that started the process is the same user that is
    // the owner of the storage directory.
    VerifyStorageDirectoryOwnerAndProcessUserOrDie(storage_directory_);

    // Create the lock file and open a handle to it. This will crash the
    // database if it can't open the file for writing or if any other process is
    // holding the file opened.
    lock_file_handle_.Open(lock_file_path_,
                           utils::OutputFile::Mode::OVERWRITE_EXISTING);
    CHECK(lock_file_handle_.AcquireLock())
        << "Couldn't acquire lock on the storage directory "
        << storage_directory_
        << "!\nAnother Memgraph process is currently running with the same "
           "storage directory, please stop it first before starting this "
           "process!";
  }
  std::optional<RecoveryInfo> ret;
  if (config_.recover_on_startup) {
    ret = RecoverData();
  } else if (config_.snapshot_wal_mode !=
                 Config::Durability::SnapshotWalMode::DISABLED ||
             config_.snapshot_on_exit) {
    bool files_moved = false;
    auto backup_root = config_.storage_directory / kBackupDirectory;
    for (const auto &[path, dirname, what] :
         {std::make_tuple(snapshot_directory_, kSnapshotDirectory, "snapshot"),
          std::make_tuple(wal_directory_, kWalDirectory, "WAL")}) {
      if (!utils::DirExists(path)) continue;
      auto backup_curr = backup_root / dirname;
      std::error_code error_code;
      for (const auto &item :
           std::filesystem::directory_iterator(path, error_code)) {
        utils::EnsureDirOrDie(backup_root);
        utils::EnsureDirOrDie(backup_curr);
        std::error_code item_error_code;
        std::filesystem::rename(
            item.path(), backup_curr / item.path().filename(), item_error_code);
        CHECK(!item_error_code)
            << "Couldn't move " << what << " file " << item.path()
            << " because of: " << item_error_code.message();
        files_moved = true;
      }
      CHECK(!error_code) << "Couldn't backup " << what
                         << " files because of: " << error_code.message();
    }
    LOG_IF(WARNING, files_moved)
        << "Since Memgraph was not supposed to recover on startup and "
           "durability is enabled, your current durability files will likely "
           "be overridden. To prevent important data loss, Memgraph has stored "
           "those files into a .backup directory inside the storage directory.";
  }
  if (config_.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Run("Snapshot", config_.snapshot_interval, [this] {
      execute_with_transaction_([this](Transaction *transaction) {
        CreateSnapshot(transaction, snapshot_directory_, wal_directory_,
                       config_.snapshot_retention_count, vertices_, edges_,
                       name_id_mapper_, indices_, constraints_, items_, uuid_);
      });
    });
  }
  return ret;
}

void Durability::Finalize() {
  wal_file_ = std::nullopt;
  if (config_.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Stop();
  }
  if (config_.snapshot_on_exit) {
    execute_with_transaction_([this](Transaction *transaction) {
      CreateSnapshot(transaction, snapshot_directory_, wal_directory_,
                     config_.snapshot_retention_count, vertices_, edges_,
                     name_id_mapper_, indices_, constraints_, items_, uuid_);
    });
  }
}

void Durability::AppendToWal(const Transaction &transaction,
                             uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  // Traverse deltas and append them to the WAL file.
  // A single transaction will always be contained in a single WAL file.
  auto current_commit_timestamp =
      transaction.commit_timestamp->load(std::memory_order_acquire);
  // Helper lambda that traverses the delta chain on order to find the first
  // delta that should be processed and then appends all discovered deltas.
  auto find_and_apply_deltas = [&](const auto *delta, const auto &parent,
                                   auto filter) {
    while (true) {
      auto older = delta->next.load(std::memory_order_acquire);
      if (older == nullptr ||
          older->timestamp->load(std::memory_order_acquire) !=
              current_commit_timestamp)
        break;
      delta = older;
    }
    while (true) {
      if (filter(delta->action)) {
        wal_file_->AppendDelta(*delta, parent, final_commit_timestamp);
      }
      auto prev = delta->prev.Get();
      if (prev.type != PreviousPtr::Type::DELTA) break;
      delta = prev.delta;
    }
  };

  // The deltas are ordered correctly in the `transaction.deltas` buffer, but we
  // don't traverse them in that order. That is because for each delta we need
  // information about the vertex or edge they belong to and that information
  // isn't stored in the deltas themselves. In order to find out information
  // about the corresponding vertex or edge it is necessary to traverse the
  // delta chain for each delta until a vertex or edge is encountered. This
  // operation is very expensive as the chain grows.
  // Instead, we traverse the edges until we find a vertex or edge and traverse
  // their delta chains. This approach has a drawback because we lose the
  // correct order of the operations. Because of that, we need to traverse the
  // deltas several times and we have to manually ensure that the stored deltas
  // will be ordered correctly.

  // 1. Process all Vertex deltas and store all operations that create vertices
  // and modify vertex data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
          return true;

        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 2. Process all Vertex deltas and store all operations that create edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::REMOVE_OUT_EDGE:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
          return false;
      }
    });
  }
  // 3. Process all Edge deltas and store all operations that modify edge data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::EDGE) continue;
    find_and_apply_deltas(&delta, *prev.edge, [](auto action) {
      switch (action) {
        case Delta::Action::SET_PROPERTY:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 4. Process all Vertex deltas and store all operations that delete edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::ADD_OUT_EDGE:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 5. Process all Vertex deltas and store all operations that delete vertices.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::RECREATE_OBJECT:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }

  // Add a delta that indicates that the transaction is fully written to the WAL
  // file.
  wal_file_->AppendTransactionEnd(final_commit_timestamp);

  FinalizeWalFile();
}

void Durability::AppendToWal(StorageGlobalOperation operation, LabelId label,
                             const std::set<PropertyId> &properties,
                             uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  wal_file_->AppendOperation(operation, label, properties,
                             final_commit_timestamp);
  FinalizeWalFile();
}

std::optional<RecoveryInfo> Durability::RecoverData() {
  if (!utils::DirExists(snapshot_directory_) &&
      !utils::DirExists(wal_directory_))
    return std::nullopt;

  // Helper lambda used to recover all discovered indices and constraints. The
  // indices and constraints must be recovered after the data recovery is done
  // to ensure that the indices and constraints are consistent at the end of the
  // recovery process.
  auto recover_indices_and_constraints = [this](
                                             const auto &indices_constraints) {
    // Recover label indices.
    for (const auto &item : indices_constraints.indices.label) {
      if (!indices_->label_index.CreateIndex(item, vertices_->access()))
        throw RecoveryFailure("The label index must be created here!");
    }

    // Recover label+property indices.
    for (const auto &item : indices_constraints.indices.label_property) {
      if (!indices_->label_property_index.CreateIndex(item.first, item.second,
                                                      vertices_->access()))
        throw RecoveryFailure("The label+property index must be created here!");
    }

    // Recover existence constraints.
    for (const auto &item : indices_constraints.constraints.existence) {
      auto ret = CreateExistenceConstraint(constraints_, item.first,
                                           item.second, vertices_->access());
      if (ret.HasError() || !ret.GetValue())
        throw RecoveryFailure("The existence constraint must be created here!");
    }

    // Recover unique constraints.
    for (const auto &item : indices_constraints.constraints.unique) {
      auto ret = constraints_->unique_constraints.CreateConstraint(
          item.first, item.second, vertices_->access());
      if (ret.HasError() ||
          ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS)
        throw RecoveryFailure("The unique constraint must be created here!");
    }
  };

  // Array of all discovered snapshots, ordered by name.
  std::vector<std::pair<std::filesystem::path, std::string>> snapshot_files;
  std::error_code error_code;
  if (utils::DirExists(snapshot_directory_)) {
    for (const auto &item :
         std::filesystem::directory_iterator(snapshot_directory_, error_code)) {
      if (!item.is_regular_file()) continue;
      try {
        auto info = ReadSnapshotInfo(item.path());
        snapshot_files.emplace_back(item.path(), info.uuid);
      } catch (const RecoveryFailure &) {
        continue;
      }
    }
    CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                       << error_code.message() << "!";
  }

  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;
  std::optional<uint64_t> snapshot_timestamp;
  if (!snapshot_files.empty()) {
    std::sort(snapshot_files.begin(), snapshot_files.end());
    // UUID used for durability is the UUID of the last snapshot file.
    uuid_ = snapshot_files.back().second;
    std::optional<RecoveredSnapshot> recovered_snapshot;
    for (auto it = snapshot_files.rbegin(); it != snapshot_files.rend(); ++it) {
      const auto &[path, uuid] = *it;
      if (uuid != uuid_) {
        LOG(WARNING) << "The snapshot file " << path
                     << " isn't related to the latest snapshot file!";
        continue;
      }
      LOG(INFO) << "Starting snapshot recovery from " << path;
      try {
        recovered_snapshot = LoadSnapshot(path, vertices_, edges_,
                                          name_id_mapper_, edge_count_, items_);
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
    if (!utils::DirExists(wal_directory_)) {
      recover_indices_and_constraints(indices_constraints);
      return recovered_snapshot->recovery_info;
    }
  } else {
    if (!utils::DirExists(wal_directory_)) return std::nullopt;
    // Array of all discovered WAL files, ordered by name.
    std::vector<std::pair<std::filesystem::path, std::string>> wal_files;
    for (const auto &item :
         std::filesystem::directory_iterator(wal_directory_, error_code)) {
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
    uuid_ = wal_files.back().second;
  }

  // Array of all discovered WAL files, ordered by sequence number.
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t, std::filesystem::path>>
      wal_files;
  for (const auto &item :
       std::filesystem::directory_iterator(wal_directory_, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      auto info = ReadWalInfo(item.path());
      if (info.uuid != uuid_) continue;
      wal_files.emplace_back(info.seq_num, info.from_timestamp,
                             info.to_timestamp, item.path());
    } catch (const RecoveryFailure &e) {
      continue;
    }
  }
  CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                     << error_code.message() << "!";
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
    std::sort(wal_files.begin(), wal_files.end());
    {
      const auto &[seq_num, from_timestamp, to_timestamp, path] = wal_files[0];
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
    for (const auto &[seq_num, from_timestamp, to_timestamp, path] :
         wal_files) {
      if (previous_seq_num && *previous_seq_num + 1 != seq_num) {
        LOG(FATAL) << "You are missing a WAL file with the sequence number "
                   << *previous_seq_num + 1 << "!";
      }
      previous_seq_num = seq_num;
      try {
        auto info =
            LoadWal(path, &indices_constraints, snapshot_timestamp, vertices_,
                    edges_, name_id_mapper_, edge_count_, items_);
        recovery_info.next_vertex_id =
            std::max(recovery_info.next_vertex_id, info.next_vertex_id);
        recovery_info.next_edge_id =
            std::max(recovery_info.next_edge_id, info.next_edge_id);
        recovery_info.next_timestamp =
            std::max(recovery_info.next_timestamp, info.next_timestamp);
      } catch (const RecoveryFailure &e) {
        LOG(FATAL) << "Couldn't recover WAL deltas from " << path
                   << " because of: " << e.what();
      }
    }
    // The sequence number needs to be recovered even though `LoadWal` didn't
    // load any deltas from that file.
    wal_seq_num_ = *previous_seq_num + 1;
  }

  recover_indices_and_constraints(indices_constraints);
  return recovery_info;
}

bool Durability::InitializeWalFile() {
  if (config_.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL)
    return false;
  if (!wal_file_) {
    wal_file_.emplace(wal_directory_, uuid_, items_, name_id_mapper_,
                      wal_seq_num_++);
  }
  return true;
}

void Durability::FinalizeWalFile() {
  ++wal_unsynced_transactions_;
  if (wal_unsynced_transactions_ >= config_.wal_file_flush_every_n_tx) {
    wal_file_->Sync();
    wal_unsynced_transactions_ = 0;
  }
  if (wal_file_->GetSize() / 1024 >= config_.wal_file_size_kibibytes) {
    wal_file_ = std::nullopt;
    wal_unsynced_transactions_ = 0;
  }
}

}  // namespace storage::durability
