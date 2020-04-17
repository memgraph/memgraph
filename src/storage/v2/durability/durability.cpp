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
#include "storage/v2/durability/version.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/vertex_accessor.hpp"
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

std::optional<Durability::RecoveryInfo> Durability::Initialize(
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
  std::optional<Durability::RecoveryInfo> ret;
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
      execute_with_transaction_(
          [this](Transaction *transaction) { CreateSnapshot(transaction); });
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
    execute_with_transaction_(
        [this](Transaction *transaction) { CreateSnapshot(transaction); });
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

void Durability::CreateSnapshot(Transaction *transaction) {
  // Ensure that the storage directory exists.
  utils::EnsureDirOrDie(snapshot_directory_);

  // Create snapshot file.
  auto path =
      snapshot_directory_ / MakeSnapshotName(transaction->start_timestamp);
  LOG(INFO) << "Starting snapshot creation to " << path;
  Encoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic, kVersion);

  // Write placeholder offsets.
  uint64_t offset_offsets = 0;
  uint64_t offset_edges = 0;
  uint64_t offset_vertices = 0;
  uint64_t offset_indices = 0;
  uint64_t offset_constraints = 0;
  uint64_t offset_mapper = 0;
  uint64_t offset_metadata = 0;
  {
    snapshot.WriteMarker(Marker::SECTION_OFFSETS);
    offset_offsets = snapshot.GetPosition();
    snapshot.WriteUint(offset_edges);
    snapshot.WriteUint(offset_vertices);
    snapshot.WriteUint(offset_indices);
    snapshot.WriteUint(offset_constraints);
    snapshot.WriteUint(offset_mapper);
    snapshot.WriteUint(offset_metadata);
  }

  // Object counters.
  uint64_t edges_count = 0;
  uint64_t vertices_count = 0;

  // Mapper data.
  std::unordered_set<uint64_t> used_ids;
  auto write_mapping = [&snapshot, &used_ids](auto mapping) {
    used_ids.insert(mapping.AsUint());
    snapshot.WriteUint(mapping.AsUint());
  };

  // Store all edges.
  if (items_.properties_on_edges) {
    offset_edges = snapshot.GetPosition();
    auto acc = edges_->access();
    for (auto &edge : acc) {
      // The edge visibility check must be done here manually because we don't
      // allow direct access to the edges through the public API.
      bool is_visible = true;
      Delta *delta = nullptr;
      {
        std::lock_guard<utils::SpinLock> guard(edge.lock);
        is_visible = !edge.deleted;
        delta = edge.delta;
      }
      ApplyDeltasForRead(transaction, delta, View::OLD,
                         [&is_visible](const Delta &delta) {
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
                             case Delta::Action::DELETE_OBJECT: {
                               is_visible = false;
                               break;
                             }
                           }
                         });
      if (!is_visible) continue;
      EdgeRef edge_ref(&edge);
      // Here we create an edge accessor that we will use to get the
      // properties of the edge. The accessor is created with an invalid
      // type and invalid from/to pointers because we don't know them here,
      // but that isn't an issue because we won't use that part of the API
      // here.
      auto ea = EdgeAccessor{edge_ref,     EdgeTypeId::FromUint(0UL),
                             nullptr,      nullptr,
                             transaction,  indices_,
                             constraints_, items_};

      // Get edge data.
      auto maybe_props = ea.Properties(View::OLD);
      CHECK(maybe_props.HasValue()) << "Invalid database state!";

      // Store the edge.
      {
        snapshot.WriteMarker(Marker::SECTION_EDGE);
        snapshot.WriteUint(edge.gid.AsUint());
        const auto &props = maybe_props.GetValue();
        snapshot.WriteUint(props.size());
        for (const auto &item : props) {
          write_mapping(item.first);
          snapshot.WritePropertyValue(item.second);
        }
      }

      ++edges_count;
    }
  }

  // Store all vertices.
  {
    offset_vertices = snapshot.GetPosition();
    auto acc = vertices_->access();
    for (auto &vertex : acc) {
      // The visibility check is implemented for vertices so we use it here.
      auto va = VertexAccessor::Create(&vertex, transaction, indices_,
                                       constraints_, items_, View::OLD);
      if (!va) continue;

      // Get vertex data.
      // TODO (mferencevic): All of these functions could be written into a
      // single function so that we traverse the undo deltas only once.
      auto maybe_labels = va->Labels(View::OLD);
      CHECK(maybe_labels.HasValue()) << "Invalid database state!";
      auto maybe_props = va->Properties(View::OLD);
      CHECK(maybe_props.HasValue()) << "Invalid database state!";
      auto maybe_in_edges = va->InEdges(View::OLD);
      CHECK(maybe_in_edges.HasValue()) << "Invalid database state!";
      auto maybe_out_edges = va->OutEdges(View::OLD);
      CHECK(maybe_out_edges.HasValue()) << "Invalid database state!";

      // Store the vertex.
      {
        snapshot.WriteMarker(Marker::SECTION_VERTEX);
        snapshot.WriteUint(vertex.gid.AsUint());
        const auto &labels = maybe_labels.GetValue();
        snapshot.WriteUint(labels.size());
        for (const auto &item : labels) {
          write_mapping(item);
        }
        const auto &props = maybe_props.GetValue();
        snapshot.WriteUint(props.size());
        for (const auto &item : props) {
          write_mapping(item.first);
          snapshot.WritePropertyValue(item.second);
        }
        const auto &in_edges = maybe_in_edges.GetValue();
        snapshot.WriteUint(in_edges.size());
        for (const auto &item : in_edges) {
          snapshot.WriteUint(item.Gid().AsUint());
          snapshot.WriteUint(item.FromVertex().Gid().AsUint());
          write_mapping(item.EdgeType());
        }
        const auto &out_edges = maybe_out_edges.GetValue();
        snapshot.WriteUint(out_edges.size());
        for (const auto &item : out_edges) {
          snapshot.WriteUint(item.Gid().AsUint());
          snapshot.WriteUint(item.ToVertex().Gid().AsUint());
          write_mapping(item.EdgeType());
        }
      }

      ++vertices_count;
    }
  }

  // Write indices.
  {
    offset_indices = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_INDICES);

    // Write label indices.
    {
      auto label = indices_->label_index.ListIndices();
      snapshot.WriteUint(label.size());
      for (const auto &item : label) {
        write_mapping(item);
      }
    }

    // Write label+property indices.
    {
      auto label_property = indices_->label_property_index.ListIndices();
      snapshot.WriteUint(label_property.size());
      for (const auto &item : label_property) {
        write_mapping(item.first);
        write_mapping(item.second);
      }
    }
  }

  // Write constraints.
  {
    offset_constraints = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_CONSTRAINTS);

    // Write existence constraints.
    {
      auto existence = ListExistenceConstraints(*constraints_);
      snapshot.WriteUint(existence.size());
      for (const auto &item : existence) {
        write_mapping(item.first);
        write_mapping(item.second);
      }
    }

    // Write unique constraints.
    {
      auto unique = constraints_->unique_constraints.ListConstraints();
      snapshot.WriteUint(unique.size());
      for (const auto &item : unique) {
        write_mapping(item.first);
        snapshot.WriteUint(item.second.size());
        for (const auto &property : item.second) {
          write_mapping(property);
        }
      }
    }
  }

  // Write mapper data.
  {
    offset_mapper = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_MAPPER);
    snapshot.WriteUint(used_ids.size());
    for (auto item : used_ids) {
      snapshot.WriteUint(item);
      snapshot.WriteString(name_id_mapper_->IdToName(item));
    }
  }

  // Write metadata.
  {
    offset_metadata = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_METADATA);
    snapshot.WriteString(uuid_);
    snapshot.WriteUint(transaction->start_timestamp);
    snapshot.WriteUint(edges_count);
    snapshot.WriteUint(vertices_count);
  }

  // Write true offsets.
  {
    snapshot.SetPosition(offset_offsets);
    snapshot.WriteUint(offset_edges);
    snapshot.WriteUint(offset_vertices);
    snapshot.WriteUint(offset_indices);
    snapshot.WriteUint(offset_constraints);
    snapshot.WriteUint(offset_mapper);
    snapshot.WriteUint(offset_metadata);
  }

  // Finalize snapshot file.
  snapshot.Finalize();
  LOG(INFO) << "Snapshot creation successful!";

  // Ensure exactly `snapshot_retention_count` snapshots exist.
  std::vector<std::pair<uint64_t, std::filesystem::path>> old_snapshot_files;
  {
    std::error_code error_code;
    for (const auto &item :
         std::filesystem::directory_iterator(snapshot_directory_, error_code)) {
      if (!item.is_regular_file()) continue;
      if (item.path() == path) continue;
      try {
        auto info = ReadSnapshotInfo(item.path());
        if (info.uuid != uuid_) continue;
        old_snapshot_files.emplace_back(info.start_timestamp, item.path());
      } catch (const RecoveryFailure &e) {
        LOG(WARNING) << "Found a corrupt snapshot file " << item.path()
                     << " because of: " << e.what();
        continue;
      }
    }
    LOG_IF(ERROR, error_code)
        << "Couldn't ensure that exactly " << config_.snapshot_retention_count
        << " snapshots exist because an error occurred: "
        << error_code.message() << "!";
    std::sort(old_snapshot_files.begin(), old_snapshot_files.end());
    if (old_snapshot_files.size() > config_.snapshot_retention_count - 1) {
      auto num_to_erase =
          old_snapshot_files.size() - (config_.snapshot_retention_count - 1);
      for (size_t i = 0; i < num_to_erase; ++i) {
        const auto &[start_timestamp, snapshot_path] = old_snapshot_files[i];
        if (!utils::DeleteFile(snapshot_path)) {
          LOG(WARNING) << "Couldn't delete snapshot file " << snapshot_path
                       << "!";
        }
      }
      old_snapshot_files.erase(old_snapshot_files.begin(),
                               old_snapshot_files.begin() + num_to_erase);
    }
  }

  // Ensure that only the absolutely necessary WAL files exist.
  if (old_snapshot_files.size() == config_.snapshot_retention_count - 1 &&
      utils::DirExists(wal_directory_)) {
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t, std::filesystem::path>>
        wal_files;
    std::error_code error_code;
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
    LOG_IF(ERROR, error_code)
        << "Couldn't ensure that only the absolutely necessary WAL files exist "
           "because an error occurred: "
        << error_code.message() << "!";
    std::sort(wal_files.begin(), wal_files.end());
    uint64_t snapshot_start_timestamp = transaction->start_timestamp;
    if (!old_snapshot_files.empty()) {
      snapshot_start_timestamp = old_snapshot_files.front().first;
    }
    std::optional<uint64_t> pos = 0;
    for (uint64_t i = 0; i < wal_files.size(); ++i) {
      const auto &[seq_num, from_timestamp, to_timestamp, wal_path] =
          wal_files[i];
      if (to_timestamp <= snapshot_start_timestamp) {
        pos = i;
      } else {
        break;
      }
    }
    if (pos && *pos > 0) {
      // We need to leave at least one WAL file that contains deltas that were
      // created before the oldest snapshot. Because we always leave at least
      // one WAL file that contains deltas before the snapshot, this correctly
      // handles the edge case when that one file is the current WAL file that
      // is being appended to.
      for (uint64_t i = 0; i < *pos; ++i) {
        const auto &[seq_num, from_timestamp, to_timestamp, wal_path] =
            wal_files[i];
        if (!utils::DeleteFile(wal_path)) {
          LOG(WARNING) << "Couldn't delete WAL file " << wal_path << "!";
        }
      }
    }
  }
}

std::optional<Durability::RecoveryInfo> Durability::RecoverData() {
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
    std::optional<Durability::RecoveredSnapshot> recovered_snapshot;
    for (auto it = snapshot_files.rbegin(); it != snapshot_files.rend(); ++it) {
      const auto &[path, uuid] = *it;
      if (uuid != uuid_) {
        LOG(WARNING) << "The snapshot file " << path
                     << " isn't related to the latest snapshot file!";
        continue;
      }
      LOG(INFO) << "Starting snapshot recovery from " << path;
      try {
        recovered_snapshot = LoadSnapshot(path);
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
        auto info = LoadWal(path, &indices_constraints, snapshot_timestamp);
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

Durability::RecoveredSnapshot Durability::LoadSnapshot(
    const std::filesystem::path &path) {
  Durability::RecoveryInfo ret;
  RecoveredIndicesAndConstraints indices_constraints;

  Decoder snapshot;
  auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version)
    throw RecoveryFailure("Couldn't read snapshot magic and/or version!");
  if (!IsVersionSupported(*version))
    throw RecoveryFailure("Invalid snapshot version!");

  // Cleanup of loaded data in case of failure.
  bool success = false;
  utils::OnScopeExit cleanup([this, &success] {
    if (!success) {
      edges_->clear();
      vertices_->clear();
    }
  });

  // Read snapshot info.
  auto info = ReadSnapshotInfo(path);

  // Check for edges.
  bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    if (!snapshot.SetPosition(info.offset_mapper))
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER)
      throw RecoveryFailure("Invalid snapshot data!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Invalid snapshot data!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Invalid snapshot data!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Invalid snapshot data!");
      auto my_id = name_id_mapper_->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
    }
  }
  auto get_label_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end())
      throw RecoveryFailure("Invalid snapshot data!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end())
      throw RecoveryFailure("Invalid snapshot data!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end())
      throw RecoveryFailure("Invalid snapshot data!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count_->store(0, std::memory_order_release);

  {
    // Recover edges.
    auto edge_acc = edges_->access();
    uint64_t last_edge_gid = 0;
    if (snapshot_has_edges) {
      if (!snapshot.SetPosition(info.offset_edges))
        throw RecoveryFailure("Couldn't read data from snapshot!");
      for (uint64_t i = 0; i < info.edges_count; ++i) {
        {
          auto marker = snapshot.ReadMarker();
          if (!marker || *marker != Marker::SECTION_EDGE)
            throw RecoveryFailure("Invalid snapshot data!");
        }

        if (items_.properties_on_edges) {
          // Insert edge.
          auto gid = snapshot.ReadUint();
          if (!gid) throw RecoveryFailure("Invalid snapshot data!");
          if (i > 0 && *gid <= last_edge_gid)
            throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = *gid;
          auto [it, inserted] =
              edge_acc.insert(Edge{Gid::FromUint(*gid), nullptr});
          if (!inserted)
            throw RecoveryFailure("The edge must be inserted here!");

          // Recover properties.
          {
            auto props_size = snapshot.ReadUint();
            if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
            auto &props = it->properties;
            for (uint64_t j = 0; j < *props_size; ++j) {
              auto key = snapshot.ReadUint();
              if (!key) throw RecoveryFailure("Invalid snapshot data!");
              auto value = snapshot.ReadPropertyValue();
              if (!value) throw RecoveryFailure("Invalid snapshot data!");
              props.SetProperty(get_property_from_id(*key), *value);
            }
          }
        } else {
          // Read edge GID.
          auto gid = snapshot.ReadUint();
          if (!gid) throw RecoveryFailure("Invalid snapshot data!");
          if (i > 0 && *gid <= last_edge_gid)
            throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = *gid;

          // Read properties.
          {
            auto props_size = snapshot.ReadUint();
            if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
            if (*props_size != 0)
              throw RecoveryFailure(
                  "The snapshot has properties on edges, but the storage is "
                  "configured without properties on edges!");
          }
        }
      }
    }

    // Recover vertices (labels and properties).
    if (!snapshot.SetPosition(info.offset_vertices))
      throw RecoveryFailure("Couldn't read data from snapshot!");
    auto vertex_acc = vertices_->access();
    uint64_t last_vertex_gid = 0;
    for (uint64_t i = 0; i < info.vertices_count; ++i) {
      {
        auto marker = snapshot.ReadMarker();
        if (!marker || *marker != Marker::SECTION_VERTEX)
          throw RecoveryFailure("Invalid snapshot data!");
      }

      // Insert vertex.
      auto gid = snapshot.ReadUint();
      if (!gid) throw RecoveryFailure("Invalid snapshot data!");
      if (i > 0 && *gid <= last_vertex_gid) {
        throw RecoveryFailure("Invalid snapshot data!");
      }
      last_vertex_gid = *gid;
      auto [it, inserted] =
          vertex_acc.insert(Vertex{Gid::FromUint(*gid), nullptr});
      if (!inserted) throw RecoveryFailure("The vertex must be inserted here!");

      // Recover labels.
      {
        auto labels_size = snapshot.ReadUint();
        if (!labels_size) throw RecoveryFailure("Invalid snapshot data!");
        auto &labels = it->labels;
        labels.reserve(*labels_size);
        for (uint64_t j = 0; j < *labels_size; ++j) {
          auto label = snapshot.ReadUint();
          if (!label) throw RecoveryFailure("Invalid snapshot data!");
          labels.emplace_back(get_label_from_id(*label));
        }
      }

      // Recover properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
        auto &props = it->properties;
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Invalid snapshot data!");
          auto value = snapshot.ReadPropertyValue();
          if (!value) throw RecoveryFailure("Invalid snapshot data!");
          props.SetProperty(get_property_from_id(*key), *value);
        }
      }

      // Skip in edges.
      {
        auto in_size = snapshot.ReadUint();
        if (!in_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *in_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto from_gid = snapshot.ReadUint();
          if (!from_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Skip out edges.
      auto out_size = snapshot.ReadUint();
      if (!out_size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t j = 0; j < *out_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto to_gid = snapshot.ReadUint();
        if (!to_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");
      }
    }

    // Recover vertices (in/out edges).
    if (!snapshot.SetPosition(info.offset_vertices))
      throw RecoveryFailure("Couldn't read data from snapshot!");
    for (auto &vertex : vertex_acc) {
      {
        auto marker = snapshot.ReadMarker();
        if (!marker || *marker != Marker::SECTION_VERTEX)
          throw RecoveryFailure("Invalid snapshot data!");
      }

      // Check vertex.
      auto gid = snapshot.ReadUint();
      if (!gid) throw RecoveryFailure("Invalid snapshot data!");
      if (gid != vertex.gid.AsUint())
        throw RecoveryFailure("Invalid snapshot data!");

      // Skip labels.
      {
        auto labels_size = snapshot.ReadUint();
        if (!labels_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *labels_size; ++j) {
          auto label = snapshot.ReadUint();
          if (!label) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Skip properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Invalid snapshot data!");
          auto value = snapshot.SkipPropertyValue();
          if (!value) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Recover in edges.
      {
        auto in_size = snapshot.ReadUint();
        if (!in_size) throw RecoveryFailure("Invalid snapshot data!");
        vertex.in_edges.reserve(*in_size);
        for (uint64_t j = 0; j < *in_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = std::max(last_edge_gid, *edge_gid);

          auto from_gid = snapshot.ReadUint();
          if (!from_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");

          auto from_vertex = vertex_acc.find(Gid::FromUint(*from_gid));
          if (from_vertex == vertex_acc.end())
            throw RecoveryFailure("Invalid from vertex!");

          EdgeRef edge_ref(Gid::FromUint(*edge_gid));
          if (items_.properties_on_edges) {
            if (snapshot_has_edges) {
              auto edge = edge_acc.find(Gid::FromUint(*edge_gid));
              if (edge == edge_acc.end())
                throw RecoveryFailure("Invalid edge!");
              edge_ref = EdgeRef(&*edge);
            } else {
              auto [edge, inserted] =
                  edge_acc.insert(Edge{Gid::FromUint(*edge_gid), nullptr});
              edge_ref = EdgeRef(&*edge);
            }
          }
          vertex.in_edges.emplace_back(get_edge_type_from_id(*edge_type),
                                       &*from_vertex, edge_ref);
        }
      }

      // Recover out edges.
      {
        auto out_size = snapshot.ReadUint();
        if (!out_size) throw RecoveryFailure("Invalid snapshot data!");
        vertex.out_edges.reserve(*out_size);
        for (uint64_t j = 0; j < *out_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = std::max(last_edge_gid, *edge_gid);

          auto to_gid = snapshot.ReadUint();
          if (!to_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");

          auto to_vertex = vertex_acc.find(Gid::FromUint(*to_gid));
          if (to_vertex == vertex_acc.end())
            throw RecoveryFailure("Invalid to vertex!");

          EdgeRef edge_ref(Gid::FromUint(*edge_gid));
          if (items_.properties_on_edges) {
            if (snapshot_has_edges) {
              auto edge = edge_acc.find(Gid::FromUint(*edge_gid));
              if (edge == edge_acc.end())
                throw RecoveryFailure("Invalid edge!");
              edge_ref = EdgeRef(&*edge);
            } else {
              auto [edge, inserted] =
                  edge_acc.insert(Edge{Gid::FromUint(*edge_gid), nullptr});
              edge_ref = EdgeRef(&*edge);
            }
          }
          vertex.out_edges.emplace_back(get_edge_type_from_id(*edge_type),
                                        &*to_vertex, edge_ref);
        }
        // Increment edge count. We only increment the count here because the
        // information is duplicated in in_edges.
        edge_count_->fetch_add(*out_size, std::memory_order_acq_rel);
      }
    }

    // Set initial values for edge/vertex ID generators.
    ret.next_edge_id = last_edge_gid + 1;
    ret.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    if (!snapshot.SetPosition(info.offset_indices))
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES)
      throw RecoveryFailure("Invalid snapshot data!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label,
                                    get_label_from_id(*label),
                                    "The label index already exists!");
      }
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(
            &indices_constraints.indices.label_property,
            {get_label_from_id(*label), get_property_from_id(*property)},
            "The label+property index already exists!");
      }
    }
  }

  // Recover constraints.
  {
    if (!snapshot.SetPosition(info.offset_constraints))
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Invalid snapshot data!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(
            &indices_constraints.constraints.existence,
            {get_label_from_id(*label), get_property_from_id(*property)},
            "The existence constraint already exists!");
      }
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    if (*version >= kUniqueConstraintVersion) {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Invalid snapshot data!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Invalid snapshot data!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique,
                                    {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
      }
    }
  }

  // Recover timestamp.
  ret.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, ret, std::move(indices_constraints)};
}

Durability::RecoveryInfo Durability::LoadWal(
    const std::filesystem::path &path,
    RecoveredIndicesAndConstraints *indices_constraints,
    std::optional<uint64_t> snapshot_timestamp) {
  Durability::RecoveryInfo ret;

  Decoder wal;
  auto version = wal.Initialize(path, kWalMagic);
  if (!version)
    throw RecoveryFailure("Couldn't read WAL magic and/or version!");
  if (!IsVersionSupported(*version))
    throw RecoveryFailure("Invalid WAL version!");

  // Read wal info.
  auto info = ReadWalInfo(path);

  // Check timestamp.
  if (snapshot_timestamp && info.to_timestamp <= *snapshot_timestamp)
    return ret;

  // Recover deltas.
  wal.SetPosition(info.offset_deltas);
  uint64_t deltas_applied = 0;
  auto edge_acc = edges_->access();
  auto vertex_acc = vertices_->access();
  for (uint64_t i = 0; i < info.num_deltas; ++i) {
    // Read WAL delta header to find out the delta timestamp.
    auto timestamp = ReadWalDeltaHeader(&wal);

    if (!snapshot_timestamp || timestamp > *snapshot_timestamp) {
      // This delta should be loaded.
      auto delta = ReadWalDeltaData(&wal);
      switch (delta.type) {
        case WalDeltaData::Type::VERTEX_CREATE: {
          auto [vertex, inserted] = vertex_acc.insert(
              Vertex{delta.vertex_create_delete.gid, nullptr});
          if (!inserted)
            throw RecoveryFailure("The vertex must be inserted here!");

          ret.next_vertex_id = std::max(
              ret.next_vertex_id, delta.vertex_create_delete.gid.AsUint() + 1);

          break;
        }
        case WalDeltaData::Type::VERTEX_DELETE: {
          auto vertex = vertex_acc.find(delta.vertex_create_delete.gid);
          if (vertex == vertex_acc.end())
            throw RecoveryFailure("The vertex doesn't exist!");
          if (!vertex->in_edges.empty() || !vertex->out_edges.empty())
            throw RecoveryFailure(
                "The vertex can't be deleted because it still has edges!");

          if (!vertex_acc.remove(delta.vertex_create_delete.gid))
            throw RecoveryFailure("The vertex must be removed here!");

          break;
        }
        case WalDeltaData::Type::VERTEX_ADD_LABEL:
        case WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
          auto vertex = vertex_acc.find(delta.vertex_add_remove_label.gid);
          if (vertex == vertex_acc.end())
            throw RecoveryFailure("The vertex doesn't exist!");

          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.vertex_add_remove_label.label));
          auto it =
              std::find(vertex->labels.begin(), vertex->labels.end(), label_id);

          if (delta.type == WalDeltaData::Type::VERTEX_ADD_LABEL) {
            if (it != vertex->labels.end())
              throw RecoveryFailure("The vertex already has the label!");
            vertex->labels.push_back(label_id);
          } else {
            if (it == vertex->labels.end())
              throw RecoveryFailure("The vertex doesn't have the label!");
            std::swap(*it, vertex->labels.back());
            vertex->labels.pop_back();
          }

          break;
        }
        case WalDeltaData::Type::VERTEX_SET_PROPERTY: {
          auto vertex = vertex_acc.find(delta.vertex_edge_set_property.gid);
          if (vertex == vertex_acc.end())
            throw RecoveryFailure("The vertex doesn't exist!");

          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.vertex_edge_set_property.property));
          auto &property_value = delta.vertex_edge_set_property.value;

          vertex->properties.SetProperty(property_id, property_value);

          break;
        }
        case WalDeltaData::Type::EDGE_CREATE: {
          auto from_vertex =
              vertex_acc.find(delta.edge_create_delete.from_vertex);
          if (from_vertex == vertex_acc.end())
            throw RecoveryFailure("The from vertex doesn't exist!");
          auto to_vertex = vertex_acc.find(delta.edge_create_delete.to_vertex);
          if (to_vertex == vertex_acc.end())
            throw RecoveryFailure("The to vertex doesn't exist!");

          auto edge_gid = delta.edge_create_delete.gid;
          auto edge_type_id = EdgeTypeId::FromUint(
              name_id_mapper_->NameToId(delta.edge_create_delete.edge_type));
          EdgeRef edge_ref(edge_gid);
          if (items_.properties_on_edges) {
            auto [edge, inserted] = edge_acc.insert(Edge{edge_gid, nullptr});
            if (!inserted)
              throw RecoveryFailure("The edge must be inserted here!");
            edge_ref = EdgeRef(&*edge);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                edge_type_id, &*to_vertex, edge_ref};
            auto it = std::find(from_vertex->out_edges.begin(),
                                from_vertex->out_edges.end(), link);
            if (it != from_vertex->out_edges.end())
              throw RecoveryFailure("The from vertex already has this edge!");
            from_vertex->out_edges.push_back(link);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                edge_type_id, &*from_vertex, edge_ref};
            auto it = std::find(to_vertex->in_edges.begin(),
                                to_vertex->in_edges.end(), link);
            if (it != to_vertex->in_edges.end())
              throw RecoveryFailure("The to vertex already has this edge!");
            to_vertex->in_edges.push_back(link);
          }

          ret.next_edge_id = std::max(ret.next_edge_id, edge_gid.AsUint() + 1);

          // Increment edge count.
          edge_count_->fetch_add(1, std::memory_order_acq_rel);

          break;
        }
        case WalDeltaData::Type::EDGE_DELETE: {
          auto from_vertex =
              vertex_acc.find(delta.edge_create_delete.from_vertex);
          if (from_vertex == vertex_acc.end())
            throw RecoveryFailure("The from vertex doesn't exist!");
          auto to_vertex = vertex_acc.find(delta.edge_create_delete.to_vertex);
          if (to_vertex == vertex_acc.end())
            throw RecoveryFailure("The to vertex doesn't exist!");

          auto edge_gid = delta.edge_create_delete.gid;
          auto edge_type_id = EdgeTypeId::FromUint(
              name_id_mapper_->NameToId(delta.edge_create_delete.edge_type));
          EdgeRef edge_ref(edge_gid);
          if (items_.properties_on_edges) {
            auto edge = edge_acc.find(edge_gid);
            if (edge == edge_acc.end())
              throw RecoveryFailure("The edge doesn't exist!");
            edge_ref = EdgeRef(&*edge);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                edge_type_id, &*to_vertex, edge_ref};
            auto it = std::find(from_vertex->out_edges.begin(),
                                from_vertex->out_edges.end(), link);
            if (it == from_vertex->out_edges.end())
              throw RecoveryFailure("The from vertex doesn't have this edge!");
            std::swap(*it, from_vertex->out_edges.back());
            from_vertex->out_edges.pop_back();
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                edge_type_id, &*from_vertex, edge_ref};
            auto it = std::find(to_vertex->in_edges.begin(),
                                to_vertex->in_edges.end(), link);
            if (it == to_vertex->in_edges.end())
              throw RecoveryFailure("The to vertex doesn't have this edge!");
            std::swap(*it, to_vertex->in_edges.back());
            to_vertex->in_edges.pop_back();
          }
          if (items_.properties_on_edges) {
            if (!edge_acc.remove(edge_gid))
              throw RecoveryFailure("The edge must be removed here!");
          }

          // Decrement edge count.
          edge_count_->fetch_add(-1, std::memory_order_acq_rel);

          break;
        }
        case WalDeltaData::Type::EDGE_SET_PROPERTY: {
          if (!items_.properties_on_edges)
            throw RecoveryFailure(
                "The WAL has properties on edges, but the storage is "
                "configured without properties on edges!");
          auto edge = edge_acc.find(delta.vertex_edge_set_property.gid);
          if (edge == edge_acc.end())
            throw RecoveryFailure("The edge doesn't exist!");
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.vertex_edge_set_property.property));
          auto &property_value = delta.vertex_edge_set_property.value;
          edge->properties.SetProperty(property_id, property_value);
          break;
        }
        case WalDeltaData::Type::TRANSACTION_END:
          break;
        case WalDeltaData::Type::LABEL_INDEX_CREATE: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label.label));
          AddRecoveredIndexConstraint(&indices_constraints->indices.label,
                                      label_id,
                                      "The label index already exists!");
          break;
        }
        case WalDeltaData::Type::LABEL_INDEX_DROP: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label.label));
          RemoveRecoveredIndexConstraint(&indices_constraints->indices.label,
                                         label_id,
                                         "The label index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_property.property));
          AddRecoveredIndexConstraint(
              &indices_constraints->indices.label_property,
              {label_id, property_id},
              "The label property index already exists!");
          break;
        }
        case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_property.property));
          RemoveRecoveredIndexConstraint(
              &indices_constraints->indices.label_property,
              {label_id, property_id},
              "The label property index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_property.property));
          AddRecoveredIndexConstraint(
              &indices_constraints->constraints.existence,
              {label_id, property_id},
              "The existence constraint already exists!");
          break;
        }
        case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_property.property));
          RemoveRecoveredIndexConstraint(
              &indices_constraints->constraints.existence,
              {label_id, property_id},
              "The existence constraint doesn't exist!");
          break;
        }
        case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE: {
          auto label_id = LabelId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_properties.label));
          std::set<PropertyId> property_ids;
          for (const auto &prop : delta.operation_label_properties.properties) {
            property_ids.insert(
                PropertyId::FromUint(name_id_mapper_->NameToId(prop)));
          }
          AddRecoveredIndexConstraint(&indices_constraints->constraints.unique,
                                      {label_id, property_ids},
                                      "The unique constraint already exists!");
          break;
        }
        case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP: {
          auto label_id = LabelId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_properties.label));
          std::set<PropertyId> property_ids;
          for (const auto &prop : delta.operation_label_properties.properties) {
            property_ids.insert(
                PropertyId::FromUint(name_id_mapper_->NameToId(prop)));
          }
          RemoveRecoveredIndexConstraint(
              &indices_constraints->constraints.unique,
              {label_id, property_ids}, "The unique constraint doesn't exist!");
          break;
        }
      }
      ret.next_timestamp = std::max(ret.next_timestamp, timestamp + 1);
      ++deltas_applied;
    } else {
      // This delta should be skipped.
      SkipWalDeltaData(&wal);
    }
  }

  LOG(INFO) << "Applied " << deltas_applied << " deltas from WAL " << path;

  return ret;
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
