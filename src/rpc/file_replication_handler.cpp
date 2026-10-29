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

#include "rpc/file_replication_handler.hpp"

#include <ranges>

#include "flags/general.hpp"
#include "slk/streams.hpp"
#include "utils/logging.hpp"
#include "wire_format/serialization.hpp"

namespace memgraph::rpc {

FileReplicationHandler::~FileReplicationHandler() {
  if (file_.IsOpen()) {
    // The file will be deleted so I don't care about syncing it before closing
    file_.Close();
  }
  // Nobody should be using paths_ after the FileReplicationHandler gets destructed.
  // A snapshot that was loaded has already been moved into the data directory; whatever is still here wasn't used.
  std::ranges::for_each(paths_ | std::views::filter([](auto const &path) {
                          std::error_code ec;
                          return std::filesystem::exists(path, ec);
                        }),
                        [](auto const &path) { utils::DeleteFile(path); }

  );
}

// The assumption is that the header, request, file name and file size will always fit into the buffer size = 64KiB
// Currently, they are taking few hundred bytes at most so this should be a valid assumption. Also, we aren't expecting
// big growth in message size/
std::optional<size_t> FileReplicationHandler::OpenFile(const uint8_t *data, size_t const size) {
  slk::Reader req_reader(data, size, size);

  auto const maybe_rel_path_str = wire_format::ReadString(&req_reader);
  if (!ValidateFilename(maybe_rel_path_str)) return std::nullopt;

  auto const rel_path = std::filesystem::path{*maybe_rel_path_str};
  auto const filename = rel_path.filename();
  auto const file_type = rel_path.parent_path().filename();  // will be wal or snapshots
  auto const gp_path =
      rel_path.parent_path().parent_path();  // grandparent path. Will be either databases/uuid or . for default db

  auto const save_dir = std::filesystem::path{FLAGS_data_directory} / gp_path / "tmp" / file_type;

  if (!utils::EnsureDir(save_dir)) {
    spdlog::error("Failed to create directory {}", save_dir.string());
    return std::nullopt;
  }

  const auto maybe_file_size = wire_format::ReadUint(&req_reader);
  if (!ValidateFileSize(maybe_file_size)) return std::nullopt;

  file_size_ = *maybe_file_size;
  auto const path = save_dir / filename;
  paths_.emplace_back(path);

  spdlog::trace("Replica will be using file {} with size {}", path, file_size_);
  if (!file_.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING)) {
    spdlog::error("Failed to open file {}. This is not a fatal failure, main will retry the sending of the file.",
                  path);
    return std::nullopt;
  }

  // First N bytes are file_name and file_size, therefore we don't read full size
  size_t const processed_bytes = req_reader.GetPos();
  auto const written = WriteToFile(data + processed_bytes, size - processed_bytes);
  if (!written) return std::nullopt;
  return processed_bytes + *written;
}

bool FileReplicationHandler::ValidateFilename(std::optional<std::string> const &maybe_filename) {
  if (!maybe_filename) {
    spdlog::error("Filename missing for the received file over the RPC");
    return false;
  }

  auto const &filename = *maybe_filename;
  if (filename.empty()) {
    spdlog::error("Filename is empty");
    return false;
  }

  if (filename.contains('.')) {
    spdlog::error("Filename must not contain extension: {}", filename);
    return false;
  }

  return true;
}

bool FileReplicationHandler::ValidateFileSize(std::optional<uint64_t> const &maybe_filesize) {
  if (!maybe_filesize) {
    spdlog::error("Failed to read file size");
    return false;
  }
  return true;
}

std::expected<size_t, utils::FileWriteError> FileReplicationHandler::WriteToFile(const uint8_t *data,
                                                                                 size_t const size) {
  if (!file_.IsOpen()) {
    return 0;
  }

  size_t processed_bytes{0};
  auto to_write = std::min(size, file_size_ - written_);

  auto const fail = [this](utils::FileWriteError const error) {
    // The file being written is always the last one opened; the handle may already be closed.
    spdlog::error("Couldn't write received file {}: {}. The transfer is dropped and main will retry it.",
                  paths_.back(),
                  utils::FileWriteErrorToString(error));
    return std::unexpected{error};
  };

  while (to_write > 0) {
    const auto chunk_size = std::min(to_write, utils::kFileBufferSize);
    if (auto res = file_.Write(data + processed_bytes, chunk_size); !res) return fail(res.error());
    to_write -= chunk_size;
    written_ += chunk_size;
    processed_bytes += chunk_size;
  }

  if (written_ == file_size_) {
    if (auto res = ResetCurrentFile(); !res) return fail(res.error());
  }
  return processed_bytes;
}

std::expected<void, utils::FileWriteError> FileReplicationHandler::ResetCurrentFile() {
  if (!file_.IsOpen()) return {};
  auto synced = file_.Sync();
  file_.Close();
  written_ = 0;
  file_size_ = 0;
  return synced;
}

bool FileReplicationHandler::HasOpenedFile() const { return file_.IsOpen(); }

uint64_t FileReplicationHandler::GetRemainingBytesToWrite() const { return file_size_ - written_; }

const std::vector<std::filesystem::path> &FileReplicationHandler::GetActiveFileNames() const { return paths_; }

}  // namespace memgraph::rpc
