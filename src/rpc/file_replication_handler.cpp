// Copyright 2025 Memgraph Ltd.
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

#include "flags/general.hpp"
#include "slk/streams.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "utils/logging.hpp"

namespace memgraph::rpc {

FileReplicationHandler::~FileReplicationHandler() { ResetCurrentFile(); }

// The assumption is that the header, request, file name and file size will always fit into the buffer size = 64KiB
// Currently, they are taking few hundred bytes at most so this should be a valid assumption. Also, we aren't expecting
// big growth in message size/
std::optional<size_t> FileReplicationHandler::OpenFile(const uint8_t *data, size_t const size) {
  slk::Reader req_reader(data, size, size);
  storage::replication::Decoder decoder(&req_reader);

  auto const maybe_rel_path_str = decoder.ReadString();
  if (!ValidateFilename(maybe_rel_path_str)) return std::nullopt;

  auto const rel_path = std::filesystem::path{*maybe_rel_path_str};
  auto const filename = rel_path.filename();
  auto const file_type = rel_path.parent_path().filename();  // will be wal or snapshots
  auto const gp_path =
      rel_path.parent_path().parent_path();  // grandparent path. Will be either databases/uuid or . for default db

  auto const save_dir = std::filesystem::path{FLAGS_data_directory} / gp_path / "tmp" / file_type;

  // We are cleaning WAL files in dbms/replication_handlers.cpp also but this is additional attempt so that durability
  // files don't pile up
  if (std::filesystem::exists(save_dir)) {
    std::filesystem::remove_all(save_dir);
  }

  if (std::error_code error_code; !std::filesystem::create_directories(save_dir, error_code)) {
    spdlog::error("Failed to create dir {} for saving received durability files", save_dir);
    return std::nullopt;
  }

  if (!utils::EnsureDir(save_dir)) {
    return std::nullopt;
  }

  const auto maybe_file_size = decoder.ReadUint();
  if (!ValidateFileSize(maybe_file_size)) return std::nullopt;

  file_size_ = *maybe_file_size;
  auto const path = save_dir / filename;
  paths_.emplace_back(path);

  spdlog::info("Replica will be using file {} with size {}", path, file_size_);
  file_.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);

  // First N bytes are file_name and file_size, therefore we don't read full size
  size_t const processed_bytes = req_reader.GetPos();
  return processed_bytes + WriteToFile(data + processed_bytes, size - processed_bytes);
}

bool FileReplicationHandler::ValidateFilename(std::optional<std::string> const &maybe_filename) {
  if (!maybe_filename.has_value()) {
    spdlog::error("Filename missing for the received file over the RPC");
    return false;
  }

  auto const &filename = *maybe_filename;
  if (filename.empty()) {
    spdlog::error("Filename is empty");
    return false;
  }

  if (filename.find('.') != std::string::npos) {
    spdlog::error("Filename must not contain extension: {}", filename);
    return false;
  }

  return true;
}

bool FileReplicationHandler::ValidateFileSize(std::optional<uint64_t> const &maybe_filesize) {
  if (!maybe_filesize.has_value()) {
    spdlog::error("Failed to read file size");
    return false;
  }
  return true;
}

size_t FileReplicationHandler::WriteToFile(const uint8_t *data, size_t const size) {
  if (!file_.IsOpen()) {
    return 0;
  }

  size_t processed_bytes{0};
  auto to_write = std::min(size, file_size_ - written_);

  while (to_write > 0) {
    const auto chunk_size = std::min(to_write, utils::kFileBufferSize);
    file_.Write(data + processed_bytes, chunk_size);
    to_write -= chunk_size;
    written_ += chunk_size;
    processed_bytes += chunk_size;
  }

  if (written_ == file_size_) {
    ResetCurrentFile();
  }
  return processed_bytes;
}

void FileReplicationHandler::ResetCurrentFile() {
  if (file_.IsOpen()) {
    file_.Sync();
    file_.Close();
    written_ = 0;
    file_size_ = 0;
  }
}

bool FileReplicationHandler::HasOpenedFile() const { return file_.IsOpen(); }

uint64_t FileReplicationHandler::GetRemainingBytesToWrite() const { return file_size_ - written_; }

const std::vector<std::filesystem::path> &FileReplicationHandler::GetActiveFileNames() const { return paths_; }

}  // namespace memgraph::rpc
