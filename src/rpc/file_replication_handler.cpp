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
#include "slk/streams.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/replication/serialization.hpp"
#include "utils/logging.hpp"

namespace memgraph::rpc {

FileReplicationHandler::~FileReplicationHandler() { ResetCurrentFile(); }

std::filesystem::path FileReplicationHandler::GetRandomDir() {
  auto const random_str = utils::GenerateUUID();
  return std::filesystem::temp_directory_path() / "memgraph" / random_str /
         storage::durability::kReplicaDurabilityDirectory;
}

// The assumption is that the header, request, file name and file size will always fit into the buffer size = 64KiB
// Currently, they are taking few hundred bytes at most so this should be a valid assumption. Also, we aren't expecting
// big growth in message size/
std::optional<size_t> FileReplicationHandler::OpenFile(const uint8_t *data, size_t const size) {
  auto const tmp_rnd_dir = GetRandomDir();

  if (!utils::EnsureDir(tmp_rnd_dir)) {
    spdlog::error("Failed to create temporary directory {}", tmp_rnd_dir);
    return std::nullopt;
  }

  slk::Reader req_reader(data, size, size);
  storage::replication::Decoder decoder(&req_reader);

  auto const maybe_filename = decoder.ReadString();
  if (!ValidateFilename(maybe_filename)) return std::nullopt;

  const auto maybe_file_size = decoder.ReadUint();
  if (!ValidateFileSize(maybe_file_size)) return std::nullopt;

  file_size_ = *maybe_file_size;
  auto const path = tmp_rnd_dir / *maybe_filename;
  paths_.emplace_back(path);

  file_.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
  spdlog::info("Replica will be using file {} with size {}", path, file_size_);

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

  if (filename.find('/') != std::string::npos || filename.find('\\') != std::string::npos) {
    spdlog::error("Filename must not contain path separators: {}", filename);
    return false;
  }

  if (filename.find('.') != std::string::npos) {
    spdlog::error("Filename must not contain extension: {}", filename);
    return false;
  }

  if (auto const file_path = std::filesystem::path(filename); file_path.has_parent_path()) {
    spdlog::error("File cannot have a parent path{}", file_path.string());
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
