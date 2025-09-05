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

// TODO: (andi) Add abstract method for resetting file replication handler
FileReplicationHandler::~FileReplicationHandler() {
  if (file_.IsOpen()) {
    ResetCurrentFile();
  }
}

// TODO: (andi) Theoretically you could be unable to read string and int from the initial data
size_t FileReplicationHandler::OpenFile(const uint8_t *data, size_t const size) {
  const auto kTempDirectory =
      std::filesystem::temp_directory_path() / "memgraph" / storage::durability::kReplicaDurabilityDirectory;

  slk::Reader req_reader(data, size, size);
  // spdlog::warn("Reader pos before creating decoder: {}", req_reader.GetPos());
  storage::replication::Decoder decoder(&req_reader);
  // spdlog::warn("Reader pos after creating decoder: {}", req_reader.GetPos());

  // We should be able to always read filename and filesize since file data starts with the new segment
  auto const maybe_filename = decoder.ReadString();
  MG_ASSERT(maybe_filename, "Filename missing for the received file over the RPC");
  file_names_.emplace_back(*maybe_filename);
  auto const path = kTempDirectory / *maybe_filename;

  // TODO: (andi) Handle error, don't crash if dir cannot be created
  utils::EnsureDirOrDie(kTempDirectory);

  spdlog::warn("Path {} will be used", path);

  file_.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);

  const auto maybe_file_size = decoder.ReadUint();
  MG_ASSERT(maybe_file_size, "File size missing");
  file_size_ = *maybe_file_size;

  spdlog::warn("File size is: {}", file_size_);

  // Because first N bytes are file_name and file_size, therefore we don't read full size
  size_t const processed_bytes = req_reader.GetPos();
  return processed_bytes + WriteToFile(data + processed_bytes, size - processed_bytes);
}

size_t FileReplicationHandler::WriteToFile(const uint8_t *data, size_t const size) {
  spdlog::warn("Got the request to write to file. Stream size is {}", size);
  if (!file_.IsOpen()) {
    spdlog::warn("Cannot write to file since it's closed");
    return 0;
  }

  size_t processed_bytes{0};

  auto to_write = std::min(size, file_size_ - written_);
  while (to_write > 0) {
    const auto chunk_size = std::min(to_write, utils::kFileBufferSize);
    file_.Write(data, chunk_size);
    to_write -= chunk_size;
    written_ += chunk_size;
    processed_bytes += chunk_size;
    spdlog::warn("Written {} bytes to file in the method, remaining {}", chunk_size, file_size_ - written_);
  }

  if (written_ == file_size_ && file_.IsOpen()) {
    spdlog::trace("Closing file: {}", file_.path());
    ResetCurrentFile();
  }
  return processed_bytes;
}

void FileReplicationHandler::ResetCurrentFile() {
  file_.Close();
  written_ = 0;
  file_size_ = 0;
}

}  // namespace memgraph::rpc
