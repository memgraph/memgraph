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

#pragma once

#include "utils/file.hpp"

namespace memgraph::rpc {
class FileReplicationHandler final {
 public:
  FileReplicationHandler() = default;
  ~FileReplicationHandler();

  FileReplicationHandler(const FileReplicationHandler &) = delete;
  FileReplicationHandler &operator=(const FileReplicationHandler &) = delete;

  FileReplicationHandler(FileReplicationHandler &&) = default;
  FileReplicationHandler &operator=(FileReplicationHandler &&) = default;

  // Returns the number of processed bytes
  size_t OpenFile(const uint8_t *data, size_t size);

  // Returns the number of processed bytes
  size_t WriteToFile(const uint8_t *data, size_t size);

  void ResetCurrentFile();

  bool HasOpenedFile() const;

  uint64_t GetRemainingBytesToWrite() const;

  const std::vector<std::string> &GetActiveFileNames() const;

 private:
  utils::OutputFile file_;
  uint64_t file_size_;
  uint64_t written_;
  // Files part of the current request
  std::vector<std::string> file_names_;
};
}  // namespace memgraph::rpc
