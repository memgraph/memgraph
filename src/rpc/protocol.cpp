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

#include "rpc/protocol.hpp"

#include <utility>

#include "rpc/exceptions.hpp"
#include "rpc/server.hpp"
#include "rpc/version.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/readable_size.hpp"
#include "utils/stat.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::rpc {

constexpr auto kBufferRetainLimit = 4 * 1024 * 1024;  // 4MiB

RpcMessageDeliverer::RpcMessageDeliverer(Server *server, io::network::Endpoint const & /*endpoint*/,
                                         communication::InputStream *input_stream,
                                         communication::OutputStream *output_stream)
    : server_(server), input_stream_(input_stream), output_stream_(output_stream) {}

void RpcMessageDeliverer::Execute() {
  // NOLINTNEXTLINE
  spdlog::trace("Memory at the start of execute: {}", utils::GetReadableSize(utils::GetMemoryRES()));

  auto const remaining_file_size = std::invoke([&]() -> std::optional<uint64_t> {
    if (!file_replication_handler_.has_value() || !file_replication_handler_->file_.IsOpen()) {
      return std::nullopt;
    }
    // If we are here it means that the file is open, hence this check is valid
    return file_replication_handler_->file_size_ - file_replication_handler_->written_;
  });

  auto ret = slk::CheckStreamStatus(input_stream_->data(), input_stream_->size(), remaining_file_size);

  if (ret.status == slk::StreamStatus::INVALID) {
    throw SessionException("Received an invalid SLK stream!");
  }
  // We resize the stream if the initial header+request cannot fit into the input stream or if we couldn't read
  // 0xFFFF/0x0000 segment
  if (ret.status == slk::StreamStatus::PARTIAL) {
    input_stream_->Resize(ret.stream_size);
    return;
  }

  // Remove the data from the stream on scope exit.
  auto const shift_data = utils::OnScopeExit{[&, ret] {
    input_stream_->Shift(ret.stream_size);
    input_stream_->ShrinkBuffer(kBufferRetainLimit);
  }};

  if (ret.status == slk::StreamStatus::NEW_FILE) {
    if (!file_replication_handler_.has_value()) {
      // Will be used at the end to construct slk::Reader. Contains message header and request. It is correct to do this
      // only when initializing FileReplicationHandler
      header_request_ = std::vector<uint8_t>{
          input_stream_->data(), input_stream_->data() + (input_stream_->size() - ret.pos - sizeof(slk::SegmentSize))};
      file_replication_handler_.emplace();
    } else {
      // If file replication handler is already active, it means that we should first finalize prior file
      file_replication_handler_->WriteToFile(input_stream_->data(), ret.pos - sizeof(slk::SegmentSize));
    }
    file_replication_handler_->OpenFile(input_stream_->data() + ret.pos, input_stream_->size() - ret.pos);
    return;
  }

  if (ret.status == slk::StreamStatus::FILE_DATA) {
    // When FILE_DATA status is received, we know that we should read from the stream start
    // Continue writing to the file
    file_replication_handler_->WriteToFile(input_stream_->data(), input_stream_->size());
    return;
  }

  // Writing last segment
  if (file_replication_handler_.has_value()) {
    file_replication_handler_->WriteToFile(input_stream_->data(), input_stream_->size());
    MG_ASSERT(!file_replication_handler_->file_.IsOpen(), "File should be closed after completing the stream");
  }

  // Prepare SLK reader and builder.
  slk::Reader req_reader = std::invoke([&]() {
    // File data wasn't received
    if (header_request_.empty()) {
      return slk::Reader{input_stream_->data(), input_stream_->size()};
    }
    // File data received
    return slk::Reader{header_request_.data(), header_request_.size()};
  });

  slk::Builder res_builder([&](const uint8_t *data, size_t const size, bool const have_more) {
    output_stream_->Write(data, size, have_more);
  });

  // Load the request ID.
  auto const maybe_message_header = std::invoke([&req_reader]() -> std::optional<ProtocolMessageHeader> {
    try {
      // Propagate UnsupportedRpcVersion Exception
      return LoadMessageHeader(&req_reader);
    } catch (const std::exception &e) {
      spdlog::error("Error occurred while loading message header: {}", e.what());
      return std::nullopt;
    }
  });

  if (!maybe_message_header.has_value()) {
    throw SlkRpcFailedException();
  }

  // Access to `callbacks_` and `extended_callbacks_` is done here without
  // acquiring the `mutex_` because we don't allow RPC registration after the
  // server was started so those two maps will never be updated when we `find`
  // over them.
  auto const it = server_->callbacks_.find(maybe_message_header->message_id);
  if (it == server_->callbacks_.end()) {
    throw SessionException("Session trying to execute an unregistered RPC call!. Request id: {}",
                           static_cast<uint64_t>(maybe_message_header->message_id));
  }

  spdlog::trace("[RpcServer] received {}, version {}", it->second.req_type.name, maybe_message_header->message_version);
  // NOLINTNEXTLINE
  spdlog::trace("Memory when RPC {} received: {}", it->second.req_type.name,
                // NOLINTNEXTLINE
                utils::GetReadableSize(utils::GetMemoryRES()));

  auto const on_exit = utils::OnScopeExit{[&] {
    file_replication_handler_.reset();
    header_request_.clear();
  }};

  try {
    it->second.callback(file_replication_handler_, maybe_message_header->message_version, &req_reader, &res_builder);
    // Finalize the SLK stream.
    req_reader.Finalize();
  }
  // NOLINTNEXTLINE
  catch (const slk::SlkReaderLeftoverDataException &) {
    // Skip, it may fail because not all data has been read, that's fine.
  } catch (const std::exception &e) {
    spdlog::error("Error occurred in the callback: {}", e.what());
    throw SlkRpcFailedException();
  }
  // Finally block to clean resources?
}

}  // namespace memgraph::rpc
