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
#include "storage/v2/durability/paths.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/stat.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::rpc {

constexpr auto kBufferRetainLimit = 4 * 1024 * 1024;  // 4MiB

RpcMessageDeliverer::RpcMessageDeliverer(Server *server, io::network::Endpoint const & /*endpoint*/,
                                         communication::InputStream *input_stream,
                                         communication::OutputStream *output_stream)
    : server_(server), input_stream_(input_stream), output_stream_(output_stream) {}

auto RpcMessageDeliverer::GetRemainingFileSize() const -> std::optional<uint64_t> {
  if (!file_replication_handler_.has_value() || !file_replication_handler_->HasOpenedFile()) {
    return std::nullopt;
  }
  // If we are here it means that the file is open, hence this check is valid
  return file_replication_handler_->GetRemainingBytesToWrite();
}

auto RpcMessageDeliverer::GetReqReader() const -> slk::Reader {
  // File data wasn't received
  if (header_request_.empty()) {
    return slk::Reader{input_stream_->data(), input_stream_->size()};
  }
  // File data received
  return slk::Reader{header_request_.data(), header_request_.size()};
}

void RpcMessageDeliverer::Execute() {
  // Consumed bytes from the input stream
  size_t consumed_bytes{0};

  // While loop is only necessary because of NEW_FILE status. It is possible that the whole file is within one segment
  // and in that case we need to check whether footer or another file follows
  while (true) {
    auto const remaining_file_size = GetRemainingFileSize();

    slk::StreamInfo const ret =
        slk::CheckStreamStatus(input_stream_->data() + consumed_bytes, input_stream_->size() - consumed_bytes,
                               remaining_file_size, consumed_bytes);

    if (ret.status == slk::StreamStatus::INVALID) {
      input_stream_->Clear();
      throw SessionException("Received an invalid SLK stream!");
    }
    // We resize the stream if the initial header+request cannot fit into the input stream or if we couldn't read
    // 0xFFFF/0x0000 segment
    if (ret.status == slk::StreamStatus::PARTIAL) {
      input_stream_->Resize(ret.stream_size);
      return;
    }

    if (ret.status == slk::StreamStatus::FILE_DATA) {
      // When FILE_DATA status is received, we know that we should read from the stream start and that we consumed
      // whole input stream so we can clear it
      file_replication_handler_->WriteToFile(input_stream_->data(), input_stream_->size());
      input_stream_->Clear();
      return;
    }

    if (ret.status == slk::StreamStatus::NEW_FILE) {
      spdlog::info("Stream size when new file is received: {}", input_stream_->size());
      if (!file_replication_handler_.has_value()) {
        // Will be used at the end to construct slk::Reader. Contains message header and request. It is necessary to do
        // this only when initializing FileReplicationHandler
        header_request_ =
            std::vector<uint8_t>{input_stream_->data(),
                                 input_stream_->data() + (input_stream_->size() - ret.pos - sizeof(slk::SegmentSize))};
        file_replication_handler_.emplace();
      } else {
        // If file replication handler is already active, and we received NEW_FILE status it means we should create new
        // file but before that, we should first finalize writing to the prior file
        file_replication_handler_->WriteToFile(input_stream_->data(), ret.pos - sizeof(slk::SegmentSize));
      }
      // We processed them either when processing header and request of the 1st file or also if writing part of the old
      // file
      consumed_bytes += ret.pos;
      // In OpenFile, we process file name, file size and file data contained in this segment
      auto const res = file_replication_handler_->OpenFile(input_stream_->data() + consumed_bytes,
                                                           input_stream_->size() - consumed_bytes);
      if (!res.has_value()) {
        throw SessionException("Error happened while opening file in RpcMessageDeliverer!");
      }

      consumed_bytes += *res;

      // If we consumed all bytes, clear the buffer
      if (consumed_bytes == input_stream_->size()) {
        input_stream_->Clear();
        input_stream_->ShrinkBuffer(kBufferRetainLimit);
        return;
      }
      continue;
    }

    // Status is COMPLETE
    break;
  }

  // Remove the data from the stream on scope exit.
  auto const shift_data = utils::OnScopeExit{[&] {
    input_stream_->Clear();
    input_stream_->ShrinkBuffer(kBufferRetainLimit);
  }};

  // Writing last segment
  if (file_replication_handler_.has_value()) {
    file_replication_handler_->WriteToFile(input_stream_->data(), input_stream_->size());
    MG_ASSERT(!file_replication_handler_->HasOpenedFile(), "File should be closed after completing the stream");
  }

  slk::Reader req_reader = GetReqReader();
  slk::Builder res_builder([&](const uint8_t *data, size_t const size, bool const have_more) {
    output_stream_->Write(data, size, have_more);
  });

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

  // FileReplicationHandler is per-request object
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
}

}  // namespace memgraph::rpc
