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
#include "utils/typeinfo.hpp"

namespace memgraph::rpc {

constexpr auto kBufferRetainLimit = 4 * 1024 * 1024;  // 4MiB

Session::Session(Server *server, io::network::Endpoint endpoint, communication::InputStream *input_stream,
                 communication::OutputStream *output_stream)
    : server_(server), endpoint_(std::move(endpoint)), input_stream_(input_stream), output_stream_(output_stream) {}

void Session::Execute() {
  auto ret = slk::CheckStreamComplete(input_stream_->data(), input_stream_->size());
  if (ret.status == slk::StreamStatus::INVALID) {
    throw SessionException("Received an invalid SLK stream!");
  }
  if (ret.status == slk::StreamStatus::PARTIAL) {
    input_stream_->Resize(ret.stream_size);
    return;
  }

  // Remove the data from the stream on scope exit.
  auto const shift_data = utils::OnScopeExit{[&, ret] {
    input_stream_->Shift(ret.stream_size);
    input_stream_->ShrinkBuffer(kBufferRetainLimit);
  }};

  // Prepare SLK reader and builder.
  slk::Reader req_reader(input_stream_->data(), input_stream_->size());
  slk::Builder res_builder(
      [&](const uint8_t *data, size_t size, bool have_more) { output_stream_->Write(data, size, have_more); });

  // Load the request ID.
  utils::TypeId req_id{utils::TypeId::UNKNOWN};
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  rpc::Version version;
  try {
    slk::Load(&req_id, &req_reader);
    slk::Load(&version, &req_reader);
  } catch (const slk::SlkReaderException &) {
    throw rpc::SlkRpcFailedException();
  }

  if (version != rpc::current_version) {
    // V1 we introduced versioning with, absolutely no backwards compatibility,
    // because it's impossible to provide backwards compatibility with pre versioning.
    // Future versions this may require mechanism for graceful version handling.
    throw SessionException("Session trying to execute a RPC call of an incorrect version!");
  }

  // Access to `callbacks_` and `extended_callbacks_` is done here without
  // acquiring the `mutex_` because we don't allow RPC registration after the
  // server was started so those two maps will never be updated when we `find`
  // over them.
  auto const it = server_->callbacks_.find(req_id);
  if (it == server_->callbacks_.end()) {
    throw SessionException("Session trying to execute an unregistered RPC call!");
  }

  spdlog::trace("[RpcServer] received {}", it->second.req_type.name);
  try {
    it->second.callback(&req_reader, &res_builder);
    // Finalize the SLK stream. It may fail because not all data has been read, that's fine.
    req_reader.Finalize();
  } catch (const slk::SlkReaderException &e) {
    spdlog::error("Error occurred in the callback: {}", e.what());
    throw rpc::SlkRpcFailedException();
  } catch (const slk::SlkReaderLeftoverDataException &) {
  }
}

}  // namespace memgraph::rpc
