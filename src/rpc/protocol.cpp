// Copyright 2024 Memgraph Ltd.
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
  } else if (ret.status == slk::StreamStatus::PARTIAL) {
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
  auto it = server_->callbacks_.find(req_id);
  auto extended_it = server_->extended_callbacks_.end();
  if (it == server_->callbacks_.end()) {
    // We couldn't find a regular callback to call, try to find an extended
    // callback to call.
    extended_it = server_->extended_callbacks_.find(req_id);

    if (extended_it == server_->extended_callbacks_.end()) {
      // Throw exception to close the socket and cleanup the session.
      throw SessionException("Session trying to execute an unregistered RPC call!");
    }
    SPDLOG_TRACE("[RpcServer] received {}", extended_it->second.req_type.name);
    slk::Save(extended_it->second.res_type.id, &res_builder);
    slk::Save(rpc::current_version, &res_builder);
    try {
      extended_it->second.callback(endpoint_, &req_reader, &res_builder);
    } catch (const slk::SlkReaderException &) {
      throw rpc::SlkRpcFailedException();
    }
  } else {
    SPDLOG_TRACE("[RpcServer] received {}", it->second.req_type.name);
    slk::Save(it->second.res_type.id, &res_builder);
    slk::Save(rpc::current_version, &res_builder);
    try {
      it->second.callback(&req_reader, &res_builder);
    } catch (const slk::SlkReaderException &) {
      throw rpc::SlkRpcFailedException();
    }
  }

  // Finalize the SLK streams.
  req_reader.Finalize();
  res_builder.Finalize();

  SPDLOG_TRACE("[RpcServer] sent {}",
               (it != server_->callbacks_.end() ? it->second.res_type.name : extended_it->second.res_type.name));
}

}  // namespace memgraph::rpc
