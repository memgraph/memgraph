#include "communication/rpc/protocol.hpp"

#include "communication/rpc/messages.hpp"
#include "communication/rpc/server.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "utils/on_scope_exit.hpp"

namespace communication::rpc {

Session::Session(Server *server, const io::network::Endpoint &endpoint,
                 communication::InputStream *input_stream,
                 communication::OutputStream *output_stream)
    : server_(server),
      endpoint_(endpoint),
      input_stream_(input_stream),
      output_stream_(output_stream) {}

void Session::Execute() {
  auto ret =
      slk::CheckStreamComplete(input_stream_->data(), input_stream_->size());
  if (ret.status == slk::StreamStatus::INVALID) {
    throw SessionException("Received an invalid SLK stream!");
  } else if (ret.status == slk::StreamStatus::PARTIAL) {
    input_stream_->Resize(ret.stream_size);
    return;
  }

  // Remove the data from the stream on scope exit.
  utils::OnScopeExit shift_data(
      [&, ret] { input_stream_->Shift(ret.stream_size); });

  // Prepare SLK reader and builder.
  slk::Reader req_reader(input_stream_->data(), input_stream_->size());
  slk::Builder res_builder(
      [&](const uint8_t *data, size_t size, bool have_more) {
        output_stream_->Write(data, size, have_more);
      });

  // Load the request ID.
  uint64_t req_id = 0;
  slk::Load(&req_id, &req_reader);

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
      throw SessionException(
          "Session trying to execute an unregistered RPC call!");
    }
    VLOG(12) << "[RpcServer] received " << extended_it->second.req_type.name;
    slk::Save(extended_it->second.res_type.id, &res_builder);
    extended_it->second.callback(endpoint_, &req_reader, &res_builder);
  } else {
    VLOG(12) << "[RpcServer] received " << it->second.req_type.name;
    slk::Save(it->second.res_type.id, &res_builder);
    it->second.callback(&req_reader, &res_builder);
  }

  // Finalize the SLK streams.
  req_reader.Finalize();
  res_builder.Finalize();

  VLOG(12) << "[RpcServer] sent "
           << (it != server_->callbacks_.end()
                   ? it->second.res_type.name
                   : extended_it->second.res_type.name);
}

}  // namespace communication::rpc
