#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/encoder/result_stream.hpp"
#include "communication/bolt/v1/state.hpp"
#include "utils/likely.hpp"

namespace communication::bolt {

/**
 * Init state run function
 * This function runs everything to initialize a Bolt session with the client.
 * @param session the session that should be used for the run
 */
template <typename Session>
State StateInitRun(Session &session) {
  DLOG(INFO) << "Parsing message";

  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    DLOG(WARNING) << "Missing header data!";
    return State::Close;
  }

  if (UNLIKELY(signature != Signature::Init)) {
    DLOG(WARNING) << fmt::format(
        "Expected Init signature, but received 0x{:02X}!",
        underlying_cast(signature));
    return State::Close;
  }
  if (UNLIKELY(marker != Marker::TinyStruct2)) {
    DLOG(WARNING) << fmt::format(
        "Expected TinyStruct2 marker, but received 0x{:02X}!",
        underlying_cast(marker));
    DLOG(WARNING) << "The client sent malformed data, but we are continuing "
                     "because the official Neo4j Java driver sends malformed "
                     "data. D'oh!";
    // TODO: this should be uncommented when the Neo4j Java driver is fixed
    // return State::Close;
  }

  query::TypedValue client_name;
  if (!session.decoder_.ReadTypedValue(&client_name,
                                       query::TypedValue::Type::String)) {
    DLOG(WARNING) << "Couldn't read client name!";
    return State::Close;
  }

  query::TypedValue metadata;
  if (!session.decoder_.ReadTypedValue(&metadata,
                                       query::TypedValue::Type::Map)) {
    DLOG(WARNING) << "Couldn't read metadata!";
    return State::Close;
  }

  LOG(INFO) << fmt::format("Client connected '{}'",
                           client_name.Value<std::string>())
            << std::endl;

  if (!session.encoder_.MessageSuccess()) {
    DLOG(WARNING) << "Couldn't send success message to the client!";
    return State::Close;
  }

  return State::Idle;
}
}
