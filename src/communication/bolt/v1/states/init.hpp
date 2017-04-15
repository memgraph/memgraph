#pragma once

#include "communication/bolt/v1/codes.hpp"
#include "communication/bolt/v1/encoder/result_stream.hpp"
#include "communication/bolt/v1/state.hpp"
#include "logging/default.hpp"
#include "utils/likely.hpp"

namespace communication::bolt {

/**
 * Init state run function
 * This function runs everything to initialize a Bolt session with the client.
 * @param session the session that should be used for the run
 */
template <typename Session>
State StateInitRun(Session &session) {
  static Logger logger = logging::log->logger("State INIT");
  logger.debug("Parsing message");

  Marker marker;
  Signature signature;
  if (!session.decoder_.ReadMessageHeader(&signature, &marker)) {
    logger.debug("Missing header data!");
    return State::Close;
  }

  if (UNLIKELY(signature != Signature::Init)) {
    logger.debug("Expected Init signature, but received 0x{:02X}!",
                 underlying_cast(signature));
    return State::Close;
  }
  if (UNLIKELY(marker != Marker::TinyStruct2)) {
    logger.debug("Expected TinyStruct2 marker, but received 0x{:02X}!",
                 underlying_cast(marker));
    return State::Close;
  }

  query::TypedValue client_name;
  if (!session.decoder_.ReadTypedValue(&client_name,
                                       query::TypedValue::Type::String)) {
    logger.debug("Couldn't read client name!");
    return State::Close;
  }

  query::TypedValue metadata;
  if (!session.decoder_.ReadTypedValue(&metadata,
                                       query::TypedValue::Type::Map)) {
    logger.debug("Couldn't read metadata!");
    return State::Close;
  }

  logger.debug("Client connected '{}'", client_name.Value<std::string>());

  if (!session.encoder_.MessageSuccess()) {
    logger.debug("Couldn't send success message to the client!");
    return State::Close;
  }

  return State::Executor;
}
}
