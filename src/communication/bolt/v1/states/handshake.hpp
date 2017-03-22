#pragma once

#include "dbms/dbms.hpp"
#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/transport/bolt_decoder.hpp"

#include "logging/default.hpp"

namespace communication::bolt {

static constexpr uint32_t preamble = 0x6060B017;

static constexpr byte protocol[4] = {0x00, 0x00, 0x00, 0x01};

template<typename Socket>
State state_handshake_run(BoltDecoder &decoder, Socket &socket_, bool *connected) {
  Logger logger = logging::log->logger("State HANDSHAKE");
  logger.debug("run");

  if (UNLIKELY(decoder.read_uint32() != preamble)) return NULLSTATE;

  // TODO so far we only support version 1 of the protocol so it doesn't
  // make sense to check which version the client prefers
  // this will change in the future

  *connected = true;
  // TODO: check for success
  socket_.Write(protocol, sizeof protocol);

  return INIT;
}
}
