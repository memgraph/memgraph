#pragma once

#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"

#include "dbms/dbms.hpp"
#include "query/engine.hpp"

#include "communication/bolt/v1/state.hpp"
#include "communication/bolt/v1/states/handshake.hpp"
#include "communication/bolt/v1/states/init.hpp"
#include "communication/bolt/v1/states/executor.hpp"
#include "communication/bolt/v1/states/error.hpp"

#include "communication/bolt/v1/serialization/record_stream.hpp"
#include "communication/bolt/v1/transport/bolt_decoder.hpp"
#include "communication/bolt/v1/transport/bolt_encoder.hpp"

#include "logging/default.hpp"

namespace bolt {

template<typename Socket>
class Session {
 public:
  using Decoder = BoltDecoder;
  using OutputStream = RecordStream<Socket>;

  Session(Socket &&socket, Dbms &dbms, QueryEngine<OutputStream> &query_engine)
      : socket(std::move(socket)),
        dbms(dbms), query_engine(query_engine),
        logger(logging::log->logger("Session")) {
    event.data.ptr = this;
    // start with a handshake state
    state = HANDSHAKE;
  }

  bool alive() const { return state != NULLSTATE; }

  int id() const { return socket.id(); }

  void execute(const byte *data, size_t len) {
    // mark the end of the message
    auto end = data + len;

    while (true) {
      auto size = end - data;

      if (LIKELY(connected)) {
        logger.debug("Decoding chunk of size {}", size);
        auto finished = decoder.decode(data, size);

        if (!finished) return;
      } else {
        logger.debug("Decoding handshake of size {}", size);
        decoder.handshake(data, size);
      }

      switch(state) {
        case HANDSHAKE:
          logger.debug("Current state: DEBUG");
          state = state_handshake_run<Socket>(decoder, this->socket, &connected);
          break;
        case INIT:
          logger.debug("Current state: INIT");
          state = state_init_run<Socket>(output_stream, decoder);
          break;
        case EXECUTOR:
          logger.debug("Current state: EXECUTOR");
          state = state_executor_run<Socket>(output_stream, decoder, dbms, query_engine);
          break;
        case ERROR:
          logger.debug("Current state: ERROR");
          state = state_error_run<Socket>(output_stream, decoder);
          break;
        case NULLSTATE:
          break;
      }

      decoder.reset();
    }
  }

  void close() {
    logger.debug("Closing session");
    this->socket.Close();
  }

  Socket socket;
  io::network::Epoll::Event event;

  Dbms &dbms;
  QueryEngine<OutputStream> &query_engine;

  GraphDbAccessor active_db() { return dbms.active(); }

  Decoder decoder;
  OutputStream output_stream{socket};

  bool connected{false};
  State state;

 protected:
  Logger logger;
};
}
