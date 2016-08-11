#pragma once

#include <atomic>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>

#include "communication/bolt/v1/bolt.hpp"
#include "communication/bolt/v1/session.hpp"
#include "logging/default.hpp"
#include "io/network/stream_reader.hpp"

namespace bolt
{

template <class Worker>
class Server;

class Worker : public io::StreamReader<Worker, Session>
{
    friend class bolt::Server<Worker>;

public:
    using sptr = std::shared_ptr<Worker>;

    Worker(Bolt &bolt) : bolt(bolt)
    {
        logger = logging::log->logger("Network");
    }

    Session &on_connect(io::Socket &&socket)
    {
        logger.trace("Accepting connection on socket {}", socket.id());

        return *bolt.get().create_session(std::forward<io::Socket>(socket));
    }

    void on_error(Session &)
    {
        logger.trace("[on_error] errno = {}", errno);

#ifndef NDEBUG
        auto err = io::NetworkError("");
        logger.debug("{}", err.what());
#endif

        logger.error("Error occured in this session");
    }

    void on_wait_timeout() {}

    Buffer on_alloc(Session &)
    {
        /* logger.trace("[on_alloc] Allocating {}B", sizeof buf); */

        return Buffer{buf, sizeof buf};
    }

    void on_read(Session &session, Buffer &buf)
    {
        logger.trace("[on_read] Received {}B", buf.len);

#ifndef NDEBUG
        std::stringstream stream;

        for (size_t i = 0; i < buf.len; ++i)
            stream << fmt::format("{:02X} ", static_cast<byte>(buf.ptr[i]));

        logger.trace("[on_read] {}", stream.str());
#endif

        try {
            session.execute(reinterpret_cast<const byte *>(buf.ptr), buf.len);
        } catch (const std::exception &e) {
            logger.error("Error occured while executing statement.");
            logger.error("{}", e.what());
        }
    }

    void on_close(Session &session)
    {
        logger.trace("[on_close] Client closed the connection");
        session.close();
    }

    char buf[65536];

protected:
    std::reference_wrapper<Bolt> bolt;

    Logger logger;
    std::thread thread;

    void start(std::atomic<bool> &alive)
    {
        thread = std::thread([&, this]() {
            while (alive)
                wait_and_process_events();
        });
    }
};
}
