#pragma once

#include "io/network/client.hpp"

template <class Derived, class Stream>
class SimpleClient : public io::Client<Derived, Stream>
{
    char buf[65535];

public:
    using Buffer = typename io::StreamReader<Derived, Stream>::Buffer;

    void on_wait_timeout() {}

    void on_error(Stream&)
    {
        std::abort();
    }

    Buffer on_alloc(Stream&)
    {
        return Buffer { buf, sizeof buf };
    }

    void on_close(Stream&) {}
};
