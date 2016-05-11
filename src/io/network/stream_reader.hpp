#pragma once

#include "event_listener.hpp"
#include "memory/literals.hpp"

namespace io
{
using namespace memory::literals;

template <class Derived, class Stream>
class StreamReader : public EventListener<Derived, Stream>
{
public:
    struct Buffer
    {
        char* ptr;
        size_t len;
    };

    StreamReader(uint32_t flags = 0) : EventListener<Derived, Stream>(flags) {}

    void on_data(Stream& stream)
    {
        while(true)
        {
            // allocate the buffer to fill the data
            auto buf = this->derived().on_alloc(stream);

            // read from the buffer at most buf.len bytes
            buf.len = stream.socket.read(buf.ptr, buf.len);

            // check for read errors
            if(buf.len == -1)
            {
                // this means we have read all available data
                if(LIKELY(errno == EAGAIN))
                {
                    LOG_DEBUG("EAGAIN read all data on socket " << stream.id());
                    break;
                }

                // some other error occurred, check errno
                this->derived().on_error(stream);
            }

            // end of file, the client has closed the connection
            if(UNLIKELY(buf.len == 0))
            {
                LOG_DEBUG("EOF stream closed on socket " << stream.id());
                stream.close();
                break;
            }

            LOG_DEBUG("data on socket " << stream.id());
            this->derived().on_read(stream, buf);
        }
    }
};

}
