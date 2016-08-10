#pragma once

#include <memory>
#include <vector>
#include <cstring>

#include "bolt/v1/config.hpp"
#include "utils/types/byte.hpp"
#include "logging/default.hpp"

namespace bolt
{

template <class Stream>
class ChunkedBuffer
{
    static constexpr size_t C = bolt::config::C; /* chunk size */

public:
    ChunkedBuffer(Stream &stream) : stream(stream) 
    {
        logger = logging::log->logger("Chunked Buffer");
    }

    void write(const byte *values, size_t n)
    {
        // TODO: think about shared pointer
        // TODO: this is naive implementation, it can be implemented much better

        logger.trace("write {} bytes", n);
       
        byte *chunk = chunk = (byte *)std::malloc(n * sizeof(byte));
        last_size = n;

        std::memcpy(chunk, values, n);

        buffer.push_back(chunk);
    }

    void flush()
    {
        logger.trace("Flush");

        for (size_t i = 0; i < buffer.size(); ++i) {
            if (i == buffer.size() - 1)
                stream.get().write(buffer[i], last_size);
            else
                stream.get().write(buffer[i], C);
        }

        destroy();
    }

    ~ChunkedBuffer()
    {
        destroy(); 
    }

private:
    Logger logger;
    std::reference_wrapper<Stream> stream;
    std::vector<byte *> buffer;
    size_t last_size {0}; // last chunk size (it is going to be less than C)

    void destroy()
    {
        for (size_t i = 0; i < buffer.size(); ++i) {
            std::free(buffer[i]);
        }
        buffer.clear();
    }
};

}
