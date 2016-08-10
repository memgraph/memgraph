#pragma once

#include <array>
#include <cstring>
#include <functional>

#include "utils/likely.hpp"

namespace bolt
{

template <class Stream>
class ChunkedEncoder
{
    static constexpr size_t N = 65535;
    static constexpr size_t C = N + 2 /* end mark */;

public:
    using byte = unsigned char;

    ChunkedEncoder(Stream& stream) : stream(stream) {}

    static constexpr size_t chunk_size = N - 2;

    void write(byte value)
    {
        if(UNLIKELY(pos == N))
            end_chunk();

        chunk[pos++] = value;
    }

    void write(const byte* values, size_t n)
    {
        while(n > 0)
        {
            auto size = n < N - pos ? n : N - pos;

            std::memcpy(chunk.data() + pos, values, size);

            pos += size;
            n -= size;

            if(pos == N)
                end_chunk();
        }
    }

    void flush()
    {
        write_chunk_header();

        // write two zeros to signal message end
        chunk[pos++] = 0x00;
        chunk[pos++] = 0x00;

        flush_stream();
    }

private:
    std::reference_wrapper<Stream> stream;

    std::array<byte, C> chunk;
    size_t pos {2};

    void end_chunk()
    {
        write_chunk_header();
        flush();
    }

    void write_chunk_header()
    {
        // write the size of the chunk
        uint16_t size = pos - 2;

        // write the higher byte
        chunk[0] = size >> 8;

        // write the lower byte
        chunk[1] = size & 0xFF;
    }

    void flush_stream()
    {
        // write chunk to the stream
        stream.get().write(chunk.data(), pos);
        pos = 2;
    }
};

}
