#pragma once

#include "bolt/v1/transport/buffer.hpp"
#include "bolt/v1/transport/chunked_decoder.hpp"
#include "utils/types/byte.hpp"

namespace bolt
{

class BoltDecoder
{
public:
    void handshake(const byte *&data, size_t len);
    bool decode(const byte *&data, size_t len);

    bool empty() const;
    void reset();

    byte peek() const;
    byte read_byte();
    void read_bytes(void *dest, size_t n);

    int16_t read_int16();
    uint16_t read_uint16();

    int32_t read_int32();
    uint32_t read_uint32();

    int64_t read_int64();
    uint64_t read_uint64();

    double read_float64();

    std::string read_string();

private:
    Buffer buffer;
    ChunkedDecoder<Buffer> decoder{buffer};
    size_t pos{0};

    const byte *raw() const;
};
}
