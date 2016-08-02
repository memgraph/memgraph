#include "bolt_decoder.hpp"

#include <iostream>

#include "utils/bswap.hpp"
#include "logging/default.hpp"

#include "bolt/v1/packing/codes.hpp"

namespace bolt
{

void BoltDecoder::handshake(const byte*& data, size_t len)
{
    buffer.write(data, len);
    data += len;
}

bool BoltDecoder::decode(const byte*& data, size_t len)
{
    return decoder(data, len);
}

bool BoltDecoder::empty() const
{
    return pos == buffer.size();
}

void BoltDecoder::reset()
{
    buffer.clear();
    pos = 0;
}

byte BoltDecoder::peek() const
{
    return buffer[pos];
}

byte BoltDecoder::read_byte()
{
    return buffer[pos++];
}

void BoltDecoder::read_bytes(void* dest, size_t n)
{
    std::memcpy(dest, buffer.data() + pos, n);
    pos += n;
}

template <class T>
T parse(const void* data)
{
    // reinterpret bytes as the target value
    auto value = reinterpret_cast<const T*>(data);

    // swap values to little endian
    return bswap(*value);
}

template <class T>
T parse(Buffer& buffer, size_t& pos)
{
    // get a pointer to the data we're converting
    auto ptr = buffer.data() + pos;

    // skip sizeof bytes that we're going to read
    pos += sizeof(T);

    // read and convert the value
    return parse<T>(ptr);
}

int16_t BoltDecoder::read_int16()
{
    return parse<int16_t>(buffer, pos);
}

uint16_t BoltDecoder::read_uint16()
{
    return parse<uint16_t>(buffer, pos);
}

int32_t BoltDecoder::read_int32()
{
    return parse<int32_t>(buffer, pos);
}

uint32_t BoltDecoder::read_uint32()
{
    return parse<uint32_t>(buffer, pos);
}

int64_t BoltDecoder::read_int64()
{
    return parse<int64_t>(buffer, pos);
}

uint64_t BoltDecoder::read_uint64()
{
    return parse<uint64_t>(buffer, pos);
}

double BoltDecoder::read_float64()
{
    auto v = parse<int64_t>(buffer, pos);
    return *reinterpret_cast<const double *>(&v);
}

std::string BoltDecoder::read_string()
{
    auto marker = read_byte();

    std::string res;
    uint32_t size;

    // if the first 4 bits equal to 1000 (0x8), this is a tiny string
    if((marker & 0xF0) == pack::TinyString)
    {
        // size is stored in the lower 4 bits of the marker byte
        size = marker & 0x0F;
    }
    // if the marker is 0xD0, size is an 8-bit unsigned integer
    if(marker == pack::String8)
    {
        size = read_byte();
    }
    // if the marker is 0xD1, size is a 16-bit big-endian unsigned integer
    else if(marker == pack::String16)
    {
        size = read_uint16();
    }
    // if the marker is 0xD2, size is a 32-bit big-endian unsigned integer
    else if(marker == pack::String32)
    {
        size = read_uint32();
    }
    else
    {
        // TODO error?
        return res;
    }

    if(size == 0)
        return res;

    res.append(reinterpret_cast<const char*>(raw()), size);
    pos += size;

    return res;
}

const byte* BoltDecoder::raw() const
{
    return buffer.data() + pos;
}

}
