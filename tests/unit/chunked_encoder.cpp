#include <cassert>
#include <deque>
#include <iostream>
#include <vector>

#include "gtest/gtest.h"

#include "communication/bolt/v1/transport/chunked_encoder.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

using byte = unsigned char;

void print_hex(byte x) { printf("%02X ", static_cast<byte>(x)); }

class DummyStream
{
public:
    void write(const byte *values, size_t n)
    {
        num_calls++;
        data.insert(data.end(), values, values + n);
    }

    byte pop()
    {
        auto c = data.front();
        data.pop_front();
        return c;
    }

    size_t pop_size() { return ((size_t)pop() << 8) | pop(); }

    void print()
    {
        for (size_t i = 0; i < data.size(); ++i)
            print_hex(data[i]);
    }

    std::deque<byte> data;
    size_t num_calls{0};
};

using Encoder = bolt::ChunkedEncoder<DummyStream>;

void write_ff(Encoder &encoder, size_t n)
{
    std::vector<byte> v;

    for (size_t i = 0; i < n; ++i)
        v.push_back('\xFF');

    encoder.write(v.data(), v.size());
}

void check_ff(DummyStream &stream, size_t n)
{
    for (size_t i = 0; i < n; ++i)
        ASSERT_EQ(stream.pop(), byte('\xFF'));

    (void)stream;
}

using encoder_t = bolt::ChunkedEncoder<DummyStream>;

TEST(ChunkedEncoderTest, Encode)
{
    DummyStream stream;
    encoder_t encoder(stream);
    size_t chunk_size = encoder_t::chunk_size;

    write_ff(encoder, 10);
    write_ff(encoder, 10);
    encoder.write_chunk();

    write_ff(encoder, 10);
    write_ff(encoder, 10);
    encoder.write_chunk();

    // this should be two chunks, one of size 65533 and the other of size 1467
    write_ff(encoder, 67000);
    encoder.write_chunk();

    for (int i = 0; i < 10000; ++i)
        write_ff(encoder, 1500);
    encoder.write_chunk();

    ASSERT_EQ(stream.pop_size(), 20);
    check_ff(stream, 20);
    ASSERT_EQ(stream.pop_size(), 0);

    ASSERT_EQ(stream.pop_size(), 20);
    check_ff(stream, 20);
    ASSERT_EQ(stream.pop_size(), 0);

    ASSERT_EQ(stream.pop_size(), chunk_size);
    check_ff(stream, chunk_size);
    ASSERT_EQ(stream.pop_size(), 0);

    ASSERT_EQ(stream.pop_size(), 1467);
    check_ff(stream, 1467);
    ASSERT_EQ(stream.pop_size(), 0);

    size_t k = 10000 * 1500;

    while (k > 0) {
        auto size = k > chunk_size ? chunk_size : k;
        ASSERT_EQ(stream.pop_size(), size);
        check_ff(stream, size);
        ASSERT_EQ(stream.pop_size(), 0);
        k -= size;
    }
    ASSERT_EQ(stream.pop_size(), 0);
}

int main(int argc, char **argv)
{
    logging::init_sync();
    logging::log->pipe(std::make_unique<Stdout>());

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
