#include <cassert>
#include <deque>
#include <iostream>
#include <vector>

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
        assert(stream.pop() == byte('\xFF'));

    (void)stream;
}

int main(void)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());
    DummyStream stream;
    bolt::ChunkedEncoder<DummyStream> encoder(stream);

    write_ff(encoder, 10);
    write_ff(encoder, 10);
    encoder.flush();

    write_ff(encoder, 10);
    write_ff(encoder, 10);
    encoder.flush();

    // this should be two chunks, one of size 65533 and the other of size 1467
    write_ff(encoder, 67000);
    encoder.flush();

    for (int i = 0; i < 10000; ++i)
        write_ff(encoder, 1500);
    encoder.flush();

    assert(stream.pop_size() == 20);
    check_ff(stream, 20);
    assert(stream.pop_size() == 0);

    assert(stream.pop_size() == 20);
    check_ff(stream, 20);
    assert(stream.pop_size() == 0);

    assert(stream.pop_size() == encoder.chunk_size);
    check_ff(stream, encoder.chunk_size);
    assert(stream.pop_size() == 1467);
    check_ff(stream, 1467);
    assert(stream.pop_size() == 0);

    size_t k = 10000 * 1500;

    while (k > 0) {
        auto size = k > encoder.chunk_size ? encoder.chunk_size : k;
        assert(stream.pop_size() == size);
        check_ff(stream, size);

        k -= size;
    }

    assert(stream.pop_size() == 0);

    return 0;
}
