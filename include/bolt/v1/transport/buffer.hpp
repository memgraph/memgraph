#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

namespace bolt
{

class Buffer
{
public:
    using byte = uint8_t;

    void write(const byte* data, size_t len);

    void clear();

    size_t size() const
    {
        return buffer.size();
    }

    byte operator[](size_t idx) const
    {
        return buffer[idx];
    }

    const byte* data() const
    {
        return buffer.data();
    }

private:
    std::vector<byte> buffer;
};


}
