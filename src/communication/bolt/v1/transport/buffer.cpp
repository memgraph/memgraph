#include "communication/bolt/v1/transport/buffer.hpp"

namespace bolt
{

void Buffer::write(const byte* data, size_t len)
{
    buffer.insert(buffer.end(), data, data + len);
}

void Buffer::clear()
{
    buffer.clear();
}

}
