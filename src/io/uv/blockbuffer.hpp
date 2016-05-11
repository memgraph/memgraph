#pragma once

#include <cstring>
#include <uv.h>

#include "utils/memory/block_allocator.hpp"

namespace uv
{

template <size_t block_size>
class BlockBuffer
{
    static BlockAllocator<block_size> allocator;

    struct Block : public uv_buf_t
    {
        Block()
        {
            // acquire a new block of memory for this buffer
            base = static_cast<char*>(allocator.acquire());
            len = 0;
        }

        ~Block()
        {
            // release the block of memory previously acquired
            allocator.release(base);
        }

        size_t append(const char* data, size_t size)
        {
            // compute the remaining capacity for this block
            auto capacity = block_size - len;

            // if the capacity is smaller than the requested size, copy only
            // up to the remaining capacity
            if(size > capacity)
                size = capacity;

            std::memcpy(base + len, data, size);
            len += size;

            // return how much we've copied
            return size;
        }
    };

public:
    BlockBuffer()
    {
        // create the first buffer
        buffers.emplace_back();
    }

    ~BlockBuffer()
    {
        buffers.clear();
    }

    BlockBuffer(BlockBuffer&) = delete;
    BlockBuffer(BlockBuffer&&) = delete;

    size_t count() const
    {
        return buffers.size();
    }

    void clear()
    {
        // pop all buffers except for the first one since we need to allocate
        // it again anyway so why not keep it in the first place
        while(count() > 1)
            buffers.pop_back();

        // pretend we just allocated our first buffer and set it's length to 0
        buffers.back().len = 0;
    }

    BlockBuffer& operator<<(const std::string& data)
    {
        append(data);
        return *this;
    }

    void append(const std::string& data)
    {
        append(data.c_str(), data.size());
    }

    void append(const char* data, size_t size)
    {
        while(true)
        {
            // try to copy as much data as possible
            auto copied = buffers.back().append(data, size);

            // if we managed to copy everything, we're done
            if(copied == size)
                break;

            // move the pointer past the copied part
            data += copied;

            // reduce the total size by the number of copied items
            size -= copied;

            // since we ran out of space, construct a new buffer
            buffers.emplace_back();
        }
    }

    operator uv_buf_t*()
    {
        return buffers.data();
    }

private:
    std::vector<Block> buffers;
};

template <size_t block_size>
BlockAllocator<block_size> BlockBuffer<block_size>::allocator;

}
