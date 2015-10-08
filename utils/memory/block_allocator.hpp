#ifndef MEMGRAPH_UTILS_MEMORY_BLOCK_ALLOCATOR_HPP
#define MEMGRAPH_UTILS_MEMORY_BLOCK_ALLOCATOR_HPP

#include <memory>
#include <vector>

#include "utils/auto_scope.hpp"

template <size_t block_size>
class BlockAllocator
{
    struct Block
    {
        Block()
        {
            data = malloc(block_size);
        }

        Block(void* ptr)
        {
            data = ptr;
        }

        void* data;
    };

public:
    static constexpr size_t size = block_size;

    BlockAllocator(size_t capacity = 0)
    {
        for(size_t i = 0; i < capacity; ++i)
            blocks.emplace_back();
    }

    void* acquire()
    {
        if(blocks.size() == 0)
            blocks.emplace_back();

        Auto(blocks.pop_back());
        return blocks.back().data;
    }
    
    void release(void* ptr)
    {
        blocks.emplace_back(ptr);
    }

private:
    std::vector<Block> blocks;
};

#endif
