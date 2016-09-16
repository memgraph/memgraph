#pragma once

#include <memory>
#include <vector>

#include "utils/auto_scope.hpp"

template <size_t block_size>
class BlockAllocator
{
    struct Block
    {
        Block() { data = malloc(block_size); }

        Block(void *ptr) { data = ptr; }

        void *data;
    };

public:
    static constexpr size_t size = block_size;

    BlockAllocator(size_t capacity = 0)
    {
        for (size_t i = 0; i < capacity; ++i)
            blocks.emplace_back();
    }

    ~BlockAllocator()
    {
        for (auto b : blocks) {
            free(b.data);
        }
        blocks.clear();
    }

    // Returns nullptr on no memory.
    void *acquire()
    {
        if (blocks.size() == 0) blocks.emplace_back();

        auto ptr = blocks.back().data;
        Auto(blocks.pop_back());
        return ptr;
    }

    void release(void *ptr) { blocks.emplace_back(ptr); }

private:
    std::vector<Block> blocks;
};
