#pragma once

#include <atomic>
#include <cassert>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

template <class block_t = uint8_t, size_t chunk_size = 32768>
class DynamicBitset : Lockable<SpinLock>
{
    struct Block
    {
        Block() = default;

        Block(Block &) = delete;
        Block(Block &&) = delete;

        static constexpr size_t size = sizeof(block_t) * 8;

        constexpr block_t bitmask(size_t group_size) const
        {
            return (block_t)(-1) >> (size - group_size);
        }

        block_t at(size_t k, size_t n, std::memory_order order)
        {
            assert(k + n - 1 < size);
            return (block.load(order) >> k) & bitmask(n);
        }

        void set(size_t k, size_t n, std::memory_order order)
        {
            assert(k + n - 1 < size);
            block.fetch_or(bitmask(n) << k, order);
        }

        void clear(size_t k, size_t n, std::memory_order order)
        {
            assert(k + n - 1 < size);
            block.fetch_and(~(bitmask(n) << k), order);
        }

        std::atomic<block_t> block{0};
    };

    struct Chunk
    {
        Chunk() : next(nullptr)
        {
            static_assert(chunk_size % sizeof(block_t) == 0,
                          "chunk size not divisible by block size");
        }

        Chunk(Chunk &) = delete;
        Chunk(Chunk &&) = delete;

        ~Chunk() { delete next; }

        static constexpr size_t size = chunk_size * Block::size;
        static constexpr size_t n_blocks = chunk_size / sizeof(block_t);

        block_t at(size_t k, size_t n, std::memory_order order)
        {
            return blocks[k / Block::size].at(k % Block::size, n, order);
        }

        void set(size_t k, size_t n, std::memory_order order)
        {
            blocks[k / Block::size].set(k % Block::size, n, order);
        }

        void clear(size_t k, size_t n, std::memory_order order)
        {
            blocks[k / Block::size].clear(k % Block::size, n, order);
        }

        Block blocks[n_blocks];
        std::atomic<Chunk *> next;
    };

public:
    DynamicBitset() : head(new Chunk()) {}

    DynamicBitset(DynamicBitset &) = delete;
    DynamicBitset(DynamicBitset &&) = delete;

    ~DynamicBitset()
    {
        auto now = head.load();
        while (now != nullptr) {
            auto next = now->next.load();
            delete now;
            now = next;
        }
    }

    block_t at(size_t k, size_t n)
    {
        auto &chunk = find_chunk(k);
        return chunk.at(k, n, std::memory_order_seq_cst);
    }

    bool at(size_t k)
    {
        auto &chunk = find_chunk(k);
        return chunk.at(k, 1, std::memory_order_seq_cst);
    }

    void set(size_t k, size_t n = 1)
    {
        auto &chunk = find_chunk(k);
        return chunk.set(k, n, std::memory_order_seq_cst);
    }

    void clear(size_t k, size_t n = 1)
    {
        auto &chunk = find_chunk(k);
        return chunk.clear(k, n, std::memory_order_seq_cst);
    }

private:
    Chunk &find_chunk(size_t &k)
    {
        Chunk *chunk = head.load(), *next = nullptr;

        // while i'm not in the right chunk
        // (my index is bigger than the size of this chunk)
        while (k >= Chunk::size) {
            next = chunk->next.load();

            // if a next chunk exists, switch to it and decrement my
            // pointer by the size of the current chunk
            if (next != nullptr) {
                chunk = next;
                k -= Chunk::size;
                continue;
            }

            // the next chunk does not exist and we need it. take an exclusive
            // lock to prevent others that also want to create a new chunk
            // from creating it
            auto guard = acquire_unique();

            // double-check locking. if the chunk exists now, some other thread
            // has just created it, continue searching for my chunk
            if (chunk->next.load() != nullptr) continue;

            chunk->next.store(new Chunk());
        }

        assert(chunk != nullptr);
        return *chunk;
    }

    std::atomic<Chunk *> head;
};
