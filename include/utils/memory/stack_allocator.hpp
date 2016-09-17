#pragma once

#include <cmath>
#include "query_engine/exceptions/exceptions.hpp"
#include "utils/memory/block_allocator.hpp"

// http://en.cppreference.com/w/cpp/language/new

// Useful for allocating memory which can be freed with one call.
// Most performant for data which need to be present to the end.
class StackAllocator
{
    static constexpr size_t page_size = 64 * 1024;

public:
    ~StackAllocator() { free(); }

    // Allocates memory for object of type T.
    // Retruns pointer to memory for it.
    template <class T>
    inline T *allocate()
    {
        // If size is bigger than pages_size then this do-whil will never end
        // until it eats all the memory.
        static_assert(sizeof(T) <= page_size,
                      "Cant allocate objects larger than page_size");
        do {
            // Mask which has log2(alignof(T)) lower bits setted to 0 and the
            // rest to 1.
            // example:
            // alignof(T)==8 => mask=0xfffffffffffffff8;
            // This will be calculated in compile time.
            size_t mask = ~(((size_t)alignof(T)) - 1);

            // aligned contains ptr aligned to alignof(T).
            // There are two types of head ptr:
            // a) aligned to alignof(T)
            // b) not aligned to alignof(T)
            // For a) whe want for aligned to be equal to head, and for b)
            // aligned shuold be first aligned ptr greater than head.
            //
            // head - 1 => turns a) into b) now whe only have to get first
            // aligned ptr greater than (head - 1).
            //
            // (head - 1) & mask => will produce first smaller than head ptr
            // aligned to alignof(T).
            //
            // ( (head - 1) & mask ) + alignof(T) => will produce first grater
            // or equal than head ptr aligned to alignof(T).
            char *aligned =
                (char *)(((((size_t)head) - 1) & mask) + alignof(T));

            // New head which points to unallocated memory points to first byte
            // after space for object T.
            char *new_head = aligned + sizeof(T);

            // If the new_head is greater than end that means that there isn't
            // enough space for object T in current block of memory.
            if (LIKELY(new_head <= end)) {

                // All is fine, head can become new_head
                head = new_head;

                // Returns aligned ptr with enough space for object T.
                return (T *)aligned;
            }

            // There isn't enough free space so whe must allocate more.
            void *alloc = blocks.acquire();

            // Check if there are memory. If not throw exception rather than
            // return nullptr.
            if (UNLIKELY(alloc == nullptr))
                throw new OutOfMemory("BlockAllocator returned nullptr");

            // Remember that whee allocated memory so that whe can free-it
            // after.
            allocated_blocks.push_back(alloc);

            // Set new head, the old one isn't needed anymore.
            head = (char *)alloc;

            // Update end to point to first byte after newly allocated memory.
            end = head + page_size;

            // After allocating new memory lets try again to "allocate" place
            // for T.
        } while (true);
    }

    template <class T, class... Args>
    inline T *make(Args &&... args)
    {
        auto ptr = allocate<T>();
        new (ptr) T(std::forward<Args>(args)...);
        return ptr;
    }

    // Relases all memory.
    void free()
    {
        while (allocated_blocks.size() > 0) {
            blocks.release(allocated_blocks.back());
            allocated_blocks.pop_back();
        }
    }

private:
    BlockAllocator<page_size> blocks;
    std::vector<void *> allocated_blocks;
    char *head = {nullptr};
    char *end = {nullptr};
};
