#pragma once

#include <cstdlib>

#include "allocator.hpp"

template <class T,
          typename... Args,
          class allocator=fast_allocator<T>>
T* makeme(Args... args)
{
    allocator alloc;
    T* mem = alloc.allocate(1);
    return new (mem) T(args...);
}

template <class T,
          class allocator=fast_allocator<T>>
void takeme(T* mem)
{
    allocator alloc;
    mem->~T();
    alloc.deallocate(mem, 1);
}
