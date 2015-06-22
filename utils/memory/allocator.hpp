#ifndef MEMGRAPH_UTILS_MEMORY_ALLOCATOR_HPP
#define MEMGRAPH_UTILS_MEMORY_ALLOCATOR_HPP

#include <cstdlib>
#include <new>

template <class Tp>
struct fast_allocator {
    typedef Tp value_type;

    fast_allocator() = default;

    template <class T>
    fast_allocator(const fast_allocator<T>&) {}
  
    Tp* allocate(std::size_t n);
    void deallocate(Tp* p, std::size_t n);
};

template <class Tp>
Tp* fast_allocator<Tp>::allocate(std::size_t n)
{
    // hopefully we're using jemalloc here!
    Tp* mem = static_cast<Tp*>(malloc(n * sizeof(Tp)));

    if(mem != nullptr)
        return mem;

    throw std::bad_alloc();
}

template <class Tp>
void fast_allocator<Tp>::deallocate(Tp* p, std::size_t)
{
    // hopefully we're using jemalloc here!
    free(p);
}

template <class T, class U>
bool operator==(const fast_allocator<T>&, const fast_allocator<U>&)
{
    return true;
}

template <class T, class U>
bool operator!=(const fast_allocator<T>& a, const fast_allocator<U>& b)
{
    return !(a == b);
}

#endif
