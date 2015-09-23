#ifndef MEMGRAPH_UTILS_MARK_REF_HPP
#define MEMGRAPH_UTILS_MARK_REF_HPP

#include <stdint.h>

template <class T>
struct MarkRef
{
    MarkRef() = default;
    MarkRef(MarkRef&) = default;
    MarkRef(MarkRef&&) = default;

    bool is_marked() const
    {
        return ptr & 0x1L;
    }

    bool set_mark()
    {
        return ptr |= 0x1L;
    }

    bool clear_mark()
    {
        return ptr &= ~0x1L;
    }

    T* get() const
    {
        return reinterpret_cast<T*>(ptr & ~0x1L);
    }

    T& operator*() { return *get(); }
    T* operator->() { return get(); }

    uintptr_t ptr;
};

//template <class T, class... Args>
//MarkRef<T> make_markref(Args&&... args)
//{
//
//}

#endif
