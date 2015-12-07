#pragma once

#include "recycler.hpp"

template <class T, class Allocator>
class DeferredRecycler : Recycler<T, Allocator>
{
public:
    using Recycler<T, Allocator>::acquire;

    void recycle(T* item)
    {
        auto guard = this->acquire_unique();
        dirty.push_back(item);
    }

    void clean()
    {
        auto guard = this->acquire_unique();

        for(auto item : dirty)
            this->recycle_or_delete(item);

        dirty.clear();
    }

private:
    std::queue<T*> dirty;
};
