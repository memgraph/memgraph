#pragma once

// TODO: remove from here and from the project
#include <iostream>

#include "memory/freelist.hpp"
#include "memory/lazy_gc.hpp"
#include "threading/sync/spinlock.hpp"

template <class T, class lock_t = SpinLock>
class SkiplistGC : public LazyGC<SkiplistGC<T, lock_t>, lock_t>
{
public:
    // release_ref method should be called by a thread
    // when the thread finish it job over object
    // which has to be lazy cleaned
    // if thread counter becames zero, all objects in the local_freelist
    // are going to be deleted
    // the only problem with this approach is that
    // GC may never be called, but for now we can deal with that
    void release_ref()
    {
        std::vector<T *> local_freelist;

        // take freelist if there is no more threads
        {
            auto lock = this->acquire_unique();
            assert(this->count > 0);
            --this->count;
            if (this->count == 0) {
                freelist.swap(local_freelist);
            }
        }

        if (local_freelist.size() > 0) {
            std::cout << "GC started" << std::endl;
            std::cout << "Local list size: " << local_freelist.size()
                      << std::endl;
            long long counter = 0;
            // destroy all elements from local_freelist
            for (auto element : local_freelist) {

                if (element->flags.is_marked()) {
                    T::destroy(element);
                    counter++;
                }
            }
            std::cout << "Number of destroyed elements " << counter
                      << std::endl;
        }
    }

    void collect(T *node) { freelist.add(node); }

private:
    FreeList<T> freelist;
};
