#ifndef MEMGRAPH_MEMORY_MEMORY_ENGINE_HPP
#define MEMGRAPH_MEMORY_MEMORY_ENGINE_HPP

#include <atomic>
#include <mutex>

#include "transaction/transaction.hpp"
#include "storage/model/record.hpp"
#include "storage/model/vertex.hpp"
#include "storage/model/edge.hpp"

// TODO implement the memory engine using the allocator style allocation to
// make this class non-dependent on the memory allocation strategy

// TODO implement real recycling of vertices and edges to improve performance
class MemoryEngine
{
public:
    
    template <class T,
              typename... Args>
    T* create(Args&&... args)
    {
        return new T(std::forward<Args>(args)...);
    }

    template<class T>
    T* allocate()
    {
        return static_cast<T*>(malloc(sizeof(T)));
    }

    template <class T>
    void recycle(Record<T>* record)
    {
        recycle(&record->derived());
    }

    void recycle(Vertex* v)
    {
        delete v;
    }

    void recycle(Edge* e)
    {
        delete e;
    }

private:
    
    std::unique_lock<SpinLock> acquire()
    {
        return std::unique_lock<SpinLock>(lock);
    }

    SpinLock lock;
};

#endif
