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

template <class id_t,
          class lock_t,
class MemoryEngine
{
    template <class T>
    using record_t = Record<T, id_t, lock_t>;

    using vertex_t = Vertex<id_t, lock_t>;
    using edge_t = Edge<id_t, lock_t>;
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
    void recycle(record_t<T>* record)
    {
        recycle(&record->derived());
    }

    void recycle(vertex_t v)
    {
        delete v;
    }

    void recycle(edge_t e)
    {
        delete e;
    }

private:
    
    std::unique_lock<lock_t> acquire()
    {
        return std::unique_lock<lock_t>(lock);
    }

    lock_t lock;
};

#endif
