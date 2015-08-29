#ifndef MEMGRAPH_STORAGE_STORAGE_ENGINE_HPP
#define MEMGRAPH_STORAGE_STORAGE_ENGINE_HPP

#include <atomic>
#include <mutex>

#include "transaction/transaction.hpp"
#include "storage/model/record.hpp"
#include "memory/memory_engine.hpp"
#include "utils/counters/atomic_counter.hpp"

class StorageEngine
{
public:
    StorageEngine(MemoryEngine& memory) : vertex_counter(0), edge_counter(0),
        memory(memory) {}

    template <class T>
    bool insert(T** record, const Transaction& t)
    {
        auto n = next<T>();
        *record = memory.create<T>(n);
        
        // set creating transaction
        (*record)->tx.min(t.id);

        // set creating command of this transaction
        (*record)->cmd.min(t.cid);
        
        return true;
    }

    template <class T>
    bool update(T* record,
                T** updated,
                const Transaction& t)
    {
        // put a lock on the node to prevent other writers from modifying it
        auto guard = record->guard();

        // find the record visible version of the record about to be updated
        record = record->latest(t);

        if(record == nullptr)
            return false; // another transaction just deleted it!

        *updated = memory.allocate<T>();
        **updated = *record; // copy the data in the current node TODO memset?

        record->newer(*updated);
        return true;
    }

    template <class T>
    bool remove(T& record,
                const Transaction& t)
    {
        // put a lock on the node to prevent other writers from modifying it
        auto guard = record->guard();

        auto latest = record.latest(t);

        if(record == nullptr)
            return false;

        // only mark the record as deleted if it isn't already deleted
        // this prevents phantom reappearance of the deleted nodes
        //
        // T1 |---------- DELETE N --------| COMMIT
        // T2      |----- DELETE N ---------------------------------| COMMIT
        // T3                                   |-- SELECT N --|
        //
        // if xmax was overwritten by T2, T3 would see that T1 was still
        // running and determined that the record hasn't been deleted yet
        // even though T1 already committed before T3 even started!

        if(record->xmax())
            return false; // another transaction deleted it! 

        record->xmax(t.id);
        return true;
    }

private:

    template<class T>
    uint64_t next();

    AtomicCounter<uint64_t> vertex_counter;
    AtomicCounter<uint64_t> edge_counter;

    MemoryEngine& memory;
};


template<>
uint64_t StorageEngine::next<Vertex>()
{
    return vertex_counter.next();
}

template<>
uint64_t StorageEngine::next<Edge>()
{
    return edge_counter.next();
}

#endif
