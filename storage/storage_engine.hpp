#ifndef MEMGRAPH_STORAGE_STORAGE_ENGINE_HPP
#define MEMGRAPH_STORAGE_STORAGE_ENGINE_HPP

#include <atomic>
#include <mutex>

#include "transaction/transaction.hpp"
#include "storage/model/record.hpp"
#include "storage/visible.hpp"
#include "memory/memory_engine.hpp"

template <class id_t,
          class lock_t>
class StorageEngine
{
    template <class T>
    using record_t = Record<T, id_t, lock_t>;
    using memory_engine_t = MemoryEngine<id_t, lock_t>;

public:
    StorageEngine(memory_engine_t& memory) : memory(memory) {}

    template <class T>
    bool insert(record_t<T>** record,
                const Transaction<id_t>& t)
    {

    }

    template <class T>
    bool update(record_t<T>* record,
                record_t<T>** updated,
                const Transaction<id_t>& t)
    {
        // put a lock on the node to prevent other writers from modifying it
        auto guard = record->guard();

        // find the newest visible version of the record about to be updated
        auto newest = max_visible(record, t);

        if(newest == nullptr)
            return false; // another transaction just deleted it!

        *updated = memory.allocate<T>();
        *updated = *newest; // copy the data in the current node TODO memset

        newest->newer(latest);
        *updated_record = newest

        return true;
    }

    template <class T>
    bool remove(record_t<T>& record,
                const Transaction<id_t>& t)
    {
        // put a lock on the node to prevent other writers from modifying it
        auto guard = record.guard();

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

        if(record.xmax())
            return false; // another transaction just deleted it!        

        record.xmax(t.id);
        return true;
    }

private:
    
    std::unique_lock<lock_t> acquire()
    {
        return std::unique_lock<lock_t>(lock);
    }

    memory_engine_t& memory;

    lock_t lock;
};

#endif
