#ifndef MEMGRAPH_TRANSACTIONS_TRANSACTION_CACHE_HPP
#define MEMGRAPH_TRANSACTIONS_TRANSACTION_CACHE_HPP

#include <map>
#include <memory>

#include "transaction.hpp"

namespace tx
{

template <class id_t>
class TransactionCache
{
public:
    Transaction* get(id_t id) const
    {
        auto it = cache.find(id);
        return it != cache.end() ? it->second.get() : nullptr;
    }

    void put(id_t id, Transaction* transaction)
    {
        cache.emplace(std::make_pair(id, std::unique_ptr<Transaction>(transaction)));
    }

    void del(id_t id)
    {
        cache.erase(id);
    }

private:
    std::map<id_t, std::unique_ptr<Transaction>> cache;
};

}

#endif
