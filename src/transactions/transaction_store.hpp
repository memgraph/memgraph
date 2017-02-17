#pragma once

#include <map>
#include <memory>

#include "transactions/transaction.hpp"

namespace tx
{

template <class id_t>
class TransactionStore
{
public:
    Transaction* get(id_t id) const
    {
        auto it = store.find(id);
        return it != store.end() ? it->second.get() : nullptr;
    }

    void put(id_t id, Transaction* transaction)
    {
        store.emplace(std::make_pair(id, std::unique_ptr<Transaction>(transaction)));
    }

    void del(id_t id)
    {
        store.erase(id);
    }

private:
    std::map<id_t, std::unique_ptr<Transaction>> store;
};

}
