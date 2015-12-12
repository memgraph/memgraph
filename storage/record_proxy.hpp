#pragma once

#include "transactions/transaction.hpp"
#include "mvcc/version_list.hpp"

template <class T, class Store, class Derived>
class RecordProxy
{
public:
    Derived update(tx::Transaction& transaction) const
    {
        // TODO: implementation
        transaction.commit();
        return nullptr;
    }

    void remove(tx::Transaction& transaction) const
    {
        // TODO: implementation
        transaction.commit();
    }

private:
    T* record;
    Store *store;
    mvcc::VersionList<T> *version_list;
};
