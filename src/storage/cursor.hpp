#pragma once

#include "transactions/transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/model/properties/property.hpp"

#include "storage/vertex.hpp"

template <class Accessor, class It, class Derived>
class Cursor : public Crtp<Derived>
{
public:
    Cursor(Accessor&& accessor, It item, tx::Transaction& t)
        : accessor(accessor), item(item), t(t)
    {

    }

    Derived& operator++()
    {
        ++item;
        return this->derived();
    }

    Derived& operator++(int)
    {
        return operator++();
    }

protected:
    Accessor accessor;
    tx::Transaction& t;
    It item;
};
