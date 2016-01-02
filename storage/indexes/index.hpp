#pragma once

#include "data_structures/skiplist/skiplist.hpp"
#include "keys/unique_key.hpp"

#include "storage/cursor.hpp"

template <class Key, class Cursor, class Item, class SortOrder>
class Index
{
    using skiplist_t = SkipList<Key, Item*>;
    using iterator_t = typename skiplist_t::Iterator;
    using accessor_t = typename skiplist_t::Accessor;
    using K = typename Key::key_t;

public:
    Cursor insert(const K& key, Item* item, tx::Transaction& t)
    {

    }


private:
    skiplist_t skiplist;
};
