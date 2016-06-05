#pragma once

#include "data_structures/skiplist/skiplist.hpp"
#include "keys/unique_key.hpp"

#include "storage/cursor.hpp"

template <class Key, class Item>
class Index
{
    using skiplist_t = SkipList<Key, Item*>;
    using iterator_t = typename skiplist_t::Iterator;
    using accessor_t = typename skiplist_t::Accessor;
    using K = typename Key::key_t;

    using cursor_t = Cursor<accessor_t, iterator_t, K>;

public:
    cursor_t insert(const K& key, Item* item, tx::Transaction& t)
    {
        auto accessor = skiplist.access();
        auto result = accessor.insert_unique(key, item);

        // todo handle existing insert
    }

private:
    skiplist_t skiplist;
};
