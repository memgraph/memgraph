#pragma once

#include <atomic>

#include "storage/indexes/index_base.hpp"
#include "utils/option.hpp"
#include "utils/option_ptr.hpp"

namespace tx
{
class Transaction;
}

// Holds one index which can be changed. Convinient class.
// TG - type group
// K - key of index_records
template <class TG, class K>
class IndexHolder
{

public:
    IndexHolder() = default;

    IndexHolder(IndexHolder const &) = delete;

    IndexHolder(IndexHolder &&) = default;

    // Sets index for this property family. returns false if index is already
    // present.
    bool set_index(std::unique_ptr<IndexBase<TG, K>> inx);

    // Returns index for read only if it is present and it's valid for read.
    OptionPtr<IndexBase<TG, K>> get_read() const;

    // Returns index for write only if it's present and transaction is
    // responsibly for updating it.
    OptionPtr<IndexBase<TG, K>> get_write(const tx::Transaction &t) const;

    // Removes index if it is given index. Caller is now responsable of
    // disposing index in a safe way.
    Option<std::unique_ptr<IndexBase<TG, K>>>
    remove_index(IndexBase<TG, K> *index);

    // Caller is now responsable of disposing index in a safe way.
    Option<std::unique_ptr<IndexBase<TG, K>>> remove_index();

private:
    std::atomic<IndexBase<TG, K> *> index = {nullptr};
};
