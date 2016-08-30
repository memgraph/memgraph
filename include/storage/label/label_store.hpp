#pragma once

#include <stdexcept>

#include "data_structures/concurrent/concurrent_set.hpp"
#include "storage/label/label.hpp"
#include "utils/char_str.hpp"

class LabelStore
{
public:
    using store_t = ConcurrentMap<CharStr, std::unique_ptr<Label>>;

    store_t::Accessor access();

    const Label &find_or_create(const char *name);

    bool contains(const char *name); // TODO: const

    // TODO: implement find method
    //       return { Label, is_found }

private:
    store_t labels;
};
