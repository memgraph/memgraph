#pragma once

#include <stdexcept>

#include "data_structures/concurrent/concurrent_set.hpp"
#include "storage/label/label.hpp"
#include "utils/char_str.hpp"

class LabelStore
{
public:
    const Label &find_or_create(const char *name);

    bool contains(const char *name); // TODO: const

    // TODO: implement find method
    //       return { Label, is_found }

private:
    ConcurrentMap<CharStr, std::unique_ptr<Label>> labels;
};
