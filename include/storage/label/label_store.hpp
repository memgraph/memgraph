#pragma once

#include <stdexcept>

#include "storage/label/label.hpp"
#include "data_structures/concurrent/concurrent_set.hpp"

class LabelStore
{
public:

    const Label& find_or_create(const std::string& name);

    bool contains(const std::string& name); // TODO: const

    // TODO: implement find method
    //       return { Label, is_found }

private:
    ConcurrentSet<Label> labels;
};
