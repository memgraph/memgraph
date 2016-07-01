#pragma once

#include <stdexcept>

#include "model/label.hpp"
#include "data_structures/skiplist/skiplistset.hpp"

class LabelStore
{
public:

    const Label& find_or_create(const std::string& name)
    {
        auto accessor = labels.access();
        return accessor.insert(Label(name)).first;
    }

    bool contains(const std::string& name) // const
    {
        auto accessor = labels.access();
        return accessor.find(Label(name)) != accessor.end();
    }

    // TODO: implement find method
    //       return { Label, is_found }

private:
    SkipListSet<Label> labels;
};
