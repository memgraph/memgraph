#pragma once

#include <set>
#include "label.hpp"

class LabelList
{
public:
    auto begin() { return labels.begin(); }
    auto begin() const { return labels.begin(); }
    auto cbegin() const { return labels.begin(); }

    auto end() { return labels.end(); }
    auto end() const { return labels.end(); }
    auto cend() const { return labels.end(); }

    bool add(const Label& label)
    {
        return labels.insert(label).second;
    }

    bool has(const Label& label) const
    {
        return labels.count(label);
    }

    size_t count() const {
        return labels.size();
    }

    bool remove(const Label& label)
    {
        auto it = labels.find(label);

        if(it == labels.end())
            return false;

        return labels.erase(it), true;
    }

    void clear()
    {
        labels.clear();
    }

private:
    std::set<const Label&> labels;
};
