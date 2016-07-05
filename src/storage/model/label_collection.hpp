#pragma once

#include <set>

#include "label.hpp"

class LabelCollection
{
public:
    auto begin() { return _labels.begin(); }
    auto begin() const { return _labels.begin(); }
    auto cbegin() const { return _labels.begin(); }

    auto end() { return _labels.end(); }
    auto end() const { return _labels.end(); }
    auto cend() const { return _labels.end(); }

    bool add(const Label& label)
    {
        return _labels.insert(label_ref_t(label)).second;
    }

    bool has(const Label& label) const
    {
        return _labels.count(label);
    }

    size_t count() const {
        return _labels.size();
    }

    bool remove(const Label& label)
    {
        auto it = _labels.find(label);

        if(it == _labels.end())
            return false;

        return _labels.erase(it), true;
    }

    void clear()
    {
        _labels.clear();
    }

    const std::set<label_ref_t>& operator()() const
    {
        return _labels;
    }

private:
    std::set<label_ref_t> _labels;
};
