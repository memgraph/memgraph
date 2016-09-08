#pragma once

#include <vector>

// #include "storage/label/label.hpp"
#include "utils/reference_wrapper.hpp"

class Label;
using label_ref_t = ReferenceWrapper<const Label>;

class LabelCollection
{
public:
    auto begin() { return _labels.begin(); }
    auto begin() const { return _labels.begin(); }
    auto cbegin() const { return _labels.begin(); }

    auto end() { return _labels.end(); }
    auto end() const { return _labels.end(); }
    auto cend() const { return _labels.end(); }

    bool add(const Label &label);
    bool has(const Label &label) const;
    size_t count() const;
    bool remove(const Label &label);
    void clear();
    const std::vector<label_ref_t> &operator()() const;

private:
    std::vector<label_ref_t> _labels;
};
