#pragma once

#include <vector>

#include "utils/reference_wrapper.hpp"

class Label;
using label_ref_t = ReferenceWrapper<const Label>;

class LabelCollection
{
public:
    auto begin() { return labels_.begin(); }
    auto begin() const { return labels_.begin(); }
    auto cbegin() const { return labels_.begin(); }

    auto end() { return labels_.end(); }
    auto end() const { return labels_.end(); }
    auto cend() const { return labels_.end(); }

    bool add(const Label &label);
    bool has(const Label &label) const;
    size_t count() const;
    bool remove(const Label &label);
    void clear();
    const std::vector<label_ref_t> &operator()() const;

    template <class Handler>
    void handle(Handler &handler) const
    {
        for (auto &label : labels_)
            handler.handle(label.get());
    }

private:
    std::vector<label_ref_t> labels_;
};
