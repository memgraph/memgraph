#include "storage/label/label_collection.hpp"

#include "storage/label/label.hpp"

bool LabelCollection::add(const Label &label)
{
    if (has(label)) {
        return false;
    } else {
        labels_.push_back(label_ref_t(label));
        return true;
    }
}

bool LabelCollection::has(const Label &label) const
{
    for (auto l : labels_) {
        if (l == label) {
            return true;
        }
    }
    return false;
}

size_t LabelCollection::count() const { return labels_.size(); }

bool LabelCollection::remove(const Label &label)
{
    auto end = labels_.end();
    for (auto it = labels_.begin(); it != end; it++) {
        if (*it == label) {
            labels_.erase(it);
            return true;
        }
    }

    return false;
}

void LabelCollection::clear() { labels_.clear(); }

const std::vector<label_ref_t> &LabelCollection::operator()() const
{
    return labels_;
}
