#include "storage/label/label_collection.hpp"

#include "storage/label/label.hpp"

auto LabelCollection::begin() { return _labels.begin(); }
auto LabelCollection::begin() const { return _labels.begin(); }
auto LabelCollection::cbegin() const { return _labels.begin(); }

auto LabelCollection::end() { return _labels.end(); }
auto LabelCollection::end() const { return _labels.end(); }
auto LabelCollection::cend() const { return _labels.end(); }

bool LabelCollection::add(const Label &label)
{
    if (has(label)) {
        return false;
    } else {
        _labels.push_back(label_ref_t(label));
        return true;
    }
    // return _labels.(label_ref_t(label)).second;
}

bool LabelCollection::has(const Label &label) const
{
    for (auto l : _labels) {
        if (l == label) {
            return true;
        }
    }
    return false;
}

size_t LabelCollection::count() const { return _labels.size(); }

bool LabelCollection::remove(const Label &label)
{
    auto end = _labels.end();
    for (auto it = _labels.begin(); it != end; it++) {
        if (*it == label) {
            _labels.erase(it);
            return true;
        }
    }

    return false;
}

void LabelCollection::clear() { _labels.clear(); }

const std::vector<label_ref_t> &LabelCollection::operator()() const
{
    return _labels;
}
