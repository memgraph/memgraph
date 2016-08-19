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
    return _labels.insert(label_ref_t(label)).second;
}

bool LabelCollection::has(const Label &label) const
{
    return _labels.count(label);
}

size_t LabelCollection::count() const { return _labels.size(); }

bool LabelCollection::remove(const Label &label)
{
    auto it = _labels.find(label);

    if (it == _labels.end()) return false;

    return _labels.erase(it), true;
}

void LabelCollection::clear() { _labels.clear(); }

const std::set<label_ref_t> &LabelCollection::operator()() const
{
    return _labels;
}
