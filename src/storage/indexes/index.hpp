#pragma once

#include <memory>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/indexes/index_record.hpp"
#include "storage/indexes/index_record_collection.hpp"
#include "storage/label/label.hpp"

template <class Key, class Item>
class Index
{
  public:
  using container_t = ConcurrentMap<Key, Item>;

  Index() : index(std::make_unique<container_t>()) {}

  auto update(const Label &label, VertexIndexRecord &&index_record)
  {
    auto accessor = index->access();
    auto label_ref = label_ref_t(label);

    // create Index Record Collection if it doesn't exist
    if (!accessor.contains(label_ref)) {
      accessor.insert(label_ref, std::move(VertexIndexRecordCollection()));
    }

    // add Vertex Index Record to the Record Collection
    auto &record_collection = (*accessor.find(label_ref)).second;
    record_collection.add(std::forward<VertexIndexRecord>(index_record));
  }

  VertexIndexRecordCollection &find(const Label &label)
  {
    // TODO: accessor should be outside?
    // bacause otherwise GC could delete record that has just be returned
    auto label_ref = label_ref_t(label);
    auto accessor = index->access();
    return (*accessor.find(label_ref)).second;
  }

  private:
  std::unique_ptr<container_t> index;
};
