#include "transactions/snapshot.hpp"

#include "utils/serialization.hpp"

namespace tx {

void Snapshot::Save(capnp::Snapshot::Builder *builder) const {
  auto list_builder = builder->initTransactionIds(transaction_ids_.size());
  utils::SaveVector(transaction_ids_, &list_builder);
}

void Snapshot::Load(const capnp::Snapshot::Reader &reader) {
  utils::LoadVector(&transaction_ids_, reader.getTransactionIds());
}

}  // namespace tx
