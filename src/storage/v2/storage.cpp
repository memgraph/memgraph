#include "storage/v2/storage.hpp"

#include <memory>

#include <glog/logging.h>

#include "storage/v2/mvcc.hpp"

namespace storage {

Storage::Accessor::Accessor(Storage *storage)
    : storage_(storage), is_transaction_starter_(true) {
  // We acquire the storage lock here because we access (and modify) the
  // transaction engine variables (`transaction_id` and `timestamp`) below.
  std::lock_guard<utils::SpinLock> guard(storage_->lock_);
  auto acc = storage_->transactions_.access();
  auto [it, inserted] = acc.insert(
      Transaction{storage_->transaction_id_++, storage_->timestamp_++});
  CHECK(inserted) << "The Transaction must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Transaction iterator!";
  transaction_ = &*it;
}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      transaction_(other.transaction_),
      is_transaction_starter_(true) {
  CHECK(other.is_transaction_starter_) << "The original accessor isn't valid!";
  // Don't allow the other accessor to abort our transaction.
  other.is_transaction_starter_ = false;
}

// This operator isn't `noexcept` because the `Abort` function isn't
// `noexcept`.
Storage::Accessor &Storage::Accessor::operator=(Accessor &&other) {
  if (this == &other) return *this;

  if (is_transaction_starter_ && transaction_->is_active) {
    Abort();
  }

  storage_ = other.storage_;
  transaction_ = other.transaction_;
  is_transaction_starter_ = true;

  CHECK(other.is_transaction_starter_) << "The original accessor isn't valid!";
  // Don't allow the other accessor to abort our transaction.
  other.is_transaction_starter_ = false;

  return *this;
}

Storage::Accessor::~Accessor() {
  if (is_transaction_starter_ && transaction_->is_active) {
    Abort();
  }
}

VertexAccessor Storage::Accessor::CreateVertex() {
  auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
  auto acc = storage_->vertices_.access();
  auto delta = CreateDeleteObjectDelta(transaction_);
  auto [it, inserted] = acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
  CHECK(inserted) << "The vertex must be inserted here!";
  CHECK(it != acc.end()) << "Invalid Vertex accessor!";
  transaction_->modified_vertices.push_back(&*it);
  return VertexAccessor::Create(&*it, transaction_, View::NEW).value();
}

std::optional<VertexAccessor> Storage::Accessor::FindVertex(Gid gid,
                                                            View view) {
  auto acc = storage_->vertices_.access();
  auto it = acc.find(gid);
  if (it == acc.end()) return std::nullopt;
  return VertexAccessor::Create(&*it, transaction_, view);
}

Result<bool> Storage::Accessor::DeleteVertex(VertexAccessor *vertex) {
  CHECK(vertex->transaction_ == transaction_)
      << "VertexAccessor must be from the same transaction as the storage "
         "accessor when deleting a vertex!";
  auto vertex_ptr = vertex->vertex_;

  std::lock_guard<utils::SpinLock> guard(vertex_ptr->lock);

  if (!PrepareForWrite(transaction_, vertex_ptr))
    return Result<bool>{Error::SERIALIZATION_ERROR};

  if (vertex_ptr->deleted) return Result<bool>{false};

  CreateAndLinkDelta(transaction_, vertex_ptr, Delta::RecreateObjectTag());

  vertex_ptr->deleted = true;

  return Result<bool>{true};
}

void Storage::Accessor::AdvanceCommand() { ++transaction_->command_id; }

void Storage::Accessor::Commit() {
  CHECK(!transaction_->must_abort) << "The transaction can't be committed!";
  CHECK(transaction_->is_active) << "The transaction is already terminated!";
  if (transaction_->deltas.empty()) {
    transaction_->commit_timestamp.store(transaction_->start_timestamp,
                                         std::memory_order_release);
  } else {
    std::lock_guard<utils::SpinLock> guard(storage_->lock_);
    transaction_->commit_timestamp.store(storage_->timestamp_++,
                                         std::memory_order_release);
    // TODO: release lock, and update all deltas to have an in-memory copy
    // of the commit id
  }
  transaction_->is_active = false;
}

void Storage::Accessor::Abort() {
  CHECK(transaction_->is_active) << "The transaction is already terminated!";
  for (Vertex *vertex : transaction_->modified_vertices) {
    std::lock_guard<utils::SpinLock> guard(vertex->lock);
    Delta *current = vertex->delta;
    while (current != nullptr &&
           current->timestamp->load(std::memory_order_acquire) ==
               transaction_->transaction_id) {
      switch (current->action) {
        case Delta::Action::REMOVE_LABEL: {
          auto it = std::find(vertex->labels.begin(), vertex->labels.end(),
                              current->label);
          CHECK(it != vertex->labels.end()) << "Invalid database state!";
          std::swap(*it, *vertex->labels.rbegin());
          vertex->labels.pop_back();
          break;
        }
        case Delta::Action::ADD_LABEL: {
          auto it = std::find(vertex->labels.begin(), vertex->labels.end(),
                              current->label);
          CHECK(it == vertex->labels.end()) << "Invalid database state!";
          vertex->labels.push_back(current->label);
          break;
        }
        case Delta::Action::SET_PROPERTY: {
          auto it = vertex->properties.find(current->property.key);
          if (it != vertex->properties.end()) {
            if (current->property.value.IsNull()) {
              // remove the property
              vertex->properties.erase(it);
            } else {
              // set the value
              it->second = current->property.value;
            }
          } else if (!current->property.value.IsNull()) {
            vertex->properties.emplace(current->property.key,
                                       current->property.value);
          }
          break;
        }
        case Delta::Action::DELETE_OBJECT: {
          auto acc = storage_->vertices_.access();
          CHECK(acc.remove(vertex->gid)) << "Invalid database state!";
          break;
        }
        case Delta::Action::RECREATE_OBJECT: {
          vertex->deleted = false;
          break;
        }
      }
      current = current->next.load(std::memory_order_acquire);
    }
    vertex->delta = current;
  }
  transaction_->is_active = false;
}

}  // namespace storage
