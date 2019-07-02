#pragma once

#include <memory>
#include <optional>

#include <glog/logging.h>

#include "utils/skip_list.hpp"

#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace storage {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

const uint64_t kTimestampInitialId = 0;
const uint64_t kTransactionInitialId = 1ULL << 63U;

class Storage final {
 public:
  class Accessor final {
   public:
    explicit Accessor(Storage *storage)
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

    Accessor(const Accessor &) = delete;
    Accessor &operator=(const Accessor &) = delete;

    Accessor(Accessor &&other) noexcept
        : storage_(other.storage_),
          transaction_(other.transaction_),
          is_transaction_starter_(true) {
      CHECK(other.is_transaction_starter_)
          << "The original accessor isn't valid!";
      // Don't allow the other accessor to abort our transaction.
      other.is_transaction_starter_ = false;
    }

    // This operator isn't `noexcept` because the `Abort` function isn't
    // `noexcept`.
    Accessor &operator=(Accessor &&other) {
      if (this == &other) return *this;

      if (is_transaction_starter_ && transaction_->is_active) {
        Abort();
      }

      storage_ = other.storage_;
      transaction_ = other.transaction_;
      is_transaction_starter_ = true;

      CHECK(other.is_transaction_starter_)
          << "The original accessor isn't valid!";
      // Don't allow the other accessor to abort our transaction.
      other.is_transaction_starter_ = false;

      return *this;
    }

    ~Accessor() {
      if (is_transaction_starter_ && transaction_->is_active) {
        Abort();
      }
    }

    VertexAccessor CreateVertex() {
      auto gid = storage_->vertex_id_.fetch_add(1, std::memory_order_acq_rel);
      auto acc = storage_->vertices_.access();
      auto delta = CreateDelta(transaction_, Delta::Action::DELETE_OBJECT, 0);
      auto [it, inserted] =
          acc.insert(Vertex{storage::Gid::FromUint(gid), delta});
      CHECK(inserted) << "The vertex must be inserted here!";
      CHECK(it != acc.end()) << "Invalid Vertex accessor!";
      transaction_->modified_vertices.push_back(&*it);
      return VertexAccessor::Create(&*it, transaction_, View::NEW).value();
    }

    std::optional<VertexAccessor> FindVertex(Gid gid, View view) {
      auto acc = storage_->vertices_.access();
      auto it = acc.find(gid);
      if (it == acc.end()) return std::nullopt;
      return VertexAccessor::Create(&*it, transaction_, view);
    }

    void AdvanceCommand() { ++transaction_->command_id; }

    void Commit() {
      CHECK(!transaction_->must_abort) << "The transaction can't be committed!";
      CHECK(transaction_->is_active)
          << "The transaction is already terminated!";
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

    void Abort() {
      CHECK(transaction_->is_active)
          << "The transaction is already terminated!";
      for (Vertex *vertex : transaction_->modified_vertices) {
        std::lock_guard<utils::SpinLock> guard(vertex->lock);
        Delta *current = vertex->delta;
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) ==
                   transaction_->transaction_id) {
          switch (current->action) {
            case Delta::Action::REMOVE_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(),
                                  current->key);
              CHECK(it != vertex->labels.end()) << "Invalid database state!";
              std::swap(*it, *vertex->labels.rbegin());
              vertex->labels.pop_back();
              break;
            }
            case Delta::Action::ADD_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(),
                                  current->key);
              CHECK(it == vertex->labels.end()) << "Invalid database state!";
              vertex->labels.push_back(current->key);
              break;
            }
            case Delta::Action::SET_PROPERTY: {
              auto it = vertex->properties.find(current->key);
              if (it != vertex->properties.end()) {
                if (current->value.IsNull()) {
                  // remove the property
                  vertex->properties.erase(it);
                } else {
                  // set the value
                  it->second = current->value;
                }
              } else if (!current->value.IsNull()) {
                vertex->properties.emplace(current->key, current->value);
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

   private:
    Storage *storage_;
    Transaction *transaction_;
    bool is_transaction_starter_;
  };

  Accessor Access() { return Accessor{this}; }

 private:
  // Main object storage
  utils::SkipList<storage::Vertex> vertices_;
  std::atomic<uint64_t> vertex_id_{0};

  // Transaction engine
  utils::SpinLock lock_;
  uint64_t timestamp_{kTimestampInitialId};
  uint64_t transaction_id_{kTransactionInitialId};
  utils::SkipList<Transaction> transactions_;
};

}  // namespace storage
