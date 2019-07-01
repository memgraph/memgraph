#pragma once

#include <memory>
#include <optional>

#include "storage/v2/vertex.hpp"

#include "storage/v2/mvcc.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace storage {

class VertexAccessor final {
 private:
  VertexAccessor(Vertex *vertex, Transaction *transaction)
      : vertex_(vertex), transaction_(transaction) {}

 public:
  static std::optional<VertexAccessor> Create(Vertex *vertex,
                                              Transaction *transaction,
                                              View view) {
    bool is_visible = true;
    Delta *delta = nullptr;
    {
      std::lock_guard<utils::SpinLock> guard(vertex->lock);
      is_visible = !vertex->deleted;
      delta = vertex->delta;
    }
    ApplyDeltasForRead(transaction, delta, view,
                       [&is_visible](const Delta &delta) {
                         switch (delta.action) {
                           case Delta::Action::ADD_LABEL:
                           case Delta::Action::REMOVE_LABEL:
                             break;
                           case Delta::Action::RECREATE_OBJECT: {
                             is_visible = true;
                             break;
                           }
                           case Delta::Action::DELETE_OBJECT: {
                             is_visible = false;
                             break;
                           }
                         }
                       });
    if (!is_visible) return std::nullopt;
    return VertexAccessor{vertex, transaction};
  }

  Result<bool> Delete() {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);

    if (!PrepareForWrite(transaction_, vertex_))
      return Result<bool>{Error::SERIALIZATION_ERROR};

    if (vertex_->deleted) return Result<bool>{false};

    CreateAndLinkDelta(transaction_, vertex_, Delta::Action::RECREATE_OBJECT,
                       0);

    vertex_->deleted = true;

    return Result<bool>{true};
  }

  Result<bool> AddLabel(uint64_t label) {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);

    if (!PrepareForWrite(transaction_, vertex_))
      return Result<bool>{Error::SERIALIZATION_ERROR};

    if (vertex_->deleted) return Result<bool>{Error::DELETED_OBJECT};

    if (std::find(vertex_->labels.begin(), vertex_->labels.end(), label) !=
        vertex_->labels.end())
      return Result<bool>{false};

    CreateAndLinkDelta(transaction_, vertex_, Delta::Action::REMOVE_LABEL,
                       label);

    vertex_->labels.push_back(label);
    return Result<bool>{true};
  }

  Result<bool> RemoveLabel(uint64_t label) {
    std::lock_guard<utils::SpinLock> guard(vertex_->lock);

    if (!PrepareForWrite(transaction_, vertex_))
      return Result<bool>{Error::SERIALIZATION_ERROR};

    if (vertex_->deleted) return Result<bool>{Error::DELETED_OBJECT};

    auto it = std::find(vertex_->labels.begin(), vertex_->labels.end(), label);
    if (it == vertex_->labels.end()) return Result<bool>{false};

    CreateAndLinkDelta(transaction_, vertex_, Delta::Action::ADD_LABEL, label);

    std::swap(*it, *vertex_->labels.rbegin());
    vertex_->labels.pop_back();
    return Result<bool>{true};
  }

  Result<bool> HasLabel(uint64_t label, View view) {
    bool deleted = false;
    bool has_label = false;
    Delta *delta = nullptr;
    {
      std::lock_guard<utils::SpinLock> guard(vertex_->lock);
      deleted = vertex_->deleted;
      has_label = std::find(vertex_->labels.begin(), vertex_->labels.end(),
                            label) != vertex_->labels.end();
      delta = vertex_->delta;
    }
    ApplyDeltasForRead(transaction_, delta, view,
                       [&deleted, &has_label, label](const Delta &delta) {
                         switch (delta.action) {
                           case Delta::Action::REMOVE_LABEL: {
                             if (delta.value == label) {
                               CHECK(has_label) << "Invalid database state!";
                               has_label = false;
                             }
                             break;
                           }
                           case Delta::Action::ADD_LABEL: {
                             if (delta.value == label) {
                               CHECK(!has_label) << "Invalid database state!";
                               has_label = true;
                             }
                             break;
                           }
                           case Delta::Action::DELETE_OBJECT: {
                             LOG(FATAL) << "Invalid accessor!";
                             break;
                           }
                           case Delta::Action::RECREATE_OBJECT: {
                             deleted = false;
                             break;
                           }
                         }
                       });
    if (deleted) return Result<bool>{Error::DELETED_OBJECT};
    return Result<bool>{has_label};
  }

  Result<std::vector<uint64_t>> Labels(View view) {
    bool deleted = false;
    std::vector<uint64_t> labels;
    Delta *delta = nullptr;
    {
      std::lock_guard<utils::SpinLock> guard(vertex_->lock);
      deleted = vertex_->deleted;
      labels = vertex_->labels;
      delta = vertex_->delta;
    }
    ApplyDeltasForRead(
        transaction_, delta, view, [&deleted, &labels](const Delta &delta) {
          switch (delta.action) {
            case Delta::Action::REMOVE_LABEL: {
              // Remove the label because we don't see the addition.
              auto it = std::find(labels.begin(), labels.end(), delta.value);
              CHECK(it != labels.end()) << "Invalid database state!";
              std::swap(*it, *labels.rbegin());
              labels.pop_back();
              break;
            }
            case Delta::Action::ADD_LABEL: {
              // Add the label because we don't see the removal.
              auto it = std::find(labels.begin(), labels.end(), delta.value);
              CHECK(it == labels.end()) << "Invalid database state!";
              labels.push_back(delta.value);
              break;
            }
            case Delta::Action::DELETE_OBJECT: {
              LOG(FATAL) << "Invalid accessor!";
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              deleted = false;
              break;
            }
          }
        });
    if (deleted) return Result<std::vector<uint64_t>>{Error::DELETED_OBJECT};
    return Result<std::vector<uint64_t>>{std::move(labels)};
  }

  Gid Gid() const { return vertex_->gid; }

 private:
  Vertex *vertex_;
  Transaction *transaction_;
};

}  // namespace storage
