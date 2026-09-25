// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "utils/epoch_tracker.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

/// Where a property name sorts, as one number per identifier.
///
/// An identifier records when a name was first seen, so two of them say nothing
/// about where their names sort against each other, and two databases holding
/// the same data give the same name different identifiers. A comparison that
/// has to answer in name order therefore cannot read identifiers, and reading
/// the names themselves costs a string compare on a path that runs per pair of
/// map entries.
///
/// This gives every identifier a number ordered the way its name is. The
/// numbers a database hands out depend on the order it saw the names in, but
/// the order they place any pair in does not, which is what a query returns.
class PropertyNameOrder {
 public:
  using Order = std::uint64_t;

  /// The room left between neighbours the first time the numbers are laid out.
  /// After that it follows how many names there are, so that the range is spread
  /// over them rather than stepped through at a fixed size.
  static constexpr Order kStride = Order{1} << 32;

  /// One layout of the numbers, read without a lock.
  ///
  /// A number means nothing outside the layout it came from: laying them out
  /// afresh keeps the order and changes every number, so two read from
  /// different layouts can place a pair backwards. Read the layout once per
  /// comparison and take both sides from it.
  class Table {
   public:
    explicit Table(std::size_t capacity) : order_{std::make_unique<Order[]>(capacity)}, capacity_{capacity} {}

    Order At(std::uint32_t id) const noexcept {
      // Held in every build: what it guards is a read past the end of the
      // array, on a path that takes no lock and so has nothing else to catch it.
      MG_ASSERT(id < size_.load(std::memory_order_acquire),
                "A property identifier reached a comparison before its place in the order was published");
      return order_[id];
    }

    auto Size() const noexcept -> std::size_t { return size_.load(std::memory_order_acquire); }

    auto Capacity() const noexcept -> std::size_t { return capacity_; }

   private:
    friend class PropertyNameOrder;

    /// Writes one number. The caller holds the mint lock, and publishes the
    /// size covering it afterwards.
    void Write(std::uint32_t id, Order order) noexcept { order_[id] = order; }

    /// Makes every number below @p size readable. Release, so a reader that
    /// sees the size sees the numbers written before it.
    void Publish(std::size_t size) noexcept { size_.store(size, std::memory_order_release); }

    std::unique_ptr<Order[]> order_;
    std::size_t capacity_;
    std::atomic<std::size_t> size_{0};
  };

  PropertyNameOrder();

  PropertyNameOrder(PropertyNameOrder const &) = delete;
  PropertyNameOrder &operator=(PropertyNameOrder const &) = delete;
  PropertyNameOrder(PropertyNameOrder &&) = delete;
  PropertyNameOrder &operator=(PropertyNameOrder &&) = delete;
  ~PropertyNameOrder() = default;

  /// A layout, held for as long as this lives.
  ///
  /// Taken before the layout is read, which is what makes a superseded layout
  /// safe to free: a reader still holding one took its place in the order
  /// before the layout was retired, and the retirement waits for it.
  class Reading {
   public:
    explicit Reading(PropertyNameOrder const &name_order) noexcept
        : readers_{&name_order.readers_},
          id_{name_order.readers_.Acquire()},
          layout_{name_order.current_.load(std::memory_order_seq_cst)} {}

    Reading(Reading const &) = delete;
    Reading &operator=(Reading const &) = delete;
    Reading(Reading &&) = delete;
    Reading &operator=(Reading &&) = delete;

    ~Reading() { readers_->Release(id_); }

    auto operator->() const noexcept -> Table const * { return layout_; }

    auto operator*() const noexcept -> Table const & { return *layout_; }

   private:
    utils::EpochTracker *readers_;
    std::uint64_t id_;
    Table const *layout_;
  };

  /// The layout as it stands, held while the returned object lives.
  auto Read() const noexcept -> Reading { return Reading{*this}; }

  /// How many layouts have been published, which is one plus the number of
  /// times the numbers ran out of room or outgrew it. Lets a test say it
  /// reached the case rather than assume it did.
  auto LayoutsPublished() const -> std::size_t {
    auto const lock = std::lock_guard{mutex_};
    return published_;
  }

  /// How much the layouts hold on to: the one in use, and any superseded one a
  /// reader might still be holding. Lets a test hold that to a stated bound
  /// rather than leave it unwatched.
  auto RetainedBytes() const -> std::size_t {
    auto const lock = std::lock_guard{mutex_};
    auto held = live_ ? live_->Capacity() * sizeof(Order) : std::size_t{0};
    for (auto const &[layout, _] : retired_) held += layout->Capacity() * sizeof(Order);
    return held;
  }

  /// Gives @p name a number placing it where it sorts among the names already
  /// here.
  ///
  /// Takes a lock, which no reader takes: a name is interned once, when a query
  /// first names a property, so this runs at the rate a schema changes rather
  /// than at the rate rows are read.
  ///
  /// @pre @p name outlives this. A name is never taken back, so the mapper's
  /// own storage gives that.
  /// @pre @p id was not handed here before.
  void Add(std::uint32_t id, std::string_view name);

  /// Forgets every name, for a mapper being scrubbed back to empty.
  ///
  /// @pre no reader holds a layout, which is the same precondition the mapper's
  /// own scrub carries: the names a layout was built from are freed by it.
  void Clear();

 private:
  /// The number to give the name now standing at @p position, or nothing where
  /// its two neighbours have no room left between them.
  auto BetweenSiblings(std::size_t position) const -> std::optional<Order>;

  /// Spreads every name evenly again, keeping the order and changing every
  /// number, into a layout of its own. The caller holds the lock.
  void LayOutAfresh();

  /// Makes one number readable, in a layout of its own where it no longer
  /// fits the room there is. The caller holds the lock.
  void PublishOne(std::uint32_t id, Order order);

  /// Puts @p fresh in use and sets aside the one it replaces, to be freed once
  /// no reader can still hold it. The caller holds the lock.
  void Supersede(std::unique_ptr<Table> fresh);

  /// Frees every superseded layout no reader can still be holding. The caller
  /// holds the lock.
  void FreeWhatNoReaderHolds();

  mutable std::mutex mutex_;

  /// The room left between two neighbours, recomputed each time the numbers are
  /// laid out afresh so that it follows how many names there are.
  Order stride_{kStride};

  /// The identifiers, ordered by the names they carry, and the name each one
  /// carries, indexed by the identifier.
  ///
  /// Two vectors rather than a tree keyed by name: identifiers come from a
  /// counter, so they are dense and index an array directly, and a tree would
  /// spend a node with two pointers and a colour on every name to answer the
  /// one question asked of it. Finding where a name goes is a search over one
  /// run of memory, and putting it there moves the tail, which costs nothing at
  /// the rate names are interned.
  std::vector<std::uint32_t> in_name_order_;
  std::vector<std::string_view> name_of_;

  /// The layout in use, and the ones it replaced that a reader may still be
  /// holding, each with the place in the reader order it was retired at. One is
  /// freed once every reader that took its place before then has finished.
  std::unique_ptr<Table> live_;
  std::vector<std::pair<std::unique_ptr<Table>, std::uint64_t>> retired_;

  /// How many layouts have been made, which the count above stops recording
  /// once they start being freed.
  std::size_t published_{1};

  /// The readers, so that a superseded layout is freed rather than kept for the
  /// life of the database.
  mutable utils::EpochTracker readers_;

  std::atomic<Table const *> current_{nullptr};
};

/// The name_order that comparisons on this thread read.
///
/// A pair of stored maps has to be placed in the order their keys' names sort
/// in, and the comparison holds identifiers rather than names. It cannot be
/// handed the name_order: the comparison *is* the ordering of the index's own
/// structure and is reached from inside it, with no argument to carry one. It
/// is installed for the thread instead, by whatever entered the storage the
/// values belong to.
///
/// Declared here and defined once, rather than inline, so that a module reading
/// it through the global fragment and a header reading it directly name the one
/// object.
extern thread_local PropertyNameOrder const *t_name_order;

/// Points the calling thread at @p name_order, which is what a comparison of two
/// stored maps reads to place them in the order their keys' names sort in.
///
/// Assigned rather than scoped: a thread takes up work in one storage and does
/// it, and a thread that goes on to serve another storage points itself at that
/// one before it compares anything of its.
void PointThisThreadAt(PropertyNameOrder const &name_order) noexcept;

}  // namespace memgraph::storage
