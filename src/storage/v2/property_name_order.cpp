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

#include "storage/v2/property_name_order.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <utility>

namespace memgraph::storage {

thread_local PropertyNameOrder const *t_name_order = nullptr;

void PointThisThreadAt(PropertyNameOrder const &name_order) noexcept { t_name_order = &name_order; }

namespace {

/// Room for @p wanted numbers, rounded up so that growing costs a new layout
/// once in a while rather than once a name.
std::size_t RoomFor(std::size_t wanted) { return std::bit_ceil(std::max<std::size_t>(wanted, 16)); }

}  // namespace

PropertyNameOrder::PropertyNameOrder() {
  live_ = std::make_unique<Table>(RoomFor(0));
  current_.store(live_.get(), std::memory_order_seq_cst);
}

auto PropertyNameOrder::BetweenSiblings(std::size_t position) const -> std::optional<Order> {
  // Read from the layout in use rather than from a copy kept beside it. Every
  // neighbour is already published, and nothing but this writes.
  auto const *live = live_.get();

  // Nothing below the first name, and the numbers start a stride above that, so
  // a name arriving before every other still has room beneath it.
  auto const below = position == 0 ? Order{0} : live->At(in_name_order_[position - 1]);

  if (position + 1 == in_name_order_.size()) {
    // Past the last name, where the only limit is the top of the range.
    if (below > std::numeric_limits<Order>::max() - stride_) return std::nullopt;
    return below + stride_;
  }

  auto const ceiling = live->At(in_name_order_[position + 1]);
  if (ceiling - below <= 1) return std::nullopt;
  return below + (ceiling - below) / 2;
}

void PropertyNameOrder::Supersede(std::unique_ptr<Table> fresh) {
  // Published before the one it replaces is set aside, so that a reader taking
  // its place in the order after this point cannot come away with the old one.
  auto *published = fresh.get();
  auto superseded = std::exchange(live_, std::move(fresh));
  current_.store(published, std::memory_order_seq_cst);

  // Read after the store, so that every reader still able to hold the
  // superseded layout took its place before this and is waited for.
  retired_.emplace_back(std::move(superseded), readers_.CurrentEpoch());
  ++published_;
  FreeWhatNoReaderHolds();
}

void PropertyNameOrder::FreeWhatNoReaderHolds() {
  std::erase_if(retired_, [this](auto const &waiting) { return readers_.IsSafeToFree(waiting.second); });
}

void PropertyNameOrder::PublishOne(std::uint32_t id, Order order) {
  auto *live = live_.get();

  // A number already readable is never written again: a reader takes two of
  // them without a lock, and one from before a fresh layout against one from
  // after places a pair by numbers that were never together. What is written
  // here is a number no reader can reach yet.
  if (id < live->Capacity()) {
    live->Write(id, order);
    if (id >= live->Size()) live->Publish(id + 1);
    return;
  }

  auto fresh = std::make_unique<Table>(RoomFor(id + 1));
  for (auto published = std::size_t{0}; published != live->Size(); ++published) {
    fresh->Write(static_cast<std::uint32_t>(published), live->At(static_cast<std::uint32_t>(published)));
  }
  fresh->Write(id, order);
  fresh->Publish(id + 1);
  Supersede(std::move(fresh));
}

void PropertyNameOrder::LayOutAfresh() {
  // Spread over the whole range rather than by a fixed step, so that the room
  // between two neighbours is as much as the names there are leave. A fixed
  // step wastes the range while there are few names and runs out early once
  // there are many, and how often the numbers are laid out afresh is what
  // decides how much is held on to.
  MG_ASSERT(!in_name_order_.empty(), "Laying out the order afresh needs a name to lay out");
  stride_ = std::numeric_limits<Order>::max() / (in_name_order_.size() + 1);

  auto const highest = *std::ranges::max_element(in_name_order_);
  auto fresh = std::make_unique<Table>(RoomFor(highest + 1U));

  auto place = Order{0};
  for (auto const id : in_name_order_) {
    place += stride_;
    fresh->Write(id, place);
  }
  fresh->Publish(highest + 1U);
  Supersede(std::move(fresh));
}

void PropertyNameOrder::Add(std::uint32_t id, std::string_view name) {
  auto const lock = std::lock_guard{mutex_};

  auto const at =
      std::ranges::lower_bound(in_name_order_, name, {}, [this](std::uint32_t held) { return name_of_[held]; });
  if (at != in_name_order_.end() && name_of_[*at] == name) return;

  if (name_of_.size() <= id) name_of_.resize(id + 1);
  name_of_[id] = name;

  auto const position = static_cast<std::size_t>(at - in_name_order_.begin());
  in_name_order_.insert(at, id);

  if (auto const between = BetweenSiblings(position)) {
    PublishOne(id, *between);
    return;
  }

  // The two names either side of this one have no room between them, so every
  // name is spread out again and this one lands in the room that makes.
  LayOutAfresh();
}

void PropertyNameOrder::Clear() {
  auto const lock = std::lock_guard{mutex_};
  in_name_order_.clear();
  name_of_.clear();
  stride_ = kStride;

  // Published before the old layouts go, so that nothing is pointed at a layout
  // being destroyed even for the moment it takes to drop them.
  auto fresh = std::make_unique<Table>(RoomFor(0));
  auto *published = fresh.get();
  live_ = std::move(fresh);
  current_.store(published, std::memory_order_seq_cst);
  retired_.clear();
  published_ = 1;
}

}  // namespace memgraph::storage
