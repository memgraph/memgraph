// Copyright 2024 Memgraph Ltd.
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

#include <utility>

#include "storage/v2/vertex_accessor.hpp"

#include "storage/v2/indices/point_index_expensive_header.hpp"

namespace memgraph::storage {
struct PointIterator {
  using value_type = VertexAccessor;

  PointIterator(Storage *storage, Transaction *transaction, CoordinateReferenceSystem crs,
                index_t<IndexPointWGS2d>::const_query_iterator iter)
      : storage_{storage}, transaction_{transaction}, crs_{crs}, wgs84_2d_{std::move(iter)} {}

  PointIterator(Storage *storage, Transaction *transaction, CoordinateReferenceSystem crs,
                index_t<IndexPointWGS3d>::const_query_iterator iter)
      : storage_{storage}, transaction_{transaction}, crs_{crs}, wgs84_3d_{std::move(iter)} {}

  PointIterator(Storage *storage, Transaction *transaction, CoordinateReferenceSystem crs,
                index_t<IndexPointCartesian2d>::const_query_iterator iter)
      : storage_{storage}, transaction_{transaction}, crs_{crs}, cartesian_2d_{std::move(iter)} {}

  PointIterator(Storage *storage, Transaction *transaction, CoordinateReferenceSystem crs,
                index_t<IndexPointCartesian3d>::const_query_iterator iter)
      : storage_{storage}, transaction_{transaction}, crs_{crs}, cartesian_3d_{std::move(iter)} {}

  PointIterator(PointIterator const &o) : storage_{o.storage_}, transaction_{o.transaction_}, crs_{o.crs_} {
    switch (crs_) {
      case CoordinateReferenceSystem::WGS84_2d: {
        std::construct_at(&wgs84_2d_, o.wgs84_2d_);
        break;
      }
      case CoordinateReferenceSystem::WGS84_3d: {
        std::construct_at(&wgs84_3d_, o.wgs84_3d_);
        break;
      }
      case CoordinateReferenceSystem::Cartesian_2d: {
        std::construct_at(&cartesian_2d_, o.cartesian_2d_);
        break;
      }
      case CoordinateReferenceSystem::Cartesian_3d: {
        std::construct_at(&cartesian_3d_, o.cartesian_3d_);
        break;
      }
    }
  }

  PointIterator(PointIterator &&o) noexcept : storage_{o.storage_}, transaction_{o.transaction_}, crs_{o.crs_} {
    switch (crs_) {
      case CoordinateReferenceSystem::WGS84_2d: {
        std::construct_at(&wgs84_2d_, std::move(o.wgs84_2d_));
        break;
      }
      case CoordinateReferenceSystem::WGS84_3d: {
        std::construct_at(&wgs84_3d_, std::move(o.wgs84_3d_));
        break;
      }
      case CoordinateReferenceSystem::Cartesian_2d: {
        std::construct_at(&cartesian_2d_, std::move(o.cartesian_2d_));
        break;
      }
      case CoordinateReferenceSystem::Cartesian_3d: {
        std::construct_at(&cartesian_3d_, std::move(o.cartesian_3d_));
        break;
      }
    }
  }

  friend bool operator==(PointIterator const &lhs, PointIterator const &rhs) {
    if (lhs.crs_ != rhs.crs_) return false;
    switch (lhs.crs_) {
      case CoordinateReferenceSystem::WGS84_2d:
        return lhs.wgs84_2d_ == rhs.wgs84_2d_;
      case CoordinateReferenceSystem::WGS84_3d:
        return lhs.wgs84_3d_ == rhs.wgs84_3d_;
      case CoordinateReferenceSystem::Cartesian_2d:
        return lhs.cartesian_2d_ == rhs.cartesian_2d_;
      case CoordinateReferenceSystem::Cartesian_3d:
        return lhs.cartesian_3d_ == rhs.cartesian_3d_;
    }
  }

  auto operator=(PointIterator const &o) -> PointIterator & {
    if (this == &o) return *this;

    if (o.crs_ != crs_) {
      std::destroy_at(this);
      std::construct_at(this, o);
    } else {
      storage_ = o.storage_;
      transaction_ = o.transaction_;
      switch (crs_) {
        case CoordinateReferenceSystem::WGS84_2d:
          wgs84_2d_ = o.wgs84_2d_;
          break;
        case CoordinateReferenceSystem::WGS84_3d:
          wgs84_3d_ = o.wgs84_3d_;
          break;
        case CoordinateReferenceSystem::Cartesian_2d:
          cartesian_2d_ = o.cartesian_2d_;
          break;
        case CoordinateReferenceSystem::Cartesian_3d:
          cartesian_3d_ = o.cartesian_3d_;
          break;
      }
    }
    return *this;
  }

  auto operator=(PointIterator &&o) noexcept -> PointIterator & {
    // boost iterators shouldn't be moved
    if (o.crs_ != crs_) {
      std::destroy_at(this);
      std::construct_at(this, std::move(o));
    } else {
      storage_ = o.storage_;
      transaction_ = o.transaction_;
      switch (crs_) {
        case CoordinateReferenceSystem::WGS84_2d:
          wgs84_2d_ = std::move(o.wgs84_2d_);
          break;
        case CoordinateReferenceSystem::WGS84_3d:
          wgs84_3d_ = std::move(o.wgs84_3d_);
          break;
        case CoordinateReferenceSystem::Cartesian_2d:
          cartesian_2d_ = std::move(o.cartesian_2d_);
          break;
        case CoordinateReferenceSystem::Cartesian_3d:
          cartesian_3d_ = std::move(o.cartesian_3d_);
          break;
      }
    }
    return *this;
  }

  auto operator++() -> PointIterator & {
    switch (crs_) {
      case CoordinateReferenceSystem::WGS84_2d:
        ++wgs84_2d_;
        break;
      case CoordinateReferenceSystem::WGS84_3d:
        ++wgs84_3d_;
        break;
      case CoordinateReferenceSystem::Cartesian_2d:
        ++cartesian_2d_;
        break;
      case CoordinateReferenceSystem::Cartesian_3d:
        ++cartesian_3d_;
        break;
    }
    return *this;
  }

  auto operator*() const -> value_type {
    switch (crs_) {
      case CoordinateReferenceSystem::WGS84_2d: {
        auto *vertex = const_cast<Vertex *>(wgs84_2d_->vertex());
        return VertexAccessor{vertex, storage_, transaction_};
      }
      case CoordinateReferenceSystem::WGS84_3d: {
        auto *vertex = const_cast<Vertex *>(wgs84_3d_->vertex());
        return VertexAccessor{vertex, storage_, transaction_};
      }
      case CoordinateReferenceSystem::Cartesian_2d: {
        auto *vertex = const_cast<Vertex *>(cartesian_2d_->vertex());
        return VertexAccessor{vertex, storage_, transaction_};
      }
      case CoordinateReferenceSystem::Cartesian_3d: {
        auto *vertex = const_cast<Vertex *>(cartesian_3d_->vertex());
        return VertexAccessor{vertex, storage_, transaction_};
      }
    }
  }

  ~PointIterator() {
    switch (crs_) {
      case CoordinateReferenceSystem::WGS84_2d: {
        std::destroy_at(&wgs84_2d_);
        break;
      }
      case CoordinateReferenceSystem::WGS84_3d: {
        std::destroy_at(&wgs84_3d_);
        break;
      }
      case CoordinateReferenceSystem::Cartesian_2d: {
        std::destroy_at(&cartesian_2d_);
        break;
      }
      case CoordinateReferenceSystem::Cartesian_3d: {
        std::destroy_at(&cartesian_3d_);
        break;
      }
    }
  }

 private:
  Storage *storage_ = nullptr;
  Transaction *transaction_ = nullptr;
  CoordinateReferenceSystem crs_;
  union {
    index_t<IndexPointWGS2d>::const_query_iterator wgs84_2d_;
    index_t<IndexPointWGS3d>::const_query_iterator wgs84_3d_;
    index_t<IndexPointCartesian2d>::const_query_iterator cartesian_2d_;
    index_t<IndexPointCartesian3d>::const_query_iterator cartesian_3d_;
  };
};
}  // namespace memgraph::storage
