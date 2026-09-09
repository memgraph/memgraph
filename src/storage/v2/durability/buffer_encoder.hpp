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

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/pipeline_budget.hpp"
#include "utils/crc_accumulator.hpp"

namespace memgraph::storage::durability {

/// BaseEncoder over an in-memory buffer with WAL-identical byte layout and CRC accumulation, so a transaction
/// encoded here can be appended verbatim to a WAL file. The buffer is std::vector<uint8_t, BudgetAllocator<uint8_t>>,
/// so every capacity growth is charged at the allocation boundary and throws PipelineBudgetExceeded when refused;
/// charges are released as allocations are freed and finally by the destructor.
class BufferEncoder final : public BaseEncoder {
 public:
  /// A policy with a null budget selects the plain allocator (no charging).
  explicit BufferEncoder(TxnAllocPolicy policy);

  BufferEncoder(BufferEncoder const &) = delete;
  BufferEncoder &operator=(BufferEncoder const &) = delete;
  BufferEncoder(BufferEncoder &&) = delete;
  BufferEncoder &operator=(BufferEncoder &&) = delete;

  void WriteMarker(Marker marker) override;
  void WriteBool(bool value) override;
  void WriteUint(uint64_t value) override;
  uint32_t WriteCrc() override;
  void WriteDouble(double value) override;
  void WriteString(std::string_view value) override;
  void WriteEnum(storage::Enum value) override;
  void WritePoint2d(storage::Point2d value) override;
  void WritePoint3d(storage::Point3d value) override;
  void WriteExternalPropertyValue(const ExternalPropertyValue &value) override;

  /// The number of bytes written so far; positions handed out to callers are relative to the buffer start.
  auto GetPosition() -> uint64_t override;

  void ResetCrcAcc() override { crc_acc_.Reset(); }

  auto CrcAccValue() const -> uint32_t override { return crc_acc_.Value(); }

  auto bytes() const -> std::span<const uint8_t> { return {buffer_.data(), buffer_.size()}; }

  /// Bytes currently charged to the budget for this buffer (its single live allocation); 0 without a budget.
  auto charged_bytes() const -> uint64_t;

 private:
  // The only function that appends to the buffer: grows the allocation when needed, copies, feeds the CRC.
  void Write(const uint8_t *data, uint64_t size);
  void WriteSize(uint64_t size);

  TxnAllocPolicy policy_;
  std::vector<uint8_t, BudgetAllocator<uint8_t>> buffer_;
  utils::CrcAccumulator crc_acc_;
};

}  // namespace memgraph::storage::durability
