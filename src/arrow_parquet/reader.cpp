// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "arrow_parquet/reader.hpp"

#include <chrono>
#include <optional>
#include <sstream>

#include "spdlog/spdlog.h"
#include "utils/memory.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"

namespace memgraph::arrow {

class RowIterator {
 public:
  RowIterator() = default;
  explicit RowIterator(std::shared_ptr<::arrow::Table> table, int num_columns, utils::MemoryResource *resource,
                       int64_t const batch_rows = 1UL << 18U)  // Increase batch size to 256K
      : num_columns_(num_columns), resource_(resource), row_buffer_(resource) {
    cols_.resize(num_columns);  // Pre-allocate columns vector
    tbr_.emplace(std::move(table));
    tbr_->set_chunksize(batch_rows);
    // Pre-allocate row buffer to avoid repeated allocations
    row_buffer_.resize(num_columns_);
  }

  // Delete copy operations since TableBatchReader can't be copied
  RowIterator(const RowIterator &) = delete;
  RowIterator &operator=(const RowIterator &) = delete;
  RowIterator(RowIterator &&) = delete;
  RowIterator &operator=(RowIterator &&) = delete;
  ~RowIterator() = default;

  std::optional<Row> Next() {
    // Need to load another batch
    if (!batch_ || row_in_batch_ >= batch_->num_rows()) {
      if (const auto status = tbr_->ReadNext(&batch_); !status.ok()) {
        return std::nullopt;
      }
      // Check if we actually got a batch (end of data)
      if (!batch_) {
        return std::nullopt;
      }

      // Cache columns using indexed assignment (no clear/push_back)
      for (int c = 0; c < num_columns_; ++c) {
        cols_[c] = batch_->column(c);
      }
      row_in_batch_ = 0;
    }
    // Reuse pre-allocated row buffer
    for (int i = 0; i < num_columns_; ++i) {
      auto scalar = cols_[i]->GetScalar(row_in_batch_).ValueOrDie();
      row_buffer_[i] = utils::pmr::string(scalar->ToString(), resource_);
    }
    row_in_batch_++;
    return row_buffer_;
  }

 private:
  int num_columns_;
  utils::MemoryResource *resource_;
  std::optional<::arrow::TableBatchReader> tbr_;
  // A record batch is table-like data structure that is semantically a sequence of fields, each a contiguous Arrow
  // array
  std::shared_ptr<::arrow::RecordBatch> batch_;
  std::vector<std::shared_ptr<::arrow::Array>> cols_;
  int64_t row_in_batch_{0};
  Row row_buffer_;  // Pre-allocated buffer to avoid repeated allocations
};

struct ParquetReader::impl {
  explicit impl(std::shared_ptr<::arrow::Table> table, utils::MemoryResource *resource);

  auto GetNextRow(utils::MemoryResource *resource) -> std::optional<Row>;

 private:
  int num_columns_;
  std::shared_ptr<::arrow::Table> table_;
  RowIterator row_it_;  // Lazy initialization with memory resource
};

ParquetReader::impl::impl(std::shared_ptr<::arrow::Table> table, utils::MemoryResource *resource)
    : num_columns_(table->num_columns()), table_(std::move(table)), row_it_(table_, num_columns_, resource) {}

// TODO: (andi)
// Next steps are optimizing this or trying Scanner
// Try to improve it by using Scanner -> it should be faster when the data is on disk, and ours it is

auto ParquetReader::impl::GetNextRow(utils::MemoryResource *resource) -> std::optional<Row> { return row_it_.Next(); }

ParquetReader::ParquetReader(std::string const &file, utils::MemoryResource *resource) {
  auto const start = std::chrono::high_resolution_clock::now();
  std::shared_ptr<::arrow::io::ReadableFile> infile;
  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_ASSIGN_OR_THROW(infile, ::arrow::io::ReadableFile::Open(file, ::arrow::default_memory_pool()));
  PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, ::arrow::default_memory_pool()));
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table_));
  auto const duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start);
  spdlog::trace("Time spent on reading table: {}ms", duration.count());
  pimpl_ = std::make_unique<impl>(table_, resource);
}

// Destructor must be defined here where impl is complete
ParquetReader::~ParquetReader() = default;

auto ParquetReader::GetNextRow(utils::MemoryResource *resource) const -> std::optional<Row> {
  return pimpl_->GetNextRow(resource);
}

auto ParquetReader::GetHeader(utils::MemoryResource *resource) const -> Row {
  auto const schema = table_->schema();
  Row header(resource);
  auto const header_size = schema->num_fields();
  header.resize(header_size);
  for (int i = 0; i < header_size; ++i) {
    header[i] = utils::pmr::string(schema->field(i)->name(), resource);
  }
  return header;
}

}  // namespace memgraph::arrow
