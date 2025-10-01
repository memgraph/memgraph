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

namespace memgraph::arrow {

struct CellRef {
  // Sequence of values with some data type
  std::shared_ptr<::arrow::Array> arr_;
  int64_t row_{0};
  // A single value
  ::arrow::Result<std::shared_ptr<::arrow::Scalar>> ToScalar() const { return arr_->GetScalar(row_); }
};

struct RowRef {
  std::vector<std::shared_ptr<::arrow::Array>> *cols_;
  int64_t row_{0};
  size_t size() const { return cols_->size(); }
  CellRef operator[](size_t const c) const { return {.arr_ = (*cols_)[c], .row_ = row_}; }
};

class RowIterator {
 public:
  RowIterator() = default;
  explicit RowIterator(std::shared_ptr<::arrow::Table> table, int num_columns, int64_t const batch_rows = 1UL << 16U)
      : num_columns_(num_columns) {
    cols_.reserve(table->num_columns());
    tbr_.emplace(std::move(table));
    tbr_->set_chunksize(batch_rows);
  }

  // Delete copy operations since TableBatchReader can't be copied
  RowIterator(const RowIterator &) = delete;
  RowIterator &operator=(const RowIterator &) = delete;
  RowIterator(RowIterator &&) = delete;
  RowIterator &operator=(RowIterator &&) = delete;
  ~RowIterator() = default;

  std::optional<RowRef> Next() {
    // Need to load another batch
    if (!batch_ || row_in_batch_ >= batch_->num_rows()) {
      if (const auto status = tbr_->ReadNext(&batch_); !status.ok()) {
        return std::nullopt;
      }
      // Check if we actually got a batch (end of data)
      if (!batch_) {
        return std::nullopt;
      }

      // Cache
      cols_.clear();
      for (int c = 0; c < num_columns_; ++c) {
        cols_.push_back(batch_->column(c));
      }
      row_in_batch_ = 0;
    }
    return RowRef(&cols_, row_in_batch_++);
  }

 private:
  int num_columns_;
  std::optional<::arrow::TableBatchReader> tbr_;
  // A record batch is table-like data structure that is semantically a sequence of fields, each a contiguous Arrow
  // array
  std::shared_ptr<::arrow::RecordBatch> batch_;
  std::vector<std::shared_ptr<::arrow::Array>> cols_;
  int64_t row_in_batch_{0};
};

struct ParquetReader::impl {
  explicit impl(std::shared_ptr<::arrow::Table> table);

  auto GetNextRow() -> std::optional<Row>;

 private:
  int num_columns_;
  std::string file_;
  RowIterator row_it_;
};

ParquetReader::impl::impl(std::shared_ptr<::arrow::Table> table)
    : num_columns_(table->num_columns()), row_it_(RowIterator(std::move(table), num_columns_)) {}

// TODO: (andi)
// Next steps are optimizing this or trying Scanner
// Try to improve it by using Scanner -> it should be faster when the data is on disk, and ours it is

auto ParquetReader::impl::GetNextRow() -> std::optional<Row> {
  auto const maybe_arrow_row = row_it_.Next();
  if (!maybe_arrow_row.has_value()) return std::nullopt;
  auto const &arrow_row = *maybe_arrow_row;

  Row row;
  row.reserve(num_columns_);

  for (size_t i = 0; i < num_columns_; i++) {
    // TODO: (andi) Not sure if this is good ValueOrDie
    auto cell = arrow_row[i].ToScalar().ValueOrDie()->ToString();
    row.push_back(std::move(cell));
  }

  return row;
}

ParquetReader::ParquetReader(std::string const &file) {
  auto const start = std::chrono::high_resolution_clock::now();
  std::shared_ptr<::arrow::io::ReadableFile> infile;
  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_ASSIGN_OR_THROW(infile, ::arrow::io::ReadableFile::Open(file, ::arrow::default_memory_pool()));
  PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, ::arrow::default_memory_pool()));
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table_));
  auto const duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start);
  spdlog::trace("Time spent on reading table: {}ms", duration.count());
  pimpl_ = std::make_unique<impl>(table_);
}

// Destructor must be defined here where impl is complete
ParquetReader::~ParquetReader() = default;

auto ParquetReader::GetNextRow() const -> std::optional<Row> { return pimpl_->GetNextRow(); }

auto ParquetReader::GetHeader() const -> Row {
  auto const schema = table_->schema();
  Row header;
  auto const header_size = schema->num_fields();
  header.reserve(header_size);
  for (auto const &field : schema->fields()) {
    header.push_back(field->name());
  }
  return header;
}

}  // namespace memgraph::arrow
