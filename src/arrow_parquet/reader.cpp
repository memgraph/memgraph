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

#include <arrow/acero/exec_plan.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/filesystem/api.h>

#include "spdlog/spdlog.h"

constexpr int64_t batch_rows = 1U << 18U;

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
  explicit RowIterator(std::unique_ptr<::arrow::RecordBatchReader> rbr, int const num_columns)
      : num_columns_(num_columns), rbr_(std::move(rbr)) {
    cols_.reserve(num_columns_);
  }

  RowIterator(const RowIterator &) = delete;
  RowIterator &operator=(const RowIterator &) = delete;
  RowIterator(RowIterator &&) = delete;
  RowIterator &operator=(RowIterator &&) = delete;
  ~RowIterator() = default;

  std::optional<RowRef> Next() {
    // Need to load another batch
    if (!batch_ || row_in_batch_ >= batch_->num_rows()) {
      auto const res = rbr_->Next();
      if (!res.ok()) {
        return std::nullopt;
      }
      batch_ = *res;
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
  std::unique_ptr<::arrow::RecordBatchReader> rbr_;
  // A record batch is table-like data structure that is semantically a sequence of fields, each a contiguous Arrow
  // array
  std::shared_ptr<::arrow::RecordBatch> batch_;
  std::vector<std::shared_ptr<::arrow::Array>> cols_;
  int64_t row_in_batch_{0};
};

struct ParquetReader::impl {
  explicit impl(std::unique_ptr<parquet::arrow::FileReader> file_reader,
                std::unique_ptr<::arrow::RecordBatchReader> rbr);

  auto GetNextRow() -> std::optional<Row>;

  auto GetSchema() -> std::shared_ptr<::arrow::Schema>;

 private:
  std::unique_ptr<parquet::arrow::FileReader> file_reader_;  // Keep FileReader alive
  std::shared_ptr<::arrow::Schema> schema_;
  int num_columns_;
  RowIterator row_it_;
};

ParquetReader::impl::impl(std::unique_ptr<parquet::arrow::FileReader> file_reader,
                          std::unique_ptr<::arrow::RecordBatchReader> rbr)
    : file_reader_(std::move(file_reader)),
      schema_(rbr->schema()),
      num_columns_(schema_->num_fields()),
      row_it_(RowIterator(std::move(rbr), num_columns_)) {}

// TODO: (andi)
// Next steps are optimizing this or trying Scanner
// Try to improve it by using Scanner -> it should be faster when the data is on disk, and ours it is
// ParquetFileReader?

auto ParquetReader::impl::GetNextRow() -> std::optional<Row> {
  auto const maybe_arrow_row = row_it_.Next();
  if (!maybe_arrow_row.has_value()) return std::nullopt;
  auto const &arrow_row = *maybe_arrow_row;

  Row row;
  row.reserve(num_columns_);

  for (int i = 0; i < num_columns_; i++) {
    // TODO: (andi) Not sure if this is good ValueOrDie
    auto cell = arrow_row[i].ToScalar().ValueOrDie()->ToString();
    row.push_back(std::move(cell));
  }

  return row;
}

auto ParquetReader::impl::GetSchema() -> std::shared_ptr<::arrow::Schema> { return schema_; }

ParquetReader::ParquetReader(std::string const &file) {
  auto const start = std::chrono::high_resolution_clock::now();
  ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

  try {
    // Open the file
    std::shared_ptr<::arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, ::arrow::io::ReadableFile::Open(file, pool));

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(batch_rows);
    arrow_reader_props.set_use_threads(true);

    // Build the reader
    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(reader_builder.Open(infile, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    PARQUET_THROW_NOT_OK(reader_builder.Build(&arrow_reader));

    // Get the RecordBatchReader
    auto res = arrow_reader->GetRecordBatchReader();
    if (!res.ok()) {
      throw std::runtime_error(res.status().message());
    }

    pimpl_ = std::make_unique<impl>(std::move(arrow_reader), std::move(*res));
  } catch (const std::exception &e) {
    spdlog::error("Failed to open parquet file '{}': {}", file, e.what());
    throw;
  }

  auto const duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start);
  spdlog::trace("Time spent on initializing parquet reader: {}ms", duration.count());
}

// Destructor must be defined here where impl is complete
ParquetReader::~ParquetReader() = default;

auto ParquetReader::GetNextRow() const -> std::optional<Row> { return pimpl_->GetNextRow(); }

auto ParquetReader::GetHeader() const -> Row {
  auto const schema = pimpl_->GetSchema();
  Row header;
  auto const header_size = schema->num_fields();
  header.reserve(header_size);
  for (auto const &field : schema->fields()) {
    header.push_back(field->name());
  }
  return header;
}

}  // namespace memgraph::arrow
