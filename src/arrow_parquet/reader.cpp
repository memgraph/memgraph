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

#include "arrow/acero/exec_plan.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/dataset.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/io/file.h"
#include "parquet/properties.h"

#include "spdlog/spdlog.h"

constexpr int64_t batch_rows = 1U << 16U;

namespace memgraph::arrow {

class BatchIterator {
 public:
  BatchIterator() = default;
  explicit BatchIterator(std::unique_ptr<::arrow::RecordBatchReader> rbr, int const num_columns)
      : num_columns_(num_columns), rbr_(std::move(rbr)) {
    cols_.resize(num_columns_);
  }

  BatchIterator(const BatchIterator &) = delete;
  BatchIterator &operator=(const BatchIterator &) = delete;
  BatchIterator(BatchIterator &&) = delete;
  BatchIterator &operator=(BatchIterator &&) = delete;
  ~BatchIterator() = default;

  // The user knows when to request the next batch
  // Return cached values for now to avoid constant creation and reallocation
  std::vector<std::shared_ptr<::arrow::Array>> *Next() {
    // Need to load another batch
    auto const res = rbr_->Next();
    if (!res.ok() || !(*res)) {
      return nullptr;
    }
    batch_ = *res;

    for (int c = 0; c < num_columns_; ++c) {
      cols_[c] = batch_->column(c);
    }

    return &cols_;
  }

 private:
  int num_columns_;
  std::unique_ptr<::arrow::RecordBatchReader> rbr_;
  // A record batch is table-like data structure that is semantically a sequence of fields, each a contiguous Arrow
  // array
  std::shared_ptr<::arrow::RecordBatch> batch_;
  std::vector<std::shared_ptr<::arrow::Array>> cols_;
};

struct ParquetReader::impl {
  explicit impl(std::unique_ptr<parquet::arrow::FileReader> file_reader,
                std::unique_ptr<::arrow::RecordBatchReader> rbr, utils::MemoryResource *resource);

  auto GetNextRow() -> std::optional<Row>;

  auto GetSchema() -> std::shared_ptr<::arrow::Schema>;

 private:
  std::unique_ptr<parquet::arrow::FileReader> file_reader_;  // Keep FileReader alive
  std::shared_ptr<::arrow::Schema> schema_;
  int num_columns_;
  BatchIterator row_it_;
  // Cached rows
  utils::pmr::vector<Row> rows_;
  uint64_t row_in_batch_{0};
  std::vector<std::shared_ptr<::arrow::Array>> *batch_ref_{nullptr};
  uint64_t current_batch_size_{0};
};

ParquetReader::impl::impl(std::unique_ptr<parquet::arrow::FileReader> file_reader,
                          std::unique_ptr<::arrow::RecordBatchReader> rbr, utils::MemoryResource *resource)
    : file_reader_(std::move(file_reader)),
      schema_(rbr->schema()),
      num_columns_(schema_->num_fields()),
      row_it_(BatchIterator(std::move(rbr), num_columns_)),
      rows_(resource) {
  // Preallocate
  rows_.resize(batch_rows);
  for (int i = 0; i < batch_rows; ++i) {
    rows_[i].resize(num_columns_);
  }
}

// TODO: (andi)
// Next steps are optimizing this or trying Scanner
// Try to improve it by using Scanner -> it should be faster when the data is on disk, and ours it is
// ParquetFileReader?

auto ParquetReader::impl::GetNextRow() -> std::optional<Row> {
  // No batch loaded or full batch was consumed
  if (row_in_batch_ >= current_batch_size_) {
    batch_ref_ = row_it_.Next();
    // No more data
    if (!batch_ref_) {
      return std::nullopt;
    }

    auto const num_rows = (*batch_ref_)[0]->length();

    // Iterate over columns to be cache friendly
    for (int j = 0U; j < num_columns_; j++) {
      auto cast_result = ::arrow::compute::Cast(*(*batch_ref_)[j], ::arrow::utf8()).ValueOrDie();
      auto const string_array = std::static_pointer_cast<::arrow::StringArray>(cast_result);
      for (int64_t i = 0U; i < num_rows; i++) {
        rows_[i][j] = string_array->GetString(i);
        // rows_[i][j] = (*batch_ref_)[j]->GetScalar(i).ValueOrDie()->ToString();
      }
    }
    row_in_batch_ = 0;
    current_batch_size_ = num_rows;
  }

  return rows_[row_in_batch_++];
}

auto ParquetReader::impl::GetSchema() -> std::shared_ptr<::arrow::Schema> { return schema_; }

ParquetReader::ParquetReader(std::string const &file, utils::MemoryResource *resource) {
  auto const start = std::chrono::high_resolution_clock::now();
  ::arrow::MemoryPool *pool = ::arrow::default_memory_pool();

  try {
    // Open the file
    std::shared_ptr<::arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, ::arrow::io::ReadableFile::Open(file, pool));

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
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

    pimpl_ = std::make_unique<impl>(std::move(arrow_reader), std::move(*res), resource);
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

auto ParquetReader::GetHeader(utils::MemoryResource *resource) const -> Row {
  auto const schema = pimpl_->GetSchema();
  Row header(resource);
  auto const header_size = schema->num_fields();
  header.reserve(header_size);
  for (auto const &field : schema->fields()) {
    header.emplace_back(field->name());
  }
  return header;
}

}  // namespace memgraph::arrow
