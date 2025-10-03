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

#include "query/arrow_parquet/reader.hpp"
#include "query/typed_value.hpp"

#include <chrono>
#include <optional>

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include "arrow/acero/exec_plan.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/filesystem/api.h"

#include "spdlog/spdlog.h"

// TODO: (andi) See if dataset can be somehow used better. One option is that you read files in parallel
// Parallelizing batches and processing + profile

constexpr int64_t batch_rows = 1U << 16U;

namespace memgraph::query {

class BatchIterator {
 public:
  BatchIterator() = default;
  explicit BatchIterator(std::shared_ptr<arrow::RecordBatchReader> rbr, int const num_columns)
      : num_columns_(num_columns), rbr_(std::move(rbr)) {}

  BatchIterator(const BatchIterator &) = delete;
  BatchIterator &operator=(const BatchIterator &) = delete;
  BatchIterator(BatchIterator &&) = delete;
  BatchIterator &operator=(BatchIterator &&) = delete;
  ~BatchIterator() = default;

  // The user knows when to request the next batch
  std::vector<std::shared_ptr<arrow::Array>> Next() const {
    auto const res = rbr_->Next();
    if (!res.ok() || !(*res)) {
      return {};
    }
    auto const &batch = *res;

    std::vector<std::shared_ptr<arrow::Array>> result;
    result.reserve(num_columns_);
    for (int c = 0; c < num_columns_; ++c) {
      result.emplace_back(batch->column(c));
    }

    return result;
  }

 private:
  int num_columns_;
  std::shared_ptr<arrow::RecordBatchReader> rbr_;
};

struct ParquetReader::impl {
  explicit impl(std::shared_ptr<arrow::RecordBatchReader> rbr, std::shared_ptr<arrow::Schema> schema,
                utils::MemoryResource *resource);

  auto GetNextRow() -> std::optional<Row>;

  auto GetSchema() -> std::shared_ptr<arrow::Schema>;

 private:
  std::shared_ptr<arrow::Schema> schema_;
  int num_columns_;
  BatchIterator row_it_;
  utils::pmr::vector<Row> rows_;  // cached, pre-allocated rows
  uint64_t row_in_batch_{0};
  uint64_t current_batch_size_{0};
  utils::MemoryResource *memory_resource_;  // For TypedValue allocations
};

ParquetReader::impl::impl(std::shared_ptr<arrow::RecordBatchReader> rbr, std::shared_ptr<arrow::Schema> schema,
                          utils::MemoryResource *resource)
    : schema_(std::move(schema)),
      num_columns_(schema_->num_fields()),
      row_it_(BatchIterator(std::move(rbr), num_columns_)),
      rows_(resource),
      memory_resource_(resource) {
  // Preallocate
  rows_.resize(batch_rows);
  for (int i = 0; i < batch_rows; ++i) {
    rows_[i] = Row(resource);
    rows_[i].resize(num_columns_);
  }
}

auto ParquetReader::impl::GetNextRow() -> std::optional<Row> {
  // No batch loaded or full batch was consumed
  if (row_in_batch_ >= current_batch_size_) {
    auto const batch_ref = row_it_.Next();
    // No more data
    if (batch_ref.empty()) {
      return std::nullopt;
    }

    auto const num_rows = batch_ref[0]->length();

    // TODO: (andi) Switch for other types that can be passed into TypedValue
    for (int j = 0U; j < num_columns_; j++) {
      auto const &column = batch_ref[j];

      if (auto const type_id = column->type_id(); type_id == arrow::Type::INT64) {
        auto const int_array = std::static_pointer_cast<arrow::Int64Array>(column);
        for (int64_t i = 0; i < num_rows; i++) {
          rows_[i][j] = TypedValue(int_array->Value(i), memory_resource_);
        }
      } else if (type_id == arrow::Type::DOUBLE) {
        auto const double_array = std::static_pointer_cast<arrow::DoubleArray>(column);
        for (int64_t i = 0; i < num_rows; i++) {
          rows_[i][j] = TypedValue(double_array->Value(i), memory_resource_);
        }
      } else {
        auto const string_array = std::static_pointer_cast<arrow::StringArray>(column);
        for (int64_t i = 0; i < num_rows; i++) {
          rows_[i][j] = TypedValue(string_array->GetString(i), memory_resource_);
        }
      }
    }

    row_in_batch_ = 0;
    current_batch_size_ = num_rows;
  }
  return rows_[row_in_batch_++];
}

auto ParquetReader::impl::GetSchema() -> std::shared_ptr<arrow::Schema> { return schema_; }

ParquetReader::ParquetReader(std::string const &file, utils::MemoryResource *resource) {
  auto const start = std::chrono::high_resolution_clock::now();

  try {
    if (auto const status = arrow::compute::Initialize(); !status.ok()) {
      throw std::runtime_error(status.message());
    }

    // Use Dataset API for better performance
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

    // Create ParquetFileFormat with optimized settings
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();

    // Configure for better performance
    auto *parquet_fragment_options = format->default_fragment_scan_options.get();
    if (parquet_fragment_options) {
    }

    // Create FileSource
    auto source = arrow::dataset::FileSource(file, fs);

    // Create Fragment
    auto fragment_result = format->MakeFragment(source);
    if (!fragment_result.ok()) {
      throw std::runtime_error("Failed to create fragment: " + fragment_result.status().ToString());
    }
    auto const fragment = fragment_result.ValueOrDie();

    // Read schema
    auto schema_result = fragment->ReadPhysicalSchema();
    if (!schema_result.ok()) {
      throw std::runtime_error("Failed to read schema: " + schema_result.status().ToString());
    }
    auto schema = schema_result.ValueOrDie();

    // Create scan options - keep it simple to avoid compute function issues
    auto scan_options = std::make_shared<arrow::dataset::ScanOptions>();
    scan_options->dataset_schema = schema;
    scan_options->batch_size = batch_rows;
    scan_options->use_threads = true;

    // Create scanner builder
    arrow::dataset::ScannerBuilder scanner_builder(schema, fragment, scan_options);

    auto scanner_result = scanner_builder.Finish();
    if (!scanner_result.ok()) {
      throw std::runtime_error("Failed to create scanner: " + scanner_result.status().ToString());
    }
    auto scanner = scanner_result.ValueOrDie();

    // Get RecordBatchReader
    auto reader_result = scanner->ToRecordBatchReader();
    if (!reader_result.ok()) {
      throw std::runtime_error("Failed to create reader: " + reader_result.status().ToString());
    }
    auto reader = reader_result.ValueOrDie();

    pimpl_ = std::make_unique<impl>(std::move(reader), std::move(schema), resource);
  } catch (const std::exception &e) {
    spdlog::error("Failed to open parquet file '{}': {}", file, e.what());
    throw;
  }

  auto const duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start);
  spdlog::trace("Dataset scanner initialized in {}ms", duration.count());
}

// Destructor must be defined here where impl is complete
ParquetReader::~ParquetReader() = default;

auto ParquetReader::GetNextRow() const -> std::optional<Row> { return pimpl_->GetNextRow(); }

auto ParquetReader::GetHeader(utils::MemoryResource *resource) const -> Header {
  auto const schema = pimpl_->GetSchema();
  Header header(resource);
  auto const header_size = schema->num_fields();
  header.reserve(header_size);
  for (auto const &field : schema->fields()) {
    header.emplace_back(field->name());
  }
  return header;
}

}  // namespace memgraph::query
