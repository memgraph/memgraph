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
#include "utils/data_queue.hpp"
#include "utils/temporal.hpp"

#include <chrono>
#include <thread>

#include "arrow/acero/exec_plan.h"
#include "arrow/api.h"
#include "arrow/io/file.h"
#include "arrow/util/float16.h"
#include "parquet/properties.h"
#include "spdlog/spdlog.h"

constexpr int64_t batch_rows = 1U << 16U;

namespace memgraph::query {

class BatchIterator {
 public:
  BatchIterator() = default;
  explicit BatchIterator(std::unique_ptr<arrow::RecordBatchReader> rbr, int const num_columns)
      : num_columns_(num_columns), rbr_(std::move(rbr)) {}
  ~BatchIterator() = default;

  BatchIterator(const BatchIterator &) = delete;
  BatchIterator &operator=(const BatchIterator &) = delete;
  BatchIterator(BatchIterator &&) = delete;
  BatchIterator &operator=(BatchIterator &&) = delete;

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
  std::unique_ptr<arrow::RecordBatchReader> rbr_;
};

struct ParquetReader::impl {
  explicit impl(std::unique_ptr<parquet::arrow::FileReader> file_reader, std::unique_ptr<arrow::RecordBatchReader> rbr);
  ~impl();

  impl(impl const &other) = delete;
  impl &operator=(impl const &) = delete;

  impl(impl &&other) = delete;
  impl &operator=(impl &&other) = delete;

  auto GetNextRow(Row &out) -> bool;

  auto GetSchema() -> std::shared_ptr<arrow::Schema>;

 private:
  // Needs to stay alive because of batch reader
  std::unique_ptr<parquet::arrow::FileReader> file_reader_;
  std::shared_ptr<arrow::Schema> schema_;
  int num_columns_;
  BatchIterator row_it_;
  std::vector<Row> rows_;  // cached, pre-allocated rows
  utils::DataQueue<std::vector<Row>> work_queue_;
  std::jthread prefetcher_thread_;  // should get destroyed before all other variables that it uses as a reference
  uint64_t row_in_batch_{0};
  uint64_t current_batch_size_{0};
};

ParquetReader::impl::impl(std::unique_ptr<parquet::arrow::FileReader> file_reader,
                          std::unique_ptr<arrow::RecordBatchReader> rbr)
    : file_reader_(std::move(file_reader)),
      schema_(rbr->schema()),
      num_columns_(schema_->num_fields()),
      row_it_(BatchIterator(std::move(rbr), num_columns_)),
      work_queue_(2),
      prefetcher_thread_{[this]() {
        while (true) {
          auto const batch_ref = row_it_.Next();
          // No more data
          if (batch_ref.empty()) {
            work_queue_.finish();
            break;
          }

          auto const num_rows = batch_ref[0]->length();
          std::vector<Row> queued_batch;
          queued_batch.resize(num_rows);
          for (int i = 0; i < num_rows; ++i) {
            queued_batch[i].resize(num_columns_);
          }

          // TODO: (andi) Probably the best way is to expose the iterator function and you can check then only once
          // if the value is null
          for (int j = 0U; j < num_columns_; j++) {
            auto const &column = batch_ref[j];

            if (auto const type_id = column->type_id(); type_id == arrow::Type::INT64) {
              auto const int_array = std::static_pointer_cast<arrow::Int64Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(int_array->Value(i));
              }
            } else if (type_id == arrow::Type::INT32) {
              auto const int32_array = std::static_pointer_cast<arrow::Int32Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(int32_array->Value(i));
              }
            } else if (type_id == arrow::Type::INT16) {
              auto const int16_array = std::static_pointer_cast<arrow::Int16Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(int16_array->Value(i));
              }
            } else if (type_id == arrow::Type::INT8) {
              auto const int8_array = std::static_pointer_cast<arrow::Int8Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(int8_array->Value(i));
              }
            } else if (type_id == arrow::Type::UINT8) {
              auto const uint8_array = std::static_pointer_cast<arrow::UInt8Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(uint8_array->Value(i));
              }
            } else if (type_id == arrow::Type::UINT16) {
              auto const uint16_array = std::static_pointer_cast<arrow::UInt16Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(uint16_array->Value(i));
              }
            } else if (type_id == arrow::Type::UINT32) {
              auto const uint32_array = std::static_pointer_cast<arrow::UInt32Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(static_cast<int64_t>(uint32_array->Value(i)));
              }
            } else if (type_id == arrow::Type::UINT64) {
              auto const uint64_array = std::static_pointer_cast<arrow::UInt64Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(static_cast<int64_t>(uint64_array->Value(i)));
              }
            } else if (type_id == arrow::Type::FLOAT) {
              auto const float_array = std::static_pointer_cast<arrow::FloatArray>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(float_array->Value(i));
              }
            } else if (type_id == arrow::Type::HALF_FLOAT) {
              auto const half_float_array = std::static_pointer_cast<arrow::HalfFloatArray>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                auto scalar = half_float_array->Value(i);
                queued_batch[i][j] = TypedValue(arrow::util::Float16::FromBits(scalar).ToFloat());
              }
            } else if (type_id == arrow::Type::DOUBLE) {
              auto const double_array = std::static_pointer_cast<arrow::DoubleArray>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(double_array->Value(i));
              }
            } else if (type_id == arrow::Type::BOOL) {
              auto const bool_array = std::static_pointer_cast<arrow::BooleanArray>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(bool_array->Value(i));
              }
            } else if (type_id == arrow::Type::STRING) {
              auto const string_array = std::static_pointer_cast<arrow::StringArray>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(string_array->GetString(i));
              }
            } else if (type_id == arrow::Type::DATE32) {
              auto const date_array = std::static_pointer_cast<arrow::Date32Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(utils::Date{date_array->Value(i)});
              }
            } else if (type_id == arrow::Type::DATE64) {
              auto const date_array = std::static_pointer_cast<arrow::Date64Array>(column);
              for (int64_t i = 0; i < num_rows; i++) {
                auto const ms = std::chrono::milliseconds(date_array->Value(i));
                auto const us = std::chrono::duration_cast<std::chrono::microseconds>(ms);
                queued_batch[i][j] = TypedValue(utils::Date{us.count()});
              }
            } else if (type_id == arrow::Type::TIME32) {
              auto const time32_array = std::static_pointer_cast<arrow::Time32Array>(column);
              switch (auto time_type = std::static_pointer_cast<arrow::Time32Type>(column->type()); time_type->unit()) {
                case arrow::TimeUnit::MILLI: {
                  for (int64_t i = 0; i < num_rows; i++) {
                    auto const val = time32_array->Value(i);
                    auto const ms = std::chrono::milliseconds(val);
                    queued_batch[i][j] =
                        TypedValue(utils::LocalTime(std::chrono::duration_cast<std::chrono::microseconds>(ms).count()));
                  }
                  break;
                }
                case arrow::TimeUnit::SECOND: {
                  for (int64_t i = 0; i < num_rows; i++) {
                    auto const val = time32_array->Value(i);
                    auto const secs = std::chrono::seconds(val);
                    queued_batch[i][j] = TypedValue(
                        utils::LocalTime(std::chrono::duration_cast<std::chrono::microseconds>(secs).count()));
                  }
                  break;
                }
                default: {
                  throw std::invalid_argument(
                      "Unsupported time unit. TIME32 should only support seconds and milliseconds");
                }
              }
            } else if (type_id == arrow::Type::TIME64) {
              auto const time64_array = std::static_pointer_cast<arrow::Time64Array>(column);
              switch (auto time_type = std::static_pointer_cast<arrow::Time64Type>(column->type()); time_type->unit()) {
                case arrow::TimeUnit::MICRO: {
                  for (int64_t i = 0; i < num_rows; i++) {
                    auto const us = time64_array->Value(i);
                    queued_batch[i][j] = TypedValue(utils::LocalTime(us));
                  }
                  break;
                }
                case arrow::TimeUnit::NANO: {
                  for (int64_t i = 0; i < num_rows; i++) {
                    auto const ns = std::chrono::nanoseconds(time64_array->Value(i));
                    queued_batch[i][j] =
                        TypedValue(utils::LocalTime(std::chrono::duration_cast<std::chrono::microseconds>(ns).count()));
                  }
                  break;
                }
                default: {
                  throw std::invalid_argument(
                      "Unsupported time unit. TIME64 should only support microseconds and nanoseconds");
                }
              }

            } else if (type_id == arrow::Type::TIMESTAMP) {
              auto const timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(column);
              // TODO: (andi) Convert to microseconds through handler and then extract for loop
              switch (auto time_type = std::static_pointer_cast<arrow::TimestampType>(column->type());
                      time_type->unit()) {
                case arrow::TimeUnit::SECOND: {
                  for (int64_t i = 0; i < num_rows; i++) {
                    auto const ms = std::chrono::seconds(timestamp_array->Value(i));
                    auto const us = std::chrono::duration_cast<std::chrono::microseconds>(ms).count();
                    queued_batch[i][j] = TypedValue(utils::LocalDateTime(us));
                  }
                  break;
                }
                case arrow::TimeUnit::MILLI: {
                  for (int64_t i = 0; i < num_rows; i++) {
                    auto const ms = std::chrono::milliseconds(timestamp_array->Value(i));
                    auto const us = std::chrono::duration_cast<std::chrono::microseconds>(ms).count();
                    queued_batch[i][j] = TypedValue(utils::LocalDateTime(us));
                  }
                  break;
                }
                case arrow::TimeUnit::MICRO: {
                  for (int64_t i = 0; i < num_rows; i++) {
                    auto const us = timestamp_array->Value(i);
                    queued_batch[i][j] = TypedValue(utils::LocalDateTime(us));
                  }
                  break;
                }
                case arrow::TimeUnit::NANO: {
                  for (int64_t i = 0; i < num_rows; i++) {
                    auto const ns = std::chrono::nanoseconds(timestamp_array->Value(i));
                    auto const us = std::chrono::duration_cast<std::chrono::microseconds>(ns).count();
                    queued_batch[i][j] = TypedValue(utils::LocalDateTime(us));
                  }
                  break;
                }
                default:
                  throw std::invalid_argument("Unsupported time unit when reading Timestamp info");
              }
            }

            else {
              // Convert unsupported types (dates, timestamps, etc.) to string
              for (int64_t i = 0; i < num_rows; i++) {
                queued_batch[i][j] = TypedValue(column->GetScalar(i).ValueOrDie()->ToString());
              }
            }
          }
          work_queue_.push(std::move(queued_batch));
        }
      }}

{
  // Preallocate
  rows_.resize(batch_rows);
  for (int i = 0; i < batch_rows; ++i) {
    rows_[i].resize(num_columns_);
  }
}

ParquetReader::impl::~impl() {}

auto ParquetReader::impl::GetNextRow(Row &out) -> bool {
  if (row_in_batch_ >= current_batch_size_) {
    // No more batches to process
    if (!work_queue_.pop(rows_)) {
      return false;
    }

    row_in_batch_ = 0;
    current_batch_size_ = rows_.size();
  }
  out = std::move(rows_[row_in_batch_++]);
  return true;
}

auto ParquetReader::impl::GetSchema() -> std::shared_ptr<arrow::Schema> { return schema_; }

ParquetReader::ParquetReader(std::string const &file) {
  auto const start = std::chrono::high_resolution_clock::now();
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  try {
    // Open the file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(file, pool));

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

    pimpl_ = std::make_unique<impl>(std::move(arrow_reader), std::move(*res));
  } catch (const std::exception &e) {
    spdlog::error("Failed to open parquet file '{}': {}", file, e.what());
    throw;
  }

  auto const duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - start);
  spdlog::trace("Time spent on initializing parquet reader: {}ms", duration.count());
}

ParquetReader::~ParquetReader() = default;

auto ParquetReader::GetNextRow(Row &out) -> bool { return pimpl_->GetNextRow(out); }

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
