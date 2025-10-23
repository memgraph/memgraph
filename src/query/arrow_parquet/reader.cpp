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
#include "arrow/util/decimal.h"
#include "arrow/util/float16.h"
#include "parquet/properties.h"
#include "spdlog/spdlog.h"

constexpr int64_t batch_rows = 1U << 16U;

using memgraph::query::TypedValue;
using memgraph::utils::Date;
using memgraph::utils::Duration;
using memgraph::utils::LocalDateTime;
using memgraph::utils::LocalTime;
using memgraph::utils::MemoryResource;

namespace {
auto ToHexString(const uint8_t *data, size_t const size) -> std::string {
  std::string hex;
  hex.reserve(size * 2);
  for (size_t i = 0; i < size; i++) {
    constexpr auto *hex_chars = "0123456789abcdef";
    hex.push_back(hex_chars[data[i] >> 4U]);
    hex.push_back(hex_chars[data[i] & 0x0FU]);
  }
  return hex;
}

// Return to microseconds
auto ArrowTimeToUs(auto const arrow_val, auto const arrow_time_unit) -> int64_t {
  switch (arrow_time_unit) {
    case arrow::TimeUnit::MICRO: {
      return arrow_val;
    }
    case arrow::TimeUnit::NANO: {
      auto const ns = std::chrono::nanoseconds(arrow_val);
      return std::chrono::duration_cast<std::chrono::microseconds>(ns).count();
    }
    case arrow::TimeUnit::MILLI: {
      auto const ms = std::chrono::milliseconds(arrow_val);
      return std::chrono::duration_cast<std::chrono::microseconds>(ms).count();
    }
    case arrow::TimeUnit::SECOND: {
      auto const secs = std::chrono::seconds(arrow_val);
      return std::chrono::duration_cast<std::chrono::microseconds>(secs).count();
    }
    default: {
      throw std::invalid_argument("Unsupported time unit. TIME32 should only support seconds and milliseconds");
    }
  }
}

std::function<TypedValue(int64_t)> CreateColumnConverter(const std::shared_ptr<arrow::Array> &column,
                                                         MemoryResource *resource) {
  TypedValue::allocator_type alloc(resource);
  switch (column->type()->id()) {
    case arrow::Type::BOOL: {
      auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(column);
      return [bool_array, alloc](int64_t const i) -> TypedValue {
        return bool_array->IsNull(i) ? TypedValue(alloc) : TypedValue(bool_array->Value(i), alloc);
      };
    }
    case arrow::Type::INT8: {
      auto int_array = std::static_pointer_cast<arrow::Int8Array>(column);
      return [int_array, alloc](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(alloc) : TypedValue(static_cast<int64_t>(int_array->Value(i)), alloc);
      };
    }
    case arrow::Type::INT16: {
      auto int_array = std::static_pointer_cast<arrow::Int16Array>(column);
      return [int_array, alloc](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(alloc) : TypedValue(static_cast<int64_t>(int_array->Value(i)), alloc);
      };
    }
    case arrow::Type::INT32: {
      auto int_array = std::static_pointer_cast<arrow::Int32Array>(column);
      return [int_array, alloc](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(alloc) : TypedValue(int_array->Value(i), alloc);
      };
    }
    case arrow::Type::INT64: {
      auto int_array = std::static_pointer_cast<arrow::Int64Array>(column);
      return [int_array, alloc](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(alloc) : TypedValue(int_array->Value(i), alloc);
      };
    }
    case arrow::Type::UINT8: {
      auto int_array = std::static_pointer_cast<arrow::UInt8Array>(column);
      return [int_array, alloc](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(alloc) : TypedValue(static_cast<int64_t>(int_array->Value(i)), alloc);
      };
    }
    case arrow::Type::UINT16: {
      auto int_array = std::static_pointer_cast<arrow::UInt16Array>(column);
      return [int_array, alloc](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(alloc) : TypedValue(static_cast<int64_t>(int_array->Value(i)), alloc);
      };
    }
    case arrow::Type::UINT32: {
      auto int_array = std::static_pointer_cast<arrow::UInt32Array>(column);
      return [int_array, alloc](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(alloc) : TypedValue(static_cast<int64_t>(int_array->Value(i)), alloc);
      };
    }
    case arrow::Type::UINT64: {
      auto int_array = std::static_pointer_cast<arrow::UInt64Array>(column);
      return [int_array, alloc](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(alloc) : TypedValue(static_cast<int64_t>(int_array->Value(i)), alloc);
      };
    }
    case arrow::Type::HALF_FLOAT: {
      auto half_float_array = std::static_pointer_cast<arrow::HalfFloatArray>(column);
      return [half_float_array, alloc](int64_t const i) -> TypedValue {
        if (half_float_array->IsNull(i)) return TypedValue(alloc);
        auto scalar = half_float_array->Value(i);
        return TypedValue(arrow::util::Float16::FromBits(scalar).ToFloat(), alloc);
      };
    }
    case arrow::Type::FLOAT: {
      auto float_array = std::static_pointer_cast<arrow::FloatArray>(column);
      return [float_array, alloc](int64_t const i) -> TypedValue {
        return float_array->IsNull(i) ? TypedValue(alloc) : TypedValue(float_array->Value(i), alloc);
      };
    }
    case arrow::Type::DOUBLE: {
      auto double_array = std::static_pointer_cast<arrow::DoubleArray>(column);
      return [double_array, alloc](int64_t const i) -> TypedValue {
        return double_array->IsNull(i) ? TypedValue(alloc) : TypedValue(double_array->Value(i), alloc);
      };
    }
    case arrow::Type::STRING: {
      auto string_array = std::static_pointer_cast<arrow::StringArray>(column);
      return [string_array, alloc](int64_t const i) -> TypedValue {
        if (string_array->IsNull(i)) return TypedValue(alloc);
        auto str = string_array->GetString(i);
        return TypedValue(TypedValue::TString{str, alloc}, alloc);
      };
    }
    case arrow::Type::LARGE_STRING: {
      auto large_string_array = std::static_pointer_cast<arrow::LargeStringArray>(column);
      return [large_string_array, alloc](int64_t const i) -> TypedValue {
        if (large_string_array->IsNull(i)) return TypedValue(alloc);
        auto view = large_string_array->GetView(i);
        return TypedValue(TypedValue::TString{view, alloc}, alloc);
      };
    }
    case arrow::Type::STRING_VIEW: {
      auto string_view_array = std::static_pointer_cast<arrow::StringViewArray>(column);
      return [string_view_array, alloc](int64_t const i) -> TypedValue {
        if (string_view_array->IsNull(i)) return TypedValue(alloc);
        auto view = string_view_array->GetView(i);
        return TypedValue(TypedValue::TString{view, alloc}, alloc);
      };
    }
    case arrow::Type::DATE32: {
      auto date_array = std::static_pointer_cast<arrow::Date32Array>(column);
      return [date_array, alloc](int64_t const i) -> TypedValue {
        return date_array->IsNull(i) ? TypedValue(alloc) : TypedValue(Date{date_array->Value(i)}, alloc);
      };
    }
    case arrow::Type::DATE64: {
      auto date_array = std::static_pointer_cast<arrow::Date64Array>(column);
      return [date_array, alloc](int64_t const i) -> TypedValue {
        if (date_array->IsNull(i)) return TypedValue(alloc);
        auto const ms = std::chrono::milliseconds(date_array->Value(i));
        auto const us = std::chrono::duration_cast<std::chrono::microseconds>(ms);
        return TypedValue(Date{us.count()}, alloc);
      };
    }
    case arrow::Type::TIME32: {
      auto time_array = std::static_pointer_cast<arrow::Time32Array>(column);
      auto time_type = std::static_pointer_cast<arrow::Time32Type>(column->type());
      return [time_array, unit = time_type->unit(), alloc](int64_t const i) -> TypedValue {
        if (time_array->IsNull(i)) return TypedValue(alloc);
        auto const arrow_val = time_array->Value(i);
        auto const us_val = ArrowTimeToUs(arrow_val, unit);
        return TypedValue(LocalTime{us_val}, alloc);
      };
    }
    case arrow::Type::TIME64: {
      auto time_array = std::static_pointer_cast<arrow::Time64Array>(column);
      auto time_type = std::static_pointer_cast<arrow::Time64Type>(column->type());
      return [time_array, unit = time_type->unit(), alloc](int64_t const i) -> TypedValue {
        if (time_array->IsNull(i)) return TypedValue(alloc);
        auto const arrow_val = time_array->Value(i);
        auto const us_val = ArrowTimeToUs(arrow_val, unit);
        return TypedValue(LocalTime{us_val}, alloc);
      };
    }
    case arrow::Type::TIMESTAMP: {
      auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(column);
      auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(column->type());
      return [timestamp_array, unit = timestamp_type->unit(), alloc](int64_t const i) -> TypedValue {
        if (timestamp_array->IsNull(i)) return TypedValue(alloc);
        auto const arrow_val = timestamp_array->Value(i);
        auto const us_val = ArrowTimeToUs(arrow_val, unit);
        return TypedValue(LocalDateTime{us_val}, alloc);
      };
    }
    case arrow::Type::DURATION: {
      auto duration_array = std::static_pointer_cast<arrow::DurationArray>(column);
      auto duration_type = std::static_pointer_cast<arrow::DurationType>(column->type());
      return [duration_array, unit = duration_type->unit(), alloc](int64_t const i) -> TypedValue {
        if (duration_array->IsNull(i)) return TypedValue(alloc);
        auto const arrow_val = duration_array->Value(i);
        auto const us_val = ArrowTimeToUs(arrow_val, unit);
        return TypedValue(Duration{us_val}, alloc);
      };
    }
    case arrow::Type::DECIMAL128: {
      auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(column);
      auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(column->type());
      int32_t const scale = decimal_type->scale();
      return [decimal_array, scale, alloc](int64_t const i) -> TypedValue {
        if (decimal_array->IsNull(i)) return TypedValue(alloc);
        uint8_t const *bytes = decimal_array->GetValue(i);
        arrow::Decimal128 const value(bytes);
        return TypedValue(value.ToDouble(scale), alloc);
      };
    }
    case arrow::Type::DECIMAL256: {
      auto decimal_array = std::static_pointer_cast<arrow::Decimal256Array>(column);
      auto decimal_type = std::static_pointer_cast<arrow::Decimal256Type>(column->type());
      int32_t const scale = decimal_type->scale();
      return [decimal_array, scale, alloc](int64_t const i) -> TypedValue {
        if (decimal_array->IsNull(i)) return TypedValue(alloc);
        uint8_t const *bytes = decimal_array->GetValue(i);
        arrow::Decimal256 const value(bytes);
        return TypedValue(value.ToDouble(scale), alloc);
      };
    }
    case arrow::Type::BINARY: {
      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(column);
      return [binary_array, alloc](int64_t const i) -> TypedValue {
        if (binary_array->IsNull(i)) return TypedValue(alloc);
        auto const view = binary_array->GetView(i);
        return TypedValue(ToHexString(reinterpret_cast<const uint8_t *>(view.data()), view.size()), alloc);
      };
    }
    case arrow::Type::LARGE_BINARY: {
      auto large_binary_array = std::static_pointer_cast<arrow::LargeBinaryArray>(column);
      return [large_binary_array, alloc](int64_t const i) -> TypedValue {
        if (large_binary_array->IsNull(i)) return TypedValue(alloc);
        auto const view = large_binary_array->GetView(i);
        return TypedValue(ToHexString(reinterpret_cast<const uint8_t *>(view.data()), view.size()), alloc);
      };
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
      auto fixed_binary = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
      int32_t const width = fixed_binary->byte_width();
      return [fixed_binary, width, alloc](int64_t const i) -> TypedValue {
        if (fixed_binary->IsNull(i)) return TypedValue(alloc);
        const uint8_t *data = fixed_binary->GetValue(i);
        return TypedValue(ToHexString(data, width), alloc);
      };
    }
    case arrow::Type::LIST: {
      auto list_array = std::static_pointer_cast<arrow::ListArray>(column);
      return [list_array, resource](int64_t const i) -> TypedValue {
        if (list_array->IsNull(i)) return TypedValue(resource);

        auto const slice = list_array->value_slice(i);
        int64_t const list_length = slice->length();

        TypedValue::TVector list_values(resource);
        list_values.reserve(list_length);

        for (int64_t k = 0; k < list_length; k++) {
          auto const elem_converter = CreateColumnConverter(slice, resource);
          list_values.emplace_back(elem_converter(k));
        }

        return TypedValue(std::move(list_values), resource);
      };
    }
    case arrow::Type::MAP: {
      auto map_array = std::static_pointer_cast<arrow::MapArray>(column);
      auto keys_array = map_array->keys();
      auto values_array = map_array->items();
      auto key_strings = std::static_pointer_cast<arrow::StringArray>(keys_array);

      return [map_array, key_strings, values_array, resource](int64_t const i) -> TypedValue {
        if (map_array->IsNull(i)) return TypedValue(resource);

        int64_t const offset_start = map_array->value_offset(i);
        int64_t const offset_end = map_array->value_offset(i + 1);

        TypedValue::TMap map_values(resource);

        for (int64_t k = offset_start; k < offset_end; k++) {
          auto key = key_strings->GetString(k);
          auto const val_converter = CreateColumnConverter(values_array, resource);
          map_values.emplace(TypedValue::TString{key, resource}, val_converter(k));
        }

        return TypedValue(std::move(map_values), resource);
      };
    }
    default: {
      // Fallback to string conversion
      return [column, alloc](int64_t const i) -> TypedValue {
        if (column->IsNull(i)) return TypedValue(alloc);
        if (auto scalar = column->GetScalar(i); scalar.ok() && scalar.ValueOrDie()) {
          return TypedValue(scalar.ValueOrDie()->ToString(), alloc);
        }
        return {};
      };
    }
  }
}

}  // namespace

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
  explicit impl(std::unique_ptr<parquet::arrow::FileReader> file_reader, std::unique_ptr<arrow::RecordBatchReader> rbr,
                Header header, utils::MemoryResource *resource);
  ~impl();

  impl(impl const &other) = delete;
  impl &operator=(impl const &) = delete;

  impl(impl &&other) = delete;
  impl &operator=(impl &&other) = delete;

  auto GetNextRow(Row &out) -> bool;

 private:
  // Needs to stay alive because of batch reader
  std::unique_ptr<parquet::arrow::FileReader> file_reader_;
  std::shared_ptr<arrow::Schema> schema_;
  int num_columns_;
  BatchIterator row_it_;
  utils::MemoryResource *resource_;
  utils::pmr::vector<Row> rows_;  // cached, pre-allocated rows
  utils::DataQueue<utils::pmr::vector<Row>> work_queue_;
  Header header_;
  std::jthread prefetcher_thread_;  // should get destroyed before all other variables that it uses as a reference
  uint64_t row_in_batch_{0};
  uint64_t current_batch_size_{0};
};

ParquetReader::impl::impl(std::unique_ptr<parquet::arrow::FileReader> file_reader,
                          std::unique_ptr<arrow::RecordBatchReader> rbr, Header header, utils::MemoryResource *resource)
    : file_reader_(std::move(file_reader)),
      schema_(rbr->schema()),
      num_columns_(schema_->num_fields()),
      row_it_(BatchIterator(std::move(rbr), num_columns_)),
      resource_(resource),
      rows_(resource),
      work_queue_(2),
      header_(std::move(header)),
      prefetcher_thread_{[this]() {
        while (true) {
          auto const batch_ref = row_it_.Next();
          // No more data
          if (batch_ref.empty()) {
            work_queue_.finish();
            break;
          }

          auto const num_rows = batch_ref[0]->length();
          utils::pmr::vector<Row> queued_batch(resource_);
          queued_batch.reserve(num_rows);
          for (auto i = 0; i < num_rows; ++i) {
            queued_batch.push_back(Row{resource_});
          }

          std::vector<std::function<TypedValue(int64_t)>> converters;
          converters.reserve(num_columns_);
          for (int j = 0U; j < num_columns_; j++) {
            converters.push_back(CreateColumnConverter(batch_ref[j], resource_));
          }

          for (int j = 0U; j < num_columns_; j++) {
            auto const &converter = converters[j];
            for (int64_t i = 0; i < num_rows; i++) {
              queued_batch[i].emplace(header_[j], converter(i));  // RVO should kick in here
            }
          }
          work_queue_.push(std::move(queued_batch));
        }
      }}

{
  // Preallocate
  rows_.reserve(batch_rows);
  for (auto i = 0U; i < batch_rows; i++) {
    rows_.push_back(Row{resource_});
  }
}

ParquetReader::impl::~impl() {}

auto ParquetReader::impl::GetNextRow(Row &out) -> bool {
  if (row_in_batch_ >= current_batch_size_) {
    if (!work_queue_.pop(rows_)) {
      return false;
    }

    row_in_batch_ = 0;
    current_batch_size_ = rows_.size();
  }
  std::swap(out, rows_[row_in_batch_++]);
  // out = std::move(rows_[row_in_batch_++]);
  return true;
}

ParquetReader::ParquetReader(std::string const &file, utils::MemoryResource *resource) {
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

    auto const schema = (*res)->schema();
    Header header(resource);
    header.reserve(schema->num_fields());
    for (auto const &field : schema->fields()) {
      header.push_back(TypedValue::TString{field->name(), resource});
    }

    pimpl_ = std::make_unique<impl>(std::move(arrow_reader), std::move(*res), std::move(header), resource);
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

}  // namespace memgraph::query
