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

module;

#include "flags/run_time_configurable.hpp"
#include "query/typed_value.hpp"
#include "requests/requests.hpp"
#include "utils/data_queue.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/temporal.hpp"

#include <chrono>
#include <string_view>
#include <thread>

#include "arrow/api.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/io/file.h"
#include "arrow/util/decimal.h"
#include "arrow/util/float16.h"
#include "ctre.hpp"
#include "parquet/arrow/reader.h"
#include "parquet/properties.h"
#include "spdlog/spdlog.h"

module memgraph.query.arrow_parquet.reader;

constexpr int64_t batch_rows = 1U << 16U;

using memgraph::query::TypedValue;
using memgraph::utils::Date;
using memgraph::utils::Duration;
using memgraph::utils::LocalDateTime;
using memgraph::utils::LocalTime;
using memgraph::utils::MemoryResource;
using namespace std::string_view_literals;

namespace {

constexpr std::string_view s3_prefix = "s3://";
constexpr auto kAwsAccessKeyEnv = "AWS_ACCESS_KEY";
constexpr auto kAwsRegionEnv = "AWS_REGION";
constexpr auto kAwsSecretKeyEnv = "AWS_SECRET_KEY";
constexpr auto kAwsEndpointUrlEnv = "AWS_ENDPOINT_URL";

class GlobalS3APIManager {
 public:
  GlobalS3APIManager(const GlobalS3APIManager &) = delete;
  GlobalS3APIManager(GlobalS3APIManager &&) = delete;
  GlobalS3APIManager &operator=(const GlobalS3APIManager &) = delete;
  GlobalS3APIManager &operator=(GlobalS3APIManager &&) = delete;

  static GlobalS3APIManager &GetInstance() {
    static GlobalS3APIManager instance;
    return instance;
  }

 private:
  GlobalS3APIManager() {
    if (auto const status = arrow::fs::EnsureS3Initialized(); !status.ok()) {
      spdlog::error("Failed to initialize S3 file system: {}", status.message());
      std::exit(1);
    }
  }

  ~GlobalS3APIManager() {
    if (arrow::fs::IsS3Initialized()) {
      if (auto const finalize_status = arrow::fs::FinalizeS3(); !finalize_status.ok()) {
        spdlog::error("Failed to finalize S3 file system");
      }
    }
  }
};

auto BuildS3Config(memgraph::query::ParquetFileConfig &config) -> memgraph::query::ParquetFileConfig {
  auto get_env_or_empty = [](const char *env_name) -> std::optional<std::string> {
    if (const auto *const env_val = std::getenv(env_name)) {
      return std::string{env_val};
    }
    return std::nullopt;
  };

  config.aws_region = config.aws_region
                          .or_else([&] {
                            auto setting = memgraph::flags::run_time::GetAwsRegion();
                            return setting.empty() ? std::nullopt : std::make_optional(std::move(setting));
                          })
                          .or_else([&] { return get_env_or_empty(kAwsRegionEnv); });

  config.aws_access_key = config.aws_access_key
                              .or_else([&] {
                                auto setting = memgraph::flags::run_time::GetAwsAccessKey();
                                return setting.empty() ? std::nullopt : std::make_optional(std::move(setting));
                              })
                              .or_else([&] { return get_env_or_empty(kAwsAccessKeyEnv); });

  config.aws_secret_key = config.aws_secret_key
                              .or_else([&] {
                                auto setting = memgraph::flags::run_time::GetAwsSecretKey();
                                return setting.empty() ? std::nullopt : std::make_optional(std::move(setting));
                              })
                              .or_else([&] { return get_env_or_empty(kAwsSecretKeyEnv); });

  config.aws_endpoint_url = config.aws_endpoint_url
                                .or_else([&] {
                                  auto setting = memgraph::flags::run_time::GetAwsEndpointUrl();
                                  return setting.empty() ? std::nullopt : std::make_optional(std::move(setting));
                                })
                                .or_else([&] { return get_env_or_empty(kAwsEndpointUrlEnv); });

  return config;
}

auto BuildHeader(std::shared_ptr<arrow::Schema> const &schema, memgraph::utils::MemoryResource *resource)
    -> memgraph::query::Header {
  memgraph::query::Header header(resource);
  header.reserve(schema->num_fields());
  for (auto const &field : schema->fields()) {
    // temporary needs to be created
    // NOLINTNEXTLINE
    header.push_back(TypedValue::TString{field->name(), resource});
  }
  return header;
}

// nullptr for error
auto LoadFileFromS3(memgraph::query::ParquetFileConfig const &s3_config)
    -> std::unique_ptr<parquet::arrow::FileReader> {
  GlobalS3APIManager::GetInstance();

  // Users needs to set aws_region, aws_access_key and aws_secret_key in some way. aws_endpoint_url is optional
  if (!s3_config.aws_region.has_value()) {
    spdlog::error(
        "AWS region configuration parameter not provided. Please provide it through the query, run-time setting {} or "
        "env variable {}",
        memgraph::query::kAwsRegionQuerySetting, kAwsRegionEnv);
    return nullptr;
  }

  if (!s3_config.aws_access_key.has_value()) {
    spdlog::error(
        "AWS access key configuration parameter not provided. Please provide it through the query, run-time setting {} "
        "or env variable {}",
        memgraph::query::kAwsAccessKeyQuerySetting, kAwsAccessKeyEnv);
    return nullptr;
  }

  if (!s3_config.aws_secret_key.has_value()) {
    spdlog::error(
        "AWS secret key configuration parameter not provided. Please provide it through the query, run-time setting {} "
        "or env variable {}",
        memgraph::query::kAwsSecretKeyQuerySetting, kAwsSecretKeyEnv);
    return nullptr;
  }

  auto s3_options = arrow::fs::S3Options::FromAccessKey(*s3_config.aws_access_key, *s3_config.aws_secret_key);
  s3_options.region = *s3_config.aws_region;
  // aws_endpoint_url is optional, by default it will try to pull from the real S3
  if (s3_config.aws_endpoint_url.has_value()) {
    s3_options.endpoint_override = *s3_config.aws_endpoint_url;
  }

  auto maybe_s3_fs = arrow::fs::S3FileSystem::Make(s3_options);
  if (!maybe_s3_fs.ok()) {
    spdlog::error(maybe_s3_fs.status().message());
    return nullptr;
  }

  auto const &s3_fs = *maybe_s3_fs;

  auto const uri_wo_prefix = s3_config.file.substr(s3_prefix.size());
  auto rnd_acc_file = s3_fs->OpenInputFile(uri_wo_prefix);
  if (!rnd_acc_file.ok()) {
    spdlog::error(rnd_acc_file.status().message());
    return nullptr;
  }

  auto maybe_parquet_reader = parquet::arrow::OpenFile(*rnd_acc_file, arrow::default_memory_pool());

  if (!maybe_parquet_reader.ok()) {
    spdlog::error(maybe_parquet_reader.status().message());
    return nullptr;
  }

  return std::move(*maybe_parquet_reader);
}

// nullptr for error
auto LoadFileFromDisk(std::string const &file_path) -> std::unique_ptr<parquet::arrow::FileReader> {
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  auto maybe_file = arrow::io::ReadableFile::Open(file_path, pool);
  if (!maybe_file.ok()) {
    spdlog::error(maybe_file.status().message());
    return nullptr;
  }

  auto const &file = *maybe_file;

  auto reader_properties = parquet::ReaderProperties(pool);
  reader_properties.enable_buffered_stream();

  auto arrow_reader_props = parquet::ArrowReaderProperties();
  arrow_reader_props.set_batch_size(batch_rows);
  arrow_reader_props.set_use_threads(true);

  parquet::arrow::FileReaderBuilder reader_builder;

  if (auto const status = reader_builder.Open(file, reader_properties); !status.ok()) {
    spdlog::error(status.message());
    return nullptr;
  }

  reader_builder.memory_pool(pool);
  reader_builder.properties(arrow_reader_props);

  std::unique_ptr<parquet::arrow::FileReader> file_reader;
  if (auto const status = reader_builder.Build(&file_reader); !status.ok()) {
    spdlog::error(status.message());
    return nullptr;
  }

  return file_reader;
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
  switch (column->type()->id()) {
    case arrow::Type::BOOL: {
      auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(column);
      return [bool_array, resource](int64_t const i) -> TypedValue {
        return bool_array->IsNull(i) ? TypedValue(resource) : TypedValue(bool_array->Value(i), resource);
      };
    }
    case arrow::Type::INT8: {
      auto int_array = std::static_pointer_cast<arrow::Int8Array>(column);
      return [int_array, resource](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(resource) : TypedValue(int_array->Value(i), resource);
      };
    }
    case arrow::Type::INT16: {
      auto int_array = std::static_pointer_cast<arrow::Int16Array>(column);
      return [int_array, resource](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(resource) : TypedValue(int_array->Value(i), resource);
      };
    }
    case arrow::Type::INT32: {
      auto int_array = std::static_pointer_cast<arrow::Int32Array>(column);
      return [int_array, resource](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(resource) : TypedValue(int_array->Value(i), resource);
      };
    }
    case arrow::Type::INT64: {
      auto int_array = std::static_pointer_cast<arrow::Int64Array>(column);
      return [int_array, resource](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(resource) : TypedValue(int_array->Value(i), resource);
      };
    }
    case arrow::Type::UINT8: {
      auto int_array = std::static_pointer_cast<arrow::UInt8Array>(column);
      return [int_array, resource](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(resource) : TypedValue(int_array->Value(i), resource);
      };
    }
    case arrow::Type::UINT16: {
      auto int_array = std::static_pointer_cast<arrow::UInt16Array>(column);
      return [int_array, resource](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(resource) : TypedValue(int_array->Value(i), resource);
      };
    }
    case arrow::Type::UINT32: {
      auto int_array = std::static_pointer_cast<arrow::UInt32Array>(column);
      return [int_array, resource](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(resource)
                                    : TypedValue(static_cast<int64_t>(int_array->Value(i)), resource);
      };
    }
    case arrow::Type::UINT64: {
      auto int_array = std::static_pointer_cast<arrow::UInt64Array>(column);
      return [int_array, resource](int64_t const i) -> TypedValue {
        return int_array->IsNull(i) ? TypedValue(resource)
                                    : TypedValue(static_cast<int64_t>(int_array->Value(i)), resource);
      };
    }
    case arrow::Type::HALF_FLOAT: {
      auto half_float_array = std::static_pointer_cast<arrow::HalfFloatArray>(column);
      return [half_float_array, resource](int64_t const i) -> TypedValue {
        if (half_float_array->IsNull(i)) return TypedValue(resource);
        auto scalar = half_float_array->Value(i);
        return TypedValue(arrow::util::Float16::FromBits(scalar).ToFloat(), resource);
      };
    }
    case arrow::Type::FLOAT: {
      auto float_array = std::static_pointer_cast<arrow::FloatArray>(column);
      return [float_array, resource](int64_t const i) -> TypedValue {
        return float_array->IsNull(i) ? TypedValue(resource) : TypedValue(float_array->Value(i), resource);
      };
    }
    case arrow::Type::DOUBLE: {
      auto double_array = std::static_pointer_cast<arrow::DoubleArray>(column);
      return [double_array, resource](int64_t const i) -> TypedValue {
        return double_array->IsNull(i) ? TypedValue(resource) : TypedValue(double_array->Value(i), resource);
      };
    }
    case arrow::Type::STRING: {
      auto string_array = std::static_pointer_cast<arrow::StringArray>(column);
      return [string_array, resource](int64_t const i) -> TypedValue {
        if (string_array->IsNull(i)) return TypedValue(resource);
        auto str = string_array->GetString(i);
        return {TypedValue::TString{str, resource}, resource};
      };
    }
    case arrow::Type::LARGE_STRING: {
      auto large_string_array = std::static_pointer_cast<arrow::LargeStringArray>(column);
      return [large_string_array, resource](int64_t const i) -> TypedValue {
        if (large_string_array->IsNull(i)) return TypedValue(resource);
        auto view = large_string_array->GetView(i);
        return {TypedValue::TString{view, resource}, resource};
      };
    }
    case arrow::Type::STRING_VIEW: {
      auto string_view_array = std::static_pointer_cast<arrow::StringViewArray>(column);
      return [string_view_array, resource](int64_t const i) -> TypedValue {
        if (string_view_array->IsNull(i)) return TypedValue(resource);
        auto view = string_view_array->GetView(i);
        return {TypedValue::TString{view, resource}, resource};
      };
    }
    case arrow::Type::DATE32: {
      auto date_array = std::static_pointer_cast<arrow::Date32Array>(column);
      return [date_array, resource](int64_t const i) -> TypedValue {
        return date_array->IsNull(i) ? TypedValue(resource)
                                     : TypedValue(Date{std::chrono::days{date_array->Value(i)}}, resource);
      };
    }
    case arrow::Type::DATE64: {
      auto date_array = std::static_pointer_cast<arrow::Date64Array>(column);
      return [date_array, resource](int64_t const i) -> TypedValue {
        if (date_array->IsNull(i)) return TypedValue(resource);
        auto const ms = std::chrono::milliseconds(date_array->Value(i));
        auto const us = std::chrono::duration_cast<std::chrono::microseconds>(ms);
        return TypedValue(Date{us}, resource);
      };
    }
    case arrow::Type::TIME32: {
      auto time_array = std::static_pointer_cast<arrow::Time32Array>(column);
      auto time_type = std::static_pointer_cast<arrow::Time32Type>(column->type());
      return [time_array, unit = time_type->unit(), resource](int64_t const i) -> TypedValue {
        if (time_array->IsNull(i)) return TypedValue(resource);
        auto const arrow_val = time_array->Value(i);
        auto const us_val = ArrowTimeToUs(arrow_val, unit);
        return TypedValue(LocalTime{us_val}, resource);
      };
    }
    case arrow::Type::TIME64: {
      auto time_array = std::static_pointer_cast<arrow::Time64Array>(column);
      auto time_type = std::static_pointer_cast<arrow::Time64Type>(column->type());
      return [time_array, unit = time_type->unit(), resource](int64_t const i) -> TypedValue {
        if (time_array->IsNull(i)) return TypedValue(resource);
        auto const arrow_val = time_array->Value(i);
        auto const us_val = ArrowTimeToUs(arrow_val, unit);
        return TypedValue(LocalTime{us_val}, resource);
      };
    }
    case arrow::Type::TIMESTAMP: {
      auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(column);
      auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(column->type());
      return [timestamp_array, unit = timestamp_type->unit(), resource](int64_t const i) -> TypedValue {
        if (timestamp_array->IsNull(i)) return TypedValue(resource);
        auto const arrow_val = timestamp_array->Value(i);
        auto const us_val = ArrowTimeToUs(arrow_val, unit);
        return TypedValue(LocalDateTime{us_val}, resource);
      };
    }
    case arrow::Type::DURATION: {
      auto duration_array = std::static_pointer_cast<arrow::DurationArray>(column);
      auto duration_type = std::static_pointer_cast<arrow::DurationType>(column->type());
      return [duration_array, unit = duration_type->unit(), resource](int64_t const i) -> TypedValue {
        if (duration_array->IsNull(i)) return TypedValue(resource);
        auto const arrow_val = duration_array->Value(i);
        auto const us_val = ArrowTimeToUs(arrow_val, unit);
        return TypedValue(Duration{us_val}, resource);
      };
    }
    case arrow::Type::DECIMAL128: {
      auto decimal_array = std::static_pointer_cast<arrow::Decimal128Array>(column);
      auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(column->type());
      int32_t const scale = decimal_type->scale();
      return [decimal_array, scale, resource](int64_t const i) -> TypedValue {
        if (decimal_array->IsNull(i)) return TypedValue(resource);
        uint8_t const *bytes = decimal_array->GetValue(i);
        arrow::Decimal128 const value(bytes);
        return TypedValue(value.ToDouble(scale), resource);
      };
    }
    case arrow::Type::DECIMAL256: {
      auto decimal_array = std::static_pointer_cast<arrow::Decimal256Array>(column);
      auto decimal_type = std::static_pointer_cast<arrow::Decimal256Type>(column->type());
      int32_t const scale = decimal_type->scale();
      return [decimal_array, scale, resource](int64_t const i) -> TypedValue {
        if (decimal_array->IsNull(i)) return TypedValue(resource);
        uint8_t const *bytes = decimal_array->GetValue(i);
        arrow::Decimal256 const value(bytes);
        return TypedValue(value.ToDouble(scale), resource);
      };
    }
    case arrow::Type::BINARY: {
      auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(column);
      return [binary_array, resource](int64_t const i) -> TypedValue {
        if (binary_array->IsNull(i)) return TypedValue(resource);
        auto const view = binary_array->GetView(i);
        return TypedValue(view, resource);
      };
    }
    case arrow::Type::LARGE_BINARY: {
      auto large_binary_array = std::static_pointer_cast<arrow::LargeBinaryArray>(column);
      return [large_binary_array, resource](int64_t const i) -> TypedValue {
        if (large_binary_array->IsNull(i)) return TypedValue(resource);
        return TypedValue(large_binary_array->GetView(i), resource);
      };
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
      auto fixed_binary = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
      return [fixed_binary, resource](int64_t const i) -> TypedValue {
        if (fixed_binary->IsNull(i)) return TypedValue(resource);
        const uint8_t *data = fixed_binary->GetValue(i);
        return TypedValue(data, resource);
      };
    }
    case arrow::Type::LIST: {
      auto list_array = std::static_pointer_cast<arrow::ListArray>(column);
      return [list_array, resource](int64_t const i) -> TypedValue {
        if (list_array->IsNull(i)) return TypedValue(resource);

        auto const slice = list_array->value_slice(i);
        auto const elem_converter = CreateColumnConverter(slice, resource);

        TypedValue::TVector list_values(resource);
        int64_t const list_length = slice->length();
        list_values.reserve(list_length);

        for (int64_t k = 0; k < list_length; k++) {
          list_values.emplace_back(elem_converter(k));
        }

        return {std::move(list_values), resource};
      };
    }
    case arrow::Type::MAP: {
      auto map_array = std::static_pointer_cast<arrow::MapArray>(column);
      auto keys_array = map_array->keys();
      auto values_array = map_array->items();
      auto key_strings = std::static_pointer_cast<arrow::StringArray>(keys_array);

      return [map_array, key_strings, values_array, resource](int64_t const i) -> TypedValue {
        if (map_array->IsNull(i)) return TypedValue(resource);

        auto const val_converter = CreateColumnConverter(values_array, resource);

        int64_t const offset_start = map_array->value_offset(i);
        int64_t const offset_end = map_array->value_offset(i + 1);

        TypedValue::TMap map_values(resource);

        for (int64_t k = offset_start; k < offset_end; k++) {
          auto key = key_strings->GetString(k);
          map_values.emplace(TypedValue::TString{key, resource}, val_converter(k));
        }

        return {std::move(map_values), resource};
      };
    }
    default: {
      // Fallback to string conversion
      return [column, resource](int64_t const i) -> TypedValue {
        if (column->IsNull(i)) return TypedValue(resource);
        if (auto scalar = column->GetScalar(i); scalar.ok() && scalar.ValueOrDie()) {
          return TypedValue(scalar.ValueOrDie()->ToString(), resource);
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
  utils::pmr::vector<Row> rows_;
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
            // temporary needs to be created
            // NOLINTNEXTLINE
            queued_batch.emplace_back(Row{resource_});
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
  rows_.reserve(batch_rows);
  for (auto i = 0U; i < batch_rows; i++) {
    // temporary needs to be created
    // NOLINTNEXTLINE
    rows_.emplace_back(Row{resource_});
  }
}

ParquetReader::impl::~impl() { prefetcher_thread_.request_stop(); }

auto ParquetReader::impl::GetNextRow(Row &out) -> bool {
  if (row_in_batch_ >= current_batch_size_) {
    if (!work_queue_.pop(rows_)) {
      return false;
    }
    row_in_batch_ = 0;
    current_batch_size_ = rows_.size();
  }
  std::swap(out, rows_[row_in_batch_++]);
  return true;
}

ParquetReader::ParquetReader(ParquetFileConfig parquet_file_config, utils::MemoryResource *resource) {
  auto file_reader = std::invoke([&]() -> std::unique_ptr<parquet::arrow::FileReader> {
    constexpr auto url_matcher = ctre::starts_with<"(https?|ftp)://">;
    constexpr auto s3_matcher = ctre::starts_with<"s3://">;

    // When using a file that should be downloaded using https or ftp, we first download it and then load it
    if (url_matcher(parquet_file_config.file)) {
      auto const file_name = std::filesystem::path{parquet_file_config.file}.filename();
      auto const local_file_path = fmt::format("/tmp/{}", file_name);
      if (requests::CreateAndDownloadFile(parquet_file_config.file, local_file_path)) {
        utils::OnScopeExit const on_exit{[&local_file_path]() { utils::DeleteFile(local_file_path); }};

        return LoadFileFromDisk(local_file_path);
      }
      spdlog::error("Couldn't download file {}", parquet_file_config.file);
      return nullptr;
    }

    if (s3_matcher(parquet_file_config.file)) {
      return LoadFileFromS3(BuildS3Config(parquet_file_config));
    }

    // Regular file that already exists on disk
    return LoadFileFromDisk(parquet_file_config.file);
  });

  if (!file_reader) {
    throw utils::BasicException("Failed to load file {}.", parquet_file_config.file);
  }

  auto maybe_batch_reader = file_reader->GetRecordBatchReader();
  if (!maybe_batch_reader.ok()) {
    throw utils::BasicException("Couldn't create RecordBatchReader because of {}",
                                maybe_batch_reader.status().message());
  }

  auto batch_reader = std::move(*maybe_batch_reader);
  auto header = BuildHeader(batch_reader->schema(), resource);

  pimpl_ = std::make_unique<impl>(std::move(file_reader), std::move(batch_reader), std::move(header), resource);
}

ParquetReader::~ParquetReader() = default;

// keep logical constness
// NOLINTNEXTLINE
auto ParquetReader::GetNextRow(Row &out) -> bool { return pimpl_->GetNextRow(out); }

}  // namespace memgraph::query
