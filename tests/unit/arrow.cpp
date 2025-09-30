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

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>
#include <atomic>
#include <chrono>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

using namespace std::chrono;

class ParquetBenchmark {
 public:
  ParquetBenchmark(const std::string &filename) : filename_(filename) {
    // Get number of hardware threads
    num_threads_ = std::thread::hardware_concurrency();
    if (num_threads_ == 0) num_threads_ = 4;  // fallback
    std::cout << "Hardware threads available: " << num_threads_ << std::endl;
  }

  // Benchmark reading the entire Parquet file
  void BenchmarkFullRead() {
    std::cout << "\n=== Benchmark: Full File Read ===" << std::endl;

    auto start = high_resolution_clock::now();

    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filename_, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Rows read: " << table->num_rows() << std::endl;
    std::cout << "  Columns: " << table->num_columns() << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << (table->num_rows() * 1000.0 / duration.count()) << " rows/sec" << std::endl;

    table_ = table;
  }

  // Benchmark reading specific columns
  void BenchmarkColumnRead() {
    std::cout << "\n=== Benchmark: Selective Column Read ===" << std::endl;

    auto start = high_resolution_clock::now();

    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filename_, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

    std::shared_ptr<arrow::Table> table;
    std::vector<int> column_indices = {0, 2};  // id and age
    PARQUET_THROW_NOT_OK(reader->ReadTable(column_indices, &table));

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Columns read: id, age" << std::endl;
    std::cout << "  Rows: " << table->num_rows() << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << (table->num_rows() * 1000.0 / duration.count()) << " rows/sec" << std::endl;
  }

  // Benchmark filtering operations
  void BenchmarkFiltering() {
    if (!table_) {
      std::cout << "Table not loaded. Run BenchmarkFullRead first." << std::endl;
      return;
    }

    std::cout << "\n=== Benchmark: Filtering (age > 30) ===" << std::endl;

    auto start = high_resolution_clock::now();

    // Get the age column
    auto age_column = table_->GetColumnByName("age");
    if (!age_column) {
      std::cerr << "Age column not found" << std::endl;
      return;
    }

    // Create filter expression (age > 30)
    auto age_array = std::static_pointer_cast<arrow::Int64Array>(age_column->chunk(0));

    arrow::Int64Builder filter_builder;
    int64_t count = 0;
    for (int64_t i = 0; i < age_array->length(); i++) {
      if (!age_array->IsNull(i) && age_array->Value(i) > 30) {
        count++;
      }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Total rows: " << table_->num_rows() << std::endl;
    std::cout << "  Filtered rows (age > 30): " << count << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << (table_->num_rows() * 1000.0 / duration.count()) << " rows/sec" << std::endl;
  }

  // Benchmark aggregation operations
  void BenchmarkAggregation() {
    if (!table_) {
      std::cout << "Table not loaded. Run BenchmarkFullRead first." << std::endl;
      return;
    }

    std::cout << "\n=== Benchmark: Aggregation (avg age) ===" << std::endl;

    auto start = high_resolution_clock::now();

    // Get the age column
    auto age_column = table_->GetColumnByName("age");
    if (!age_column) {
      std::cerr << "Age column not found" << std::endl;
      return;
    }

    // Calculate average age
    auto age_array = std::static_pointer_cast<arrow::Int64Array>(age_column->chunk(0));

    int64_t sum = 0;
    int64_t count = 0;
    for (int64_t i = 0; i < age_array->length(); i++) {
      if (!age_array->IsNull(i)) {
        sum += age_array->Value(i);
        count++;
      }
    }

    double average = static_cast<double>(sum) / count;

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Average age: " << std::fixed << std::setprecision(2) << average << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << (table_->num_rows() * 1000.0 / duration.count()) << " rows/sec" << std::endl;
  }

  // Benchmark sequential scan
  void BenchmarkSequentialScan() {
    if (!table_) {
      std::cout << "Table not loaded. Run BenchmarkFullRead first." << std::endl;
      return;
    }

    std::cout << "\n=== Benchmark: Sequential Scan ===" << std::endl;

    auto start = high_resolution_clock::now();

    int64_t total_rows = 0;

    // Iterate through all columns and rows
    for (int col = 0; col < table_->num_columns(); col++) {
      auto column = table_->column(col);
      for (auto &chunk : column->chunks()) {
        total_rows += chunk->length();
      }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Rows scanned: " << total_rows / table_->num_columns() << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << (total_rows * 1000.0 / table_->num_columns() / duration.count()) << " rows/sec" << std::endl;
  }

  // Optimized Strategy 1: Parallel Row Group Reading
  void ParallelRowGroupRead() {
    std::cout << "\n=== Optimized: Parallel Row Group Reading ===" << std::endl;
    auto start = high_resolution_clock::now();

    // Open file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filename_, arrow::default_memory_pool()));

    // Create file reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

    // Get metadata
    std::shared_ptr<parquet::FileMetaData> file_metadata = reader->parquet_reader()->metadata();
    int num_row_groups = file_metadata->num_row_groups();

    std::cout << "Number of row groups: " << num_row_groups << std::endl;

    // Read row groups in parallel
    std::vector<std::future<std::shared_ptr<arrow::Table>>> futures;
    futures.reserve(num_row_groups);

    // Launch parallel reads
    for (int i = 0; i < num_row_groups; ++i) {
      futures.push_back(std::async(std::launch::async, [this, i]() -> std::shared_ptr<arrow::Table> {
        // Each thread creates its own reader
        std::shared_ptr<arrow::io::ReadableFile> local_infile;
        PARQUET_ASSIGN_OR_THROW(local_infile, arrow::io::ReadableFile::Open(filename_, arrow::default_memory_pool()));

        std::unique_ptr<parquet::arrow::FileReader> local_reader;
        PARQUET_ASSIGN_OR_THROW(local_reader, parquet::arrow::OpenFile(local_infile, arrow::default_memory_pool()));

        std::shared_ptr<arrow::Table> table;
        auto status = local_reader->ReadRowGroup(i, &table);
        if (!status.ok()) {
          std::cerr << "Failed to read row group " << i << ": " << status << std::endl;
          return nullptr;
        }
        return table;
      }));
    }

    // Collect results
    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (auto &future : futures) {
      auto table = future.get();
      if (table) {
        tables.push_back(table);
      }
    }

    // Combine tables
    auto combined_result = arrow::ConcatenateTables(tables);
    if (!combined_result.ok()) {
      std::cerr << "Failed to concatenate tables: " << combined_result.status() << std::endl;
      return;
    }
    std::shared_ptr<arrow::Table> combined_table = combined_result.ValueOrDie();

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Total rows read: " << combined_table->num_rows() << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << (combined_table->num_rows() * 1000.0 / duration.count()) << " rows/sec" << std::endl;
  }

  // Optimized Strategy 2: Memory-Mapped File Reading
  void MemoryMappedRead() {
    std::cout << "\n=== Optimized: Memory-Mapped File Reading ===" << std::endl;
    auto start = high_resolution_clock::now();

    // Use memory-mapped file for better performance
    std::shared_ptr<arrow::io::MemoryMappedFile> mmapped_file;
    PARQUET_ASSIGN_OR_THROW(mmapped_file, arrow::io::MemoryMappedFile::Open(filename_, arrow::io::FileMode::READ));

    // Create reader with memory-mapped file
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(mmapped_file, arrow::default_memory_pool()));

    // Read entire table
    std::shared_ptr<arrow::Table> table;
    auto status = reader->ReadTable(&table);
    if (!status.ok()) {
      std::cerr << "Failed to read table: " << status << std::endl;
      return;
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Total rows read: " << table->num_rows() << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << (table->num_rows() * 1000.0 / duration.count()) << " rows/sec" << std::endl;

    table_ = table;  // Store for other benchmarks
  }

  // Optimized Strategy 3: Thread Pool for Row Groups
  void ThreadPoolRead() {
    std::cout << "\n=== Optimized: Thread Pool Reading ===" << std::endl;
    auto start = high_resolution_clock::now();

    // Open file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filename_, arrow::default_memory_pool()));

    // Create reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

    // Get metadata
    std::shared_ptr<parquet::FileMetaData> file_metadata = reader->parquet_reader()->metadata();
    int num_row_groups = file_metadata->num_row_groups();

    // Create thread pool
    const int pool_size = std::min(num_threads_, (unsigned int)num_row_groups);
    std::atomic<int> next_row_group(0);
    std::vector<std::shared_ptr<arrow::Table>> results(num_row_groups);
    std::vector<std::thread> threads;

    std::cout << "Using thread pool with " << pool_size << " threads for " << num_row_groups << " row groups"
              << std::endl;

    // Worker function
    auto worker = [&]() {
      // Each worker gets its own reader
      std::shared_ptr<arrow::io::ReadableFile> local_infile;
      PARQUET_ASSIGN_OR_THROW(local_infile, arrow::io::ReadableFile::Open(filename_, arrow::default_memory_pool()));

      std::unique_ptr<parquet::arrow::FileReader> local_reader;
      PARQUET_ASSIGN_OR_THROW(local_reader, parquet::arrow::OpenFile(local_infile, arrow::default_memory_pool()));

      while (true) {
        int rg = next_row_group.fetch_add(1);
        if (rg >= num_row_groups) break;

        std::shared_ptr<arrow::Table> table;
        auto status = local_reader->ReadRowGroup(rg, &table);
        if (status.ok()) {
          results[rg] = table;
        }
      }
    };

    // Launch workers
    for (int i = 0; i < pool_size; ++i) {
      threads.emplace_back(worker);
    }

    // Wait for completion
    for (auto &t : threads) {
      t.join();
    }

    // Combine results
    std::vector<std::shared_ptr<arrow::Table>> valid_tables;
    for (const auto &table : results) {
      if (table) {
        valid_tables.push_back(table);
      }
    }

    auto combined_result = arrow::ConcatenateTables(valid_tables);
    if (!combined_result.ok()) {
      std::cerr << "Failed to concatenate tables: " << combined_result.status() << std::endl;
      return;
    }
    std::shared_ptr<arrow::Table> combined_table = combined_result.ValueOrDie();

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Total rows read: " << combined_table->num_rows() << std::endl;
    std::cout << "  Time: " << duration.count() << " ms" << std::endl;
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
              << (combined_table->num_rows() * 1000.0 / duration.count()) << " rows/sec" << std::endl;
  }

  // Run all benchmarks
  void RunAllBenchmarks() {
    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << "  Apache Arrow/Parquet Performance Benchmark" << std::endl;
    std::cout << "  File: " << filename_ << std::endl;
    std::cout << std::string(50, '=') << std::endl;

    // Standard benchmarks
    BenchmarkFullRead();
    BenchmarkColumnRead();
    BenchmarkFiltering();
    BenchmarkAggregation();
    BenchmarkSequentialScan();

    // Optimized parallel reading strategies
    std::cout << "\n" << std::string(50, '-') << std::endl;
    std::cout << "  Optimized Reading Strategies" << std::endl;
    std::cout << std::string(50, '-') << std::endl;

    MemoryMappedRead();
    ParallelRowGroupRead();
    ThreadPoolRead();

    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << "  Benchmark Complete" << std::endl;
    std::cout << std::string(50, '=') << std::endl;
  }

 private:
  std::string filename_;
  std::shared_ptr<arrow::Table> table_;
  unsigned int num_threads_;
};

TEST(Storage, Benchmark) {
  ParquetBenchmark benchmark("/home/andi.linux/Memgraph/code/memgraph/tests/unit/nodes_10m.parquet");
  benchmark.RunAllBenchmarks();
}

TEST(Storage, OptimizedReading) {
  std::cout << "\n=== Testing Optimized Parquet Reading Strategies ===" << std::endl;

  ParquetBenchmark benchmark("/home/andi.linux/Memgraph/code/memgraph/tests/unit/nodes_10m.parquet");

  // Test individual optimized strategies
  std::cout << "\n--- Memory Mapped Reading ---" << std::endl;
  benchmark.MemoryMappedRead();

  std::cout << "\n--- Parallel Row Group Reading ---" << std::endl;
  benchmark.ParallelRowGroupRead();

  std::cout << "\n--- Thread Pool Reading ---" << std::endl;
  benchmark.ThreadPoolRead();
}

TEST(Storage, Reader) {
  std::cout << "Reading /home/andi.linux/Memgraph/code/memgraph/tests/unit/nodes.parquet at once" << std::endl;

  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
      infile, arrow::io::ReadableFile::Open("/home/andi.linux/Memgraph/code/memgraph/tests/unit/nodes.parquet",
                                            arrow::default_memory_pool()));

  auto reader_result = parquet::arrow::OpenFile(infile, arrow::default_memory_pool());
  if (!reader_result.ok()) {
    throw std::runtime_error("Failed to open Parquet file: " + reader_result.status().ToString());
  }
  std::unique_ptr<parquet::arrow::FileReader> reader = std::move(*reader_result);
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns() << " columns." << std::endl;
}
