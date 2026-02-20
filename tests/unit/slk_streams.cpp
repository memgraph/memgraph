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

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "slk/streams.hpp"

#include "slk_common.hpp"

TEST(Builder, SingleSegment) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto input = GetRandomData(5);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());

  ASSERT_EQ(buffer.size(), input.size() + 2 * sizeof(memgraph::slk::SegmentSize));

  auto splits =
      BufferToBinaryData(buffer.data(),
                         buffer.size(),
                         {sizeof(memgraph::slk::SegmentSize), input.size(), sizeof(memgraph::slk::SegmentSize)});

  auto header_expected = SizeToBinaryData(input.size());
  ASSERT_EQ(splits[0], header_expected);

  ASSERT_EQ(splits[1], input);

  auto footer_expected = SizeToBinaryData(0);
  ASSERT_EQ(splits[2], footer_expected);
}

TEST(Builder, MultipleSegments) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 100);
  builder.Save(input.data(), input.size());
  builder.Finalize();

  ASSERT_EQ(buffer.size(), input.size() + 3 * sizeof(memgraph::slk::SegmentSize));

  auto splits = BufferToBinaryData(buffer.data(),
                                   buffer.size(),
                                   {sizeof(memgraph::slk::SegmentSize),
                                    memgraph::slk::kSegmentMaxDataSize,
                                    sizeof(memgraph::slk::SegmentSize),
                                    input.size() - memgraph::slk::kSegmentMaxDataSize,
                                    sizeof(memgraph::slk::SegmentSize)});

  auto datas =
      BufferToBinaryData(input.data(),
                         input.size(),
                         {memgraph::slk::kSegmentMaxDataSize, input.size() - memgraph::slk::kSegmentMaxDataSize});

  auto header1_expected = SizeToBinaryData(memgraph::slk::kSegmentMaxDataSize);
  ASSERT_EQ(splits[0], header1_expected);

  ASSERT_EQ(splits[1], datas[0]);

  auto header2_expected = SizeToBinaryData(input.size() - memgraph::slk::kSegmentMaxDataSize);
  ASSERT_EQ(splits[2], header2_expected);

  ASSERT_EQ(splits[3], datas[1]);

  auto footer_expected = SizeToBinaryData(0);
  ASSERT_EQ(splits[4], footer_expected);
}

TEST(Builder, PrepareForFileSending) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto input = GetRandomData(100);
  builder.PrepareForFileSending();
  ASSERT_TRUE(builder.GetFileData());
  ASSERT_TRUE(builder.SaveFileBuffer(input.data(), input.size()).has_value());
  ASSERT_TRUE(builder.Finalize().has_value());

  ASSERT_EQ(buffer.size(), input.size() + sizeof(memgraph::slk::SegmentSize) + sizeof(memgraph::slk::SegmentSize));

  // Test kFileSegmentMask
  {
    memgraph::slk::SegmentSize len;
    memcpy(&len, buffer.data(), sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(len, memgraph::slk::kFileSegmentMask);
  }

  // Test footer at the end
  {
    memgraph::slk::SegmentSize footer;
    memcpy(
        &footer, buffer.data() + input.size() + sizeof(memgraph::slk::SegmentSize), sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(footer, memgraph::slk::kFooter);
  }
}

TEST(Builder, FlushWithoutFile) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto input = GetRandomData(20);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());
  ASSERT_EQ(buffer.size(), input.size() + 2 * sizeof(memgraph::slk::SegmentSize));

  // Test that 4B at the beginning represent size of the content
  {
    memgraph::slk::SegmentSize len;
    memcpy(&len, buffer.data(), sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(len, 20);
  }

  // Test footer at the end
  {
    memgraph::slk::SegmentSize footer;
    memcpy(
        &footer, buffer.data() + input.size() + sizeof(memgraph::slk::SegmentSize), sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(footer, memgraph::slk::kFooter);
  }
}

TEST(Reader, InitializeReaderWithHave) {
  auto input = GetRandomData(5);

  memgraph::slk::Reader(input.data(), input.size(), input.size());
}

TEST(Reader, SingleSegment) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto input = GetRandomData(5);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());

  // test with missing data
  for (size_t i = 0; i < buffer.size(); ++i) {
    memgraph::slk::Reader reader(buffer.data(), i);
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size());
    reader.Finalize();
    ASSERT_TRUE(reader.GetError().has_value());
  }

  // test with complete data
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size());
    reader.Finalize();
    ASSERT_FALSE(reader.GetError().has_value());
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // test with leftover data (benign, no error)
  {
    auto extended_buffer = BinaryData(buffer.data(), buffer.size()) + GetRandomData(5);
    memgraph::slk::Reader reader(extended_buffer.data(), extended_buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size());
    reader.Finalize();
    ASSERT_FALSE(reader.GetError().has_value());
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // read more data than there is in the stream
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, memgraph::slk::kSegmentMaxDataSize);
    ASSERT_TRUE(reader.GetError().has_value());
  }

  // don't consume all data from the stream (leftover data is benign, no error)
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size() / 2);
    reader.Finalize();
    ASSERT_FALSE(reader.GetError().has_value());
  }

  // read data with several loads
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    for (size_t i = 0; i < input.size(); ++i) {
      reader.Load(block + i, 1);
    }
    reader.Finalize();
    ASSERT_FALSE(reader.GetError().has_value());
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // modify the end mark
  buffer[buffer.size() - 1] = 1;
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size());
    reader.Finalize();
    ASSERT_TRUE(reader.GetError().has_value());
  }
}

TEST(Reader, MultipleSegments) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 100);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());

  // test with missing data
  for (size_t i = 0; i < buffer.size(); ++i) {
    memgraph::slk::Reader reader(buffer.data(), i);
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size());
    reader.Finalize();
    ASSERT_TRUE(reader.GetError().has_value());
  }

  // test with complete data
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size());
    reader.Finalize();
    ASSERT_FALSE(reader.GetError().has_value());
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // test with leftover data (benign, no error)
  {
    auto extended_buffer = BinaryData(buffer.data(), buffer.size()) + GetRandomData(5);
    memgraph::slk::Reader reader(extended_buffer.data(), extended_buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size());
    reader.Finalize();
    ASSERT_FALSE(reader.GetError().has_value());
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // read more data than there is in the stream
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, memgraph::slk::kSegmentMaxDataSize * 2);
    ASSERT_TRUE(reader.GetError().has_value());
  }

  // don't consume all data from the stream (leftover data is benign, no error)
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size() / 2);
    reader.Finalize();
    ASSERT_FALSE(reader.GetError().has_value());
  }

  // read data with several loads
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    for (size_t i = 0; i < input.size(); ++i) {
      reader.Load(block + i, 1);
    }
    reader.Finalize();
    ASSERT_FALSE(reader.GetError().has_value());
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // modify the end mark
  buffer[buffer.size() - 1] = 1;
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size());
    reader.Finalize();
    ASSERT_TRUE(reader.GetError().has_value());
  }
}

TEST(BuilderError, WriteFunctionFailureStickyError) {
  int write_call_count = 0;
  memgraph::slk::Builder builder(
      [&write_call_count](const uint8_t *, size_t, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
        ++write_call_count;
        return std::unexpected{memgraph::utils::RpcError::GENERIC_RPC_ERROR};
      });

  // Save data that fills a segment to trigger a flush on Finalize
  auto input = GetRandomData(5);
  builder.Save(input.data(), input.size());
  // Save itself doesn't flush for small data, so no error yet
  ASSERT_FALSE(builder.GetError().has_value());

  // Finalize triggers the flush which calls the write function
  auto result = builder.Finalize();
  ASSERT_FALSE(result.has_value());
  ASSERT_EQ(result.error(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
  ASSERT_TRUE(builder.GetError().has_value());
  ASSERT_EQ(*builder.GetError(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
  ASSERT_EQ(write_call_count, 1);
}

TEST(BuilderError, SaveIsNoOpAfterError) {
  int write_call_count = 0;
  memgraph::slk::Builder builder(
      [&write_call_count](const uint8_t *, size_t, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
        ++write_call_count;
        return std::unexpected{memgraph::utils::RpcError::GENERIC_RPC_ERROR};
      });

  // Fill a full segment to force a flush during Save
  auto large_input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 1);
  builder.Save(large_input.data(), large_input.size());

  // The flush should have failed — write was called once, then Save stopped
  ASSERT_TRUE(builder.GetError().has_value());
  ASSERT_EQ(write_call_count, 1);

  // Subsequent Save calls are no-ops — write function is NOT called again
  auto more_data = GetRandomData(10);
  builder.Save(more_data.data(), more_data.size());
  ASSERT_EQ(write_call_count, 1);

  // Finalize also returns the same sticky error
  auto result = builder.Finalize();
  ASSERT_FALSE(result.has_value());
  ASSERT_EQ(result.error(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
  ASSERT_EQ(write_call_count, 1);
}

TEST(BuilderError, SaveFileBufferReturnsErrorAfterFailure) {
  memgraph::slk::Builder builder([](const uint8_t *, size_t, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
    return std::unexpected{memgraph::utils::RpcError::GENERIC_RPC_ERROR};
  });

  // Force the error via a large Save that triggers a flush
  auto large_input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 1);
  builder.Save(large_input.data(), large_input.size());
  ASSERT_TRUE(builder.GetError().has_value());

  // SaveFileBuffer should return the sticky error immediately
  auto file_data = GetRandomData(10);
  auto result = builder.SaveFileBuffer(file_data.data(), file_data.size());
  ASSERT_FALSE(result.has_value());
  ASSERT_EQ(result.error(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
}

TEST(BuilderError, PrepareForFileSendingIsNoOpAfterError) {
  memgraph::slk::Builder builder([](const uint8_t *, size_t, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
    return std::unexpected{memgraph::utils::RpcError::GENERIC_RPC_ERROR};
  });

  auto large_input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 1);
  builder.Save(large_input.data(), large_input.size());
  ASSERT_TRUE(builder.GetError().has_value());

  // PrepareForFileSending should be a no-op — file_data_ should NOT be set
  builder.PrepareForFileSending();
  ASSERT_FALSE(builder.GetFileData());
}

TEST(BuilderError, FlushSegmentReturnsErrorAfterFailure) {
  memgraph::slk::Builder builder([](const uint8_t *, size_t, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
    return std::unexpected{memgraph::utils::RpcError::TIMEOUT_ERROR};
  });

  auto large_input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 1);
  builder.Save(large_input.data(), large_input.size());
  ASSERT_TRUE(builder.GetError().has_value());

  // Explicit FlushSegment also returns the sticky error
  auto result = builder.FlushSegment(true, true);
  ASSERT_FALSE(result.has_value());
  ASSERT_EQ(result.error(), memgraph::utils::RpcError::TIMEOUT_ERROR);
}

TEST(BuilderError, FailureOnSecondSegmentPreservesFirstError) {
  int write_call_count = 0;
  memgraph::slk::Builder builder(
      [&write_call_count](const uint8_t *, size_t, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
        ++write_call_count;
        if (write_call_count == 1) return {};                              // First segment succeeds
        return std::unexpected{memgraph::utils::RpcError::TIMEOUT_ERROR};  // Second fails
      });

  // Write enough data to span two segments
  auto input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 100);
  builder.Save(input.data(), input.size());

  // First segment flushed OK, second failed during Save (when it tried to flush the full first segment)
  // The error should surface at Finalize
  auto result = builder.Finalize();
  ASSERT_FALSE(result.has_value());
  ASSERT_EQ(result.error(), memgraph::utils::RpcError::TIMEOUT_ERROR);
  ASSERT_EQ(write_call_count, 2);
}

TEST(BuilderError, MovedFromBuilderReturnsError) {
  std::vector<uint8_t> buffer;
  auto write_fn = [&buffer](
                      const uint8_t *data, size_t size, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
    return {};
  };

  memgraph::slk::Builder original(write_fn);
  auto input = GetRandomData(5);
  original.Save(input.data(), input.size());

  // Move-construct with a new write function
  memgraph::slk::Builder moved(std::move(original), write_fn);

  // The moved-to builder should work fine
  auto result = moved.Finalize();
  ASSERT_TRUE(result.has_value());
  ASSERT_FALSE(moved.GetError().has_value());
}

TEST(ReaderError, LoadAfterErrorIsNoOp) {
  // Create a valid stream
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
        return {};
      });

  auto input = GetRandomData(10);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());

  // Truncate the buffer to cause an error during Load
  memgraph::slk::Reader reader(buffer.data(), sizeof(memgraph::slk::SegmentSize) + 2);
  uint8_t block[20] = {};
  reader.Load(block, 10);  // Will fail — not enough data
  ASSERT_TRUE(reader.GetError().has_value());
  ASSERT_EQ(*reader.GetError(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);

  // Subsequent Load is a no-op
  uint8_t block2[5] = {0xAA, 0xAA, 0xAA, 0xAA, 0xAA};
  reader.Load(block2, 5);
  // block2 should be untouched since Load was a no-op
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(block2[i], 0xAA);
  }
  ASSERT_TRUE(reader.GetError().has_value());
}

TEST(ReaderError, FinalizeAfterErrorIsNoOp) {
  // Empty buffer — reader will fail immediately
  uint8_t data[1] = {0};
  memgraph::slk::Reader reader(data, 0);

  uint8_t block[1];
  reader.Load(block, 1);  // Fails — size data missing
  ASSERT_TRUE(reader.GetError().has_value());

  // Finalize should be a no-op and not change the error
  reader.Finalize();
  ASSERT_TRUE(reader.GetError().has_value());
  ASSERT_EQ(*reader.GetError(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
}

TEST(ReaderError, SizeDataMissing) {
  // Buffer too small to even contain a segment header
  uint8_t data[2] = {0x01, 0x00};
  memgraph::slk::Reader reader(data, sizeof(data));

  uint8_t block[1];
  reader.Load(block, 1);
  ASSERT_TRUE(reader.GetError().has_value());
  ASSERT_EQ(*reader.GetError(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
}

TEST(ReaderError, NonEmptySegmentWhenExpectingFinal) {
  // Craft a buffer where the footer position has a non-zero length
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
        return {};
      });

  auto input = GetRandomData(5);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());

  // Corrupt the footer: change 0x0000 to a non-zero value
  // Footer is at the end: last sizeof(SegmentSize) bytes
  memgraph::slk::SegmentSize fake_len = 42;
  memcpy(buffer.data() + buffer.size() - sizeof(memgraph::slk::SegmentSize),
         &fake_len,
         sizeof(memgraph::slk::SegmentSize));

  memgraph::slk::Reader reader(buffer.data(), buffer.size());
  uint8_t block[10];
  reader.Load(block, input.size());
  reader.Finalize();  // Should detect the corrupted footer
  ASSERT_TRUE(reader.GetError().has_value());
  ASSERT_EQ(*reader.GetError(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
}

TEST(ReaderError, EmptySegmentWhenExpectingData) {
  // Craft a buffer with a zero-length first segment (footer where data is expected)
  memgraph::slk::SegmentSize zero = 0;
  uint8_t buffer[sizeof(memgraph::slk::SegmentSize)];
  memcpy(buffer, &zero, sizeof(zero));

  memgraph::slk::Reader reader(buffer, sizeof(buffer));
  uint8_t block[1];
  reader.Load(block, 1);  // Expects non-empty segment, gets footer
  ASSERT_TRUE(reader.GetError().has_value());
  ASSERT_EQ(*reader.GetError(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
}

TEST(ReaderError, NotEnoughDataInStream) {
  // Segment header claims 100 bytes but buffer only has a few
  memgraph::slk::SegmentSize len = 100;
  uint8_t buffer[sizeof(memgraph::slk::SegmentSize) + 10];
  memcpy(buffer, &len, sizeof(len));
  // Only 10 bytes of payload, but header claims 100

  memgraph::slk::Reader reader(buffer, sizeof(buffer));
  uint8_t block[100];
  reader.Load(block, 100);
  ASSERT_TRUE(reader.GetError().has_value());
  ASSERT_EQ(*reader.GetError(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
}

TEST(ReaderError, FileSegmentMaskInNonFinalPosition) {
  // kFileSegmentMask encountered when should_be_final=false triggers error
  memgraph::slk::SegmentSize mask = memgraph::slk::kFileSegmentMask;
  uint8_t buffer[sizeof(memgraph::slk::SegmentSize)];
  memcpy(buffer, &mask, sizeof(mask));

  memgraph::slk::Reader reader(buffer, sizeof(buffer));
  uint8_t block[1];
  reader.Load(block, 1);  // GetSegment(false) encounters kFileSegmentMask
  ASSERT_TRUE(reader.GetError().has_value());
  ASSERT_EQ(*reader.GetError(), memgraph::utils::RpcError::GENERIC_RPC_ERROR);
}

TEST(ReaderError, LeftoverDataIsBenign) {
  // Build a valid stream
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
        return {};
      });

  auto input = GetRandomData(10);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());

  // Read only half the data, then Finalize — should be fine (leftover is benign)
  memgraph::slk::Reader reader(buffer.data(), buffer.size());
  uint8_t block[5];
  reader.Load(block, 5);
  reader.Finalize();
  ASSERT_FALSE(reader.GetError().has_value());
}

TEST(ReaderError, InitializedWithHaveSkipsGetSegment) {
  // When initialized with have > 0, GetSegment is skipped (data already available)
  auto input = GetRandomData(10);
  memgraph::slk::Reader reader(input.data(), input.size(), input.size());

  uint8_t block[10];
  reader.Load(block, 10);
  ASSERT_FALSE(reader.GetError().has_value());
  auto output = BinaryData(block, 10);
  ASSERT_EQ(output, BinaryData(input.data(), input.size()));
}

TEST(BuilderReaderRoundTrip, ErrorInBuilderDoesNotProduceValidStream) {
  std::vector<uint8_t> buffer;
  bool first_write = true;
  memgraph::slk::Builder builder([&buffer, &first_write](const uint8_t *data,
                                                         size_t size,
                                                         bool) -> memgraph::slk::BuilderWriteFunction::result_type {
    if (first_write) {
      // First segment succeeds
      for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
      first_write = false;
      return {};
    }
    // Second segment fails
    return std::unexpected{memgraph::utils::RpcError::GENERIC_RPC_ERROR};
  });

  auto input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 100);
  builder.Save(input.data(), input.size());
  auto result = builder.Finalize();
  ASSERT_FALSE(result.has_value());

  // The partial buffer should NOT be a complete stream
  auto stream_info = memgraph::slk::CheckStreamStatus(buffer.data(), buffer.size());
  ASSERT_NE(stream_info.status, memgraph::slk::StreamStatus::COMPLETE);
}

TEST(CheckStreamStatus, SingleSegment) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto input = GetRandomData(5);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());

  // test with missing data
  for (size_t i = 0; i < sizeof(memgraph::slk::SegmentSize); ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, memgraph::slk::kSegmentMaxTotalSize);
    ASSERT_EQ(data_size, 0);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize); i < sizeof(memgraph::slk::SegmentSize) + input.size(); ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, memgraph::slk::kSegmentMaxTotalSize + sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(data_size, 0);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize) + input.size(); i < buffer.size(); ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, memgraph::slk::kSegmentMaxTotalSize + sizeof(memgraph::slk::SegmentSize) + input.size());
    ASSERT_EQ(data_size, input.size());
  }

  // test with complete data
  {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), buffer.size());
    ASSERT_EQ(status, memgraph::slk::StreamStatus::COMPLETE);
    ASSERT_EQ(stream_size, buffer.size());
    ASSERT_EQ(data_size, input.size());
  }

  // test with leftover data
  {
    auto extended_buffer = BinaryData(buffer.data(), buffer.size()) + GetRandomData(5);
    auto [status, stream_size, data_size, pos] =
        memgraph::slk::CheckStreamStatus(extended_buffer.data(), extended_buffer.size());
    ASSERT_EQ(status, memgraph::slk::StreamStatus::COMPLETE);
    ASSERT_EQ(stream_size, buffer.size());
    ASSERT_EQ(data_size, input.size());
  }
}

TEST(CheckStreamStatus, MultipleSegments) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 100);
  builder.Save(input.data(), input.size());
  ASSERT_TRUE(builder.Finalize().has_value());

  // test with missing data
  for (size_t i = 0; i < sizeof(memgraph::slk::SegmentSize); ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, memgraph::slk::kSegmentMaxTotalSize);
    ASSERT_EQ(data_size, 0);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize);
       i < sizeof(memgraph::slk::SegmentSize) + memgraph::slk::kSegmentMaxDataSize;
       ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, memgraph::slk::kSegmentMaxTotalSize + sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(data_size, 0);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize) + memgraph::slk::kSegmentMaxDataSize;
       i < sizeof(memgraph::slk::SegmentSize) * 2 + memgraph::slk::kSegmentMaxDataSize;
       ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(
        stream_size,
        sizeof(memgraph::slk::SegmentSize) + memgraph::slk::kSegmentMaxDataSize + memgraph::slk::kSegmentMaxTotalSize);
    ASSERT_EQ(data_size, memgraph::slk::kSegmentMaxDataSize);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize) * 2 + memgraph::slk::kSegmentMaxDataSize;
       i < sizeof(memgraph::slk::SegmentSize) * 2 + input.size();
       ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size,
              sizeof(memgraph::slk::SegmentSize) * 2 + memgraph::slk::kSegmentMaxDataSize +
                  memgraph::slk::kSegmentMaxTotalSize);
    ASSERT_EQ(data_size, memgraph::slk::kSegmentMaxDataSize);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize) * 2 + input.size(); i < buffer.size(); ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, memgraph::slk::kSegmentMaxTotalSize + sizeof(memgraph::slk::SegmentSize) * 2 + input.size());
    ASSERT_EQ(data_size, input.size());
  }

  // test with complete data
  {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), buffer.size());
    ASSERT_EQ(status, memgraph::slk::StreamStatus::COMPLETE);
    ASSERT_EQ(stream_size, buffer.size());
    ASSERT_EQ(data_size, input.size());
  }

  // test with leftover data
  {
    auto extended_buffer = BinaryData(buffer.data(), buffer.size()) + GetRandomData(5);
    auto [status, stream_size, data_size, pos] =
        memgraph::slk::CheckStreamStatus(extended_buffer.data(), extended_buffer.size());
    ASSERT_EQ(status, memgraph::slk::StreamStatus::COMPLETE);
    ASSERT_EQ(stream_size, buffer.size());
    ASSERT_EQ(data_size, input.size());
  }
}

TEST(CheckStreamStatus, InvalidSegment) {
  auto input = SizeToBinaryData(0);
  auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(input.data(), input.size());
  ASSERT_EQ(status, memgraph::slk::StreamStatus::INVALID);
  ASSERT_EQ(stream_size, 0);
  ASSERT_EQ(data_size, 0);
}

TEST(CheckStreamStatus, StreamCompleteNoFileData) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto const input = GetRandomData(20);
  ASSERT_FALSE(builder.GetFileData());
  builder.Save(input.data(), input.size());
  ASSERT_FALSE(builder.GetFileData());
  ASSERT_TRUE(builder.Finalize().has_value());
  ASSERT_FALSE(builder.GetFileData());

  auto const res = memgraph::slk::CheckStreamStatus(buffer.data(), buffer.size());
  ASSERT_EQ(res.status, memgraph::slk::StreamStatus::COMPLETE);
}

TEST(CheckStreamStatus, WholeFileInSegment) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder(
      [&buffer](const uint8_t *data, size_t size, bool have_more) -> memgraph::slk::BuilderWriteFunction::result_type {
        for (size_t i = 0; i < size; ++i) {
          buffer.push_back(data[i]);
        }
        return {};
      });

  auto const file_data = GetRandomData(5);
  builder.PrepareForFileSending();
  ASSERT_TRUE(builder.SaveFileBuffer(file_data.data(), file_data.size()).has_value());
  ASSERT_TRUE(builder.Finalize().has_value());

  auto const res = memgraph::slk::CheckStreamStatus(buffer.data(), buffer.size());
  ASSERT_EQ(res.status, memgraph::slk::StreamStatus::NEW_FILE);
  ASSERT_EQ(res.pos, sizeof(memgraph::slk::SegmentSize));
}

TEST(CheckStreamStatus, FooterOnly) {
  uint8_t constexpr footer[] = {0x0, 0x0, 0x0, 0x0};

  {
    // Intentionally set size to some large value
    auto const res = memgraph::slk::CheckStreamStatus(footer, 30);
    ASSERT_EQ(res.status, memgraph::slk::StreamStatus::INVALID);
  }
  {
    // Intentionally set size to some large value. When we already processed some bytes, the stream should be considered
    // complete
    auto const res = memgraph::slk::CheckStreamStatus(footer, 30, std::nullopt, 10);
    ASSERT_EQ(res.status, memgraph::slk::StreamStatus::COMPLETE);
  }
}

TEST(CheckStreamStatus, FileDataStatus) {
  auto const file_data = GetRandomData(5);

  // When remaining file size is larger than what we can read, we return FILE_DATA
  auto const res = memgraph::slk::CheckStreamStatus(file_data.data(), file_data.size(), 20);
  ASSERT_EQ(res.status, memgraph::slk::StreamStatus::FILE_DATA);
}

TEST(CheckStreamStatus, FileDataAndFooter) {
  uint8_t constexpr footer[] = {0x0, 0x0, 0x0, 0x0};

  // When remaining file size is larger than what we can read, we return FILE_DATA
  auto const res = memgraph::slk::CheckStreamStatus(footer, sizeof(footer), 0);
  ASSERT_EQ(res.status, memgraph::slk::StreamStatus::INVALID);
}

TEST(CheckStreamStatus, PartialHeaderOnly) {
  // Provide fewer than sizeof(SegmentSize) bytes
  auto data = GetRandomData(sizeof(memgraph::slk::SegmentSize) - 1);
  auto const res = memgraph::slk::CheckStreamStatus(data.data(), data.size());
  ASSERT_EQ(res.status, memgraph::slk::StreamStatus::PARTIAL);
  ASSERT_GE(res.stream_size, memgraph::slk::kSegmentMaxTotalSize);
  ASSERT_EQ(res.encoded_data_size, 0);
}

TEST(CheckStreamStatus, SegmentLargerThanAvailable) {
  std::vector<uint8_t> buffer;
  memgraph::slk::SegmentSize fake_len = 100;
  buffer.resize(sizeof(fake_len));
  memcpy(buffer.data(), &fake_len, sizeof(fake_len));

  // Not enough payload bytes after header
  auto const res = memgraph::slk::CheckStreamStatus(buffer.data(), buffer.size());
  ASSERT_EQ(res.status, memgraph::slk::StreamStatus::PARTIAL);
}
