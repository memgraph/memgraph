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

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <random>
#include <vector>

#include "slk/streams.hpp"

#include "slk_common.hpp"

TEST(Builder, SingleSegment) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto input = GetRandomData(5);
  builder.Save(input.data(), input.size());
  builder.Finalize();

  ASSERT_EQ(buffer.size(), input.size() + 2 * sizeof(memgraph::slk::SegmentSize));

  auto splits =
      BufferToBinaryData(buffer.data(), buffer.size(),
                         {sizeof(memgraph::slk::SegmentSize), input.size(), sizeof(memgraph::slk::SegmentSize)});

  auto header_expected = SizeToBinaryData(input.size());
  ASSERT_EQ(splits[0], header_expected);

  ASSERT_EQ(splits[1], input);

  auto footer_expected = SizeToBinaryData(0);
  ASSERT_EQ(splits[2], footer_expected);
}

TEST(Builder, MultipleSegments) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 100);
  builder.Save(input.data(), input.size());
  builder.Finalize();

  ASSERT_EQ(buffer.size(), input.size() + 3 * sizeof(memgraph::slk::SegmentSize));

  auto splits = BufferToBinaryData(
      buffer.data(), buffer.size(),
      {sizeof(memgraph::slk::SegmentSize), memgraph::slk::kSegmentMaxDataSize, sizeof(memgraph::slk::SegmentSize),
       input.size() - memgraph::slk::kSegmentMaxDataSize, sizeof(memgraph::slk::SegmentSize)});

  auto datas =
      BufferToBinaryData(input.data(), input.size(),
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
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto input = GetRandomData(100);
  builder.PrepareForFileSending();
  ASSERT_TRUE(builder.GetFileData());
  builder.SaveFileBuffer(input.data(), input.size());
  builder.Finalize();

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
    memcpy(&footer, buffer.data() + input.size() + sizeof(memgraph::slk::SegmentSize),
           sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(footer, memgraph::slk::kFooter);
  }
}

TEST(Builder, FlushWithoutFile) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto input = GetRandomData(20);
  builder.Save(input.data(), input.size());
  builder.Finalize();
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
    memcpy(&footer, buffer.data() + input.size() + sizeof(memgraph::slk::SegmentSize),
           sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(footer, memgraph::slk::kFooter);
  }
}

TEST(Reader, InitializeReaderWithHave) {
  auto input = GetRandomData(5);

  memgraph::slk::Reader(input.data(), input.size(), input.size());
}

TEST(Reader, SingleSegment) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto input = GetRandomData(5);
  builder.Save(input.data(), input.size());
  builder.Finalize();

  // test with missing data
  for (size_t i = 0; i < buffer.size(); ++i) {
    memgraph::slk::Reader reader(buffer.data(), i);
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    ASSERT_THROW(
        {
          reader.Load(block, input.size());
          reader.Finalize();
        },
        memgraph::slk::SlkReaderException);
  }

  // test with complete data
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size());
    reader.Finalize();
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // test with leftover data
  {
    auto extended_buffer = BinaryData(buffer.data(), buffer.size()) + GetRandomData(5);
    memgraph::slk::Reader reader(extended_buffer.data(), extended_buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size());
    reader.Finalize();
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // read more data than there is in the stream
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    ASSERT_THROW(reader.Load(block, memgraph::slk::kSegmentMaxDataSize), memgraph::slk::SlkReaderException);
  }

  // don't consume all data from the stream
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size() / 2);
    ASSERT_THROW(reader.Finalize(), memgraph::slk::SlkReaderLeftoverDataException);
  }

  // read data with several loads
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    for (size_t i = 0; i < input.size(); ++i) {
      reader.Load(block + i, 1);
    }
    reader.Finalize();
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // modify the end mark
  buffer[buffer.size() - 1] = 1;
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize];
    reader.Load(block, input.size());
    ASSERT_THROW(reader.Finalize(), memgraph::slk::SlkReaderException);
  }
}

TEST(Reader, MultipleSegments) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 100);
  builder.Save(input.data(), input.size());
  builder.Finalize();

  // test with missing data
  for (size_t i = 0; i < buffer.size(); ++i) {
    memgraph::slk::Reader reader(buffer.data(), i);
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    ASSERT_THROW(
        {
          reader.Load(block, input.size());
          reader.Finalize();
        },
        memgraph::slk::SlkReaderException);
  }

  // test with complete data
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size());
    reader.Finalize();
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // test with leftover data
  {
    auto extended_buffer = BinaryData(buffer.data(), buffer.size()) + GetRandomData(5);
    memgraph::slk::Reader reader(extended_buffer.data(), extended_buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size());
    reader.Finalize();
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // read more data than there is in the stream
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    ASSERT_THROW(reader.Load(block, memgraph::slk::kSegmentMaxDataSize * 2), memgraph::slk::SlkReaderException);
  }

  // don't consume all data from the stream
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size() / 2);
    ASSERT_THROW(reader.Finalize(), memgraph::slk::SlkReaderLeftoverDataException);
  }

  // read data with several loads
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    for (size_t i = 0; i < input.size(); ++i) {
      reader.Load(block + i, 1);
    }
    reader.Finalize();
    auto output = BinaryData(block, input.size());
    ASSERT_EQ(output, input);
  }

  // modify the end mark
  buffer[buffer.size() - 1] = 1;
  {
    memgraph::slk::Reader reader(buffer.data(), buffer.size());
    uint8_t block[memgraph::slk::kSegmentMaxDataSize * 2];
    reader.Load(block, input.size());
    ASSERT_THROW(reader.Finalize(), memgraph::slk::SlkReaderException);
  }
}

TEST(CheckStreamStatus, SingleSegment) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto input = GetRandomData(5);
  builder.Save(input.data(), input.size());
  builder.Finalize();

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
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto input = GetRandomData(memgraph::slk::kSegmentMaxDataSize + 100);
  builder.Save(input.data(), input.size());
  builder.Finalize();

  // test with missing data
  for (size_t i = 0; i < sizeof(memgraph::slk::SegmentSize); ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, memgraph::slk::kSegmentMaxTotalSize);
    ASSERT_EQ(data_size, 0);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize);
       i < sizeof(memgraph::slk::SegmentSize) + memgraph::slk::kSegmentMaxDataSize; ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, memgraph::slk::kSegmentMaxTotalSize + sizeof(memgraph::slk::SegmentSize));
    ASSERT_EQ(data_size, 0);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize) + memgraph::slk::kSegmentMaxDataSize;
       i < sizeof(memgraph::slk::SegmentSize) * 2 + memgraph::slk::kSegmentMaxDataSize; ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, sizeof(memgraph::slk::SegmentSize) + memgraph::slk::kSegmentMaxDataSize +
                               memgraph::slk::kSegmentMaxTotalSize);
    ASSERT_EQ(data_size, memgraph::slk::kSegmentMaxDataSize);
  }
  for (size_t i = sizeof(memgraph::slk::SegmentSize) * 2 + memgraph::slk::kSegmentMaxDataSize;
       i < sizeof(memgraph::slk::SegmentSize) * 2 + input.size(); ++i) {
    auto [status, stream_size, data_size, pos] = memgraph::slk::CheckStreamStatus(buffer.data(), i);
    ASSERT_EQ(status, memgraph::slk::StreamStatus::PARTIAL);
    ASSERT_EQ(stream_size, sizeof(memgraph::slk::SegmentSize) * 2 + memgraph::slk::kSegmentMaxDataSize +
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
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto const input = GetRandomData(20);
  ASSERT_FALSE(builder.GetFileData());
  builder.Save(input.data(), input.size());
  ASSERT_FALSE(builder.GetFileData());
  builder.Finalize();
  ASSERT_FALSE(builder.GetFileData());

  auto const res = memgraph::slk::CheckStreamStatus(buffer.data(), buffer.size());
  ASSERT_EQ(res.status, memgraph::slk::StreamStatus::COMPLETE);
}

TEST(CheckStreamStatus, WholeFileInSegment) {
  std::vector<uint8_t> buffer;
  memgraph::slk::Builder builder([&buffer](const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) buffer.push_back(data[i]);
  });

  auto const file_data = GetRandomData(5);
  builder.PrepareForFileSending();
  builder.SaveFileBuffer(file_data.data(), file_data.size());
  builder.Finalize();

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
