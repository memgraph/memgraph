// Copyright 2024 Memgraph Ltd.
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

#include "slk/serialization.hpp"

#include "slk_common.hpp"

#define CREATE_PRIMITIVE_TEST(primitive_type, original_value, decoded_value) \
  {                                                                          \
    ASSERT_NE(original_value, decoded_value);                                \
    primitive_type original = original_value;                                \
    memgraph::slk::Loopback loopback;                                        \
    auto builder = loopback.GetBuilder();                                    \
    memgraph::slk::Save(original, builder);                                  \
    primitive_type decoded = decoded_value;                                  \
    auto reader = loopback.GetReader();                                      \
    memgraph::slk::Load(&decoded, reader);                                   \
    ASSERT_EQ(original, decoded);                                            \
    ASSERT_EQ(loopback.size(), sizeof(primitive_type));                      \
  }

TEST(SlkCore, Primitive) {
  CREATE_PRIMITIVE_TEST(bool, true, false);
  CREATE_PRIMITIVE_TEST(int8_t, 0x12, 0);
  CREATE_PRIMITIVE_TEST(uint8_t, 0x12, 0);
  CREATE_PRIMITIVE_TEST(int16_t, 0x1234, 0);
  CREATE_PRIMITIVE_TEST(uint16_t, 0x1234, 0);
  CREATE_PRIMITIVE_TEST(int32_t, 0x12345678, 0);
  CREATE_PRIMITIVE_TEST(uint32_t, 0x12345678, 0);
  CREATE_PRIMITIVE_TEST(int64_t, 0x1234567890abcdef, 0);
  CREATE_PRIMITIVE_TEST(uint64_t, 0x1234567890abcdef, 0);
  CREATE_PRIMITIVE_TEST(float, 1.23456789, 0);
  CREATE_PRIMITIVE_TEST(double, 1234567890.1234567890, 0);
}

TEST(SlkCore, String) {
  std::string original = "hello world";
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::string decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), sizeof(uint64_t) + original.size());
}

TEST(SlkCore, ConstStringLiteral) {
  const char *original = "hello world";
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::string decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), sizeof(uint64_t) + strlen(original));
}

TEST(SlkCore, VectorPrimitive) {
  std::vector<int> original{1, 2, 3, 4, 5};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::vector<int> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), sizeof(uint64_t) + original.size() * sizeof(int));
}

TEST(SlkCore, VectorString) {
  std::vector<std::string> original{"hai hai hai", "nandare!"};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  uint64_t size = sizeof(uint64_t);
  for (const auto &item : original) {
    size += sizeof(uint64_t) + item.size();
  }
  std::vector<std::string> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), size);
}

TEST(SlkCore, SetPrimitive) {
  std::set<int> original{1, 2, 3, 4, 5};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::set<int> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), sizeof(uint64_t) + original.size() * sizeof(int));
}

TEST(SlkCore, SetString) {
  std::set<std::string, std::less<>> original{"hai hai hai", "nandare!"};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  uint64_t size = sizeof(uint64_t);
  for (const auto &item : original) {
    size += sizeof(uint64_t) + item.size();
  }
  std::set<std::string, std::less<>> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), size);
}

TEST(SlkCore, MapPrimitive) {
  std::map<int, int> original{{1, 2}, {3, 4}, {5, 6}};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::map<int, int> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), sizeof(uint64_t) + original.size() * sizeof(int) * 2);
}

TEST(SlkCore, MapString) {
  std::map<std::string, std::string> original{{"hai hai hai", "nandare!"}};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  uint64_t size = sizeof(uint64_t);
  for (const auto &item : original) {
    size += sizeof(uint64_t) + item.first.size();
    size += sizeof(uint64_t) + item.second.size();
  }
  std::map<std::string, std::string> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), size);
}

TEST(SlkCore, UnorderedMapPrimitive) {
  std::unordered_map<int, int> original{{1, 2}, {3, 4}, {5, 6}};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::unordered_map<int, int> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), sizeof(uint64_t) + original.size() * sizeof(int) * 2);
}

TEST(SlkCore, UnorderedMapString) {
  std::unordered_map<std::string, std::string> original{{"hai hai hai", "nandare!"}};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  uint64_t size = sizeof(uint64_t);
  for (const auto &item : original) {
    size += sizeof(uint64_t) + item.first.size();
    size += sizeof(uint64_t) + item.second.size();
  }
  std::unordered_map<std::string, std::string> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), size);
}

TEST(SlkCore, UniquePtrEmpty) {
  std::unique_ptr<std::string> original;
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::unique_ptr<std::string> decoded = std::make_unique<std::string>("nandare!");
  ASSERT_NE(decoded.get(), nullptr);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(decoded.get(), nullptr);
  ASSERT_EQ(loopback.size(), sizeof(bool));
}

TEST(SlkCore, UniquePtrFull) {
  std::unique_ptr<std::string> original = std::make_unique<std::string>("nandare!");
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::unique_ptr<std::string> decoded;
  ASSERT_EQ(decoded.get(), nullptr);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_NE(decoded.get(), nullptr);
  ASSERT_EQ(*original.get(), *decoded.get());
  ASSERT_EQ(loopback.size(), sizeof(bool) + sizeof(uint64_t) + original.get()->size());
}

TEST(SlkCore, OptionalPrimitiveEmpty) {
  std::optional<int> original;
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::optional<int> decoded = 5;
  ASSERT_NE(decoded, std::nullopt);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(decoded, std::nullopt);
  ASSERT_EQ(loopback.size(), sizeof(bool));
}

TEST(SlkCore, OptionalPrimitiveFull) {
  std::optional<int> original = 5;
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::optional<int> decoded;
  ASSERT_EQ(decoded, std::nullopt);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_NE(decoded, std::nullopt);
  ASSERT_EQ(*original, *decoded);
  ASSERT_EQ(loopback.size(), sizeof(bool) + sizeof(int));
}

TEST(SlkCore, OptionalStringEmpty) {
  std::optional<std::string> original;
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::optional<std::string> decoded = "nandare!";
  ASSERT_NE(decoded, std::nullopt);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(decoded, std::nullopt);
  ASSERT_EQ(loopback.size(), sizeof(bool));
}

TEST(SlkCore, OptionalStringFull) {
  std::optional<std::string> original = "nandare!";
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::optional<std::string> decoded;
  ASSERT_EQ(decoded, std::nullopt);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_NE(decoded, std::nullopt);
  ASSERT_EQ(*original, *decoded);
  ASSERT_EQ(loopback.size(), sizeof(bool) + sizeof(uint64_t) + original->size());
}

TEST(SlkCore, Pair) {
  std::pair<std::string, int> original{"nandare!", 5};
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);
  std::pair<std::string, int> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);
  ASSERT_EQ(loopback.size(), sizeof(uint64_t) + original.first.size() + sizeof(int));
}

TEST(SlkCore, SharedPtrEmpty) {
  std::shared_ptr<std::string> original;
  std::vector<std::string *> saved;
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder, &saved);
  std::shared_ptr<std::string> decoded = std::make_shared<std::string>("nandare!");
  std::vector<std::shared_ptr<std::string>> loaded;
  ASSERT_NE(decoded.get(), nullptr);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader, &loaded);
  ASSERT_EQ(decoded.get(), nullptr);
  ASSERT_EQ(saved.size(), 0);
  ASSERT_EQ(loaded.size(), 0);
  ASSERT_EQ(loopback.size(), sizeof(bool));
}

TEST(SlkCore, SharedPtrFull) {
  std::shared_ptr<std::string> original = std::make_shared<std::string>("nandare!");
  std::vector<std::string *> saved;
  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder, &saved);
  std::shared_ptr<std::string> decoded;
  std::vector<std::shared_ptr<std::string>> loaded;
  ASSERT_EQ(decoded.get(), nullptr);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader, &loaded);
  ASSERT_NE(decoded.get(), nullptr);
  ASSERT_EQ(*original.get(), *decoded.get());
  ASSERT_EQ(saved.size(), 1);
  ASSERT_EQ(loaded.size(), 1);
  ASSERT_EQ(loopback.size(), sizeof(bool) * 2 + sizeof(uint64_t) + original.get()->size());
}

TEST(SlkCore, SharedPtrMultiple) {
  std::shared_ptr<std::string> ptr1 = std::make_shared<std::string>("nandare!");
  std::shared_ptr<std::string> ptr2;
  std::shared_ptr<std::string> ptr3 = std::make_shared<std::string>("hai hai hai");
  std::vector<std::string *> saved;

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(ptr1, builder, &saved);
  memgraph::slk::Save(ptr2, builder, &saved);
  memgraph::slk::Save(ptr3, builder, &saved);
  memgraph::slk::Save(ptr1, builder, &saved);
  memgraph::slk::Save(ptr3, builder, &saved);

  std::shared_ptr<std::string> dec1, dec2, dec3, dec4, dec5;
  std::vector<std::shared_ptr<std::string>> loaded;

  auto reader = loopback.GetReader();
  memgraph::slk::Load(&dec1, reader, &loaded);
  memgraph::slk::Load(&dec2, reader, &loaded);
  memgraph::slk::Load(&dec3, reader, &loaded);
  memgraph::slk::Load(&dec4, reader, &loaded);
  memgraph::slk::Load(&dec5, reader, &loaded);

  ASSERT_EQ(saved.size(), 2);
  ASSERT_EQ(loaded.size(), 2);

  ASSERT_NE(dec1.get(), nullptr);
  ASSERT_EQ(dec2.get(), nullptr);
  ASSERT_NE(dec3.get(), nullptr);
  ASSERT_NE(dec4.get(), nullptr);
  ASSERT_NE(dec5.get(), nullptr);

  ASSERT_EQ(*dec1.get(), *ptr1.get());
  ASSERT_EQ(*dec3.get(), *ptr3.get());
  ASSERT_EQ(*dec4.get(), *ptr1.get());
  ASSERT_EQ(*dec5.get(), *ptr3.get());

  ASSERT_NE(dec1.get(), dec3.get());

  ASSERT_EQ(dec4.get(), dec1.get());
  ASSERT_EQ(dec5.get(), dec3.get());

  // clang-format off
  ASSERT_EQ(loopback.size(),
            sizeof(bool) * 2 + sizeof(uint64_t) + ptr1.get()->size() +
            sizeof(bool) +
            sizeof(bool) * 2 + sizeof(uint64_t) + ptr3.get()->size() +
            sizeof(bool) * 2 + sizeof(uint64_t) +
            sizeof(bool) * 2 + sizeof(uint64_t));
  // clang-format on
}

TEST(SlkCore, SharedPtrInvalid) {
  std::shared_ptr<std::string> ptr = std::make_shared<std::string>("nandare!");
  std::vector<std::string *> saved;

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(ptr, builder, &saved);
  // Here we mess with the `saved` vector to cause an invalid index to be
  // written to the SLK stream so that we can check the error handling in the
  // `Load` function later.
  saved.insert(saved.begin(), nullptr);
  // Save the pointer again with an invalid index.
  memgraph::slk::Save(ptr, builder, &saved);

  std::shared_ptr<std::string> dec1, dec2;
  std::vector<std::shared_ptr<std::string>> loaded;

  auto reader = loopback.GetReader();
  memgraph::slk::Load(&dec1, reader, &loaded);
  ASSERT_THROW(memgraph::slk::Load(&dec2, reader, &loaded), memgraph::slk::SlkDecodeException);
}

TEST(SlkCore, Complex) {
  std::unique_ptr<std::vector<std::optional<std::string>>> original =
      std::make_unique<std::vector<std::optional<std::string>>>();
  original.get()->push_back("nandare!");
  original.get()->push_back(std::nullopt);
  original.get()->push_back("hai hai hai");

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  std::unique_ptr<std::vector<std::optional<std::string>>> decoded;
  ASSERT_EQ(decoded.get(), nullptr);
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_NE(decoded.get(), nullptr);
  ASSERT_EQ(*original.get(), *decoded.get());

  // clang-format off
  ASSERT_EQ(loopback.size(),
            sizeof(bool) +
              sizeof(uint64_t) +
                sizeof(bool) + sizeof(uint64_t) + (*original.get())[0]->size() +
                sizeof(bool) +
                sizeof(bool) + sizeof(uint64_t) + (*original.get())[2]->size());
  // clang-format on
}

struct Foo {
  std::string name;
  std::optional<int> value;
};

bool operator==(const Foo &a, const Foo &b) { return a.name == b.name && a.value == b.value; }

namespace memgraph::slk {
void Save(const Foo &obj, Builder *builder) {
  Save(obj.name, builder);
  Save(obj.value, builder);
}

void Load(Foo *obj, Reader *reader) {
  Load(&obj->name, reader);
  Load(&obj->value, reader);
}
}  // namespace memgraph::slk

TEST(SlkCore, VectorStruct) {
  std::vector<Foo> original;
  original.push_back({"hai hai hai", 5});
  original.push_back({"nandare!", std::nullopt});

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save(original, builder);

  std::vector<Foo> decoded;
  auto reader = loopback.GetReader();
  memgraph::slk::Load(&decoded, reader);
  ASSERT_EQ(original, decoded);

  // clang-format off
  ASSERT_EQ(loopback.size(),
            sizeof(uint64_t) +
              sizeof(uint64_t) + original[0].name.size() + sizeof(bool) +
sizeof(int) + sizeof(uint64_t) + original[1].name.size() + sizeof(bool));
  // clang-format on
}

TEST(SlkCore, VectorSharedPtr) {
  std::shared_ptr<std::string> ptr1 = std::make_shared<std::string>("nandare!");
  std::shared_ptr<std::string> ptr2;
  std::shared_ptr<std::string> ptr3 = std::make_shared<std::string>("hai hai hai");

  std::vector<std::shared_ptr<std::string>> original;
  std::vector<std::string *> saved;

  original.push_back(ptr1);
  original.push_back(ptr2);
  original.push_back(ptr3);
  original.push_back(ptr1);
  original.push_back(ptr3);

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save<std::shared_ptr<std::string>>(
      original, builder, [&saved](const auto &item, auto *builder) { Save(item, builder, &saved); });

  std::vector<std::shared_ptr<std::string>> decoded;
  std::vector<std::shared_ptr<std::string>> loaded;

  auto reader = loopback.GetReader();
  memgraph::slk::Load<std::shared_ptr<std::string>>(
      &decoded, reader, [&loaded](auto *item, auto *reader) { Load(item, reader, &loaded); });

  ASSERT_EQ(decoded.size(), original.size());

  ASSERT_EQ(saved.size(), 2);
  ASSERT_EQ(loaded.size(), saved.size());

  ASSERT_EQ(*decoded[0].get(), *ptr1.get());
  ASSERT_EQ(decoded[1].get(), nullptr);
  ASSERT_EQ(*decoded[2].get(), *ptr3.get());
  ASSERT_EQ(*decoded[3].get(), *ptr1.get());
  ASSERT_EQ(*decoded[4].get(), *ptr3.get());

  ASSERT_NE(decoded[0].get(), decoded[2].get());

  ASSERT_EQ(decoded[3].get(), decoded[0].get());
  ASSERT_EQ(decoded[4].get(), decoded[2].get());
}

TEST(SlkCore, OptionalSharedPtr) {
  std::optional<std::shared_ptr<std::string>> original = std::make_shared<std::string>("nandare!");
  std::vector<std::string *> saved;

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save<std::shared_ptr<std::string>>(
      original, builder, [&saved](const auto &item, auto *builder) { Save(item, builder, &saved); });

  std::optional<std::shared_ptr<std::string>> decoded;
  std::vector<std::shared_ptr<std::string>> loaded;

  auto reader = loopback.GetReader();
  memgraph::slk::Load<std::shared_ptr<std::string>>(
      &decoded, reader, [&loaded](auto *item, auto *reader) { Load(item, reader, &loaded); });

  ASSERT_NE(decoded, std::nullopt);

  ASSERT_EQ(saved.size(), 1);
  ASSERT_EQ(loaded.size(), 1);

  ASSERT_EQ(*decoded->get(), *original->get());
}

TEST(SlkCore, OptionalSharedPtrEmpty) {
  std::optional<std::shared_ptr<std::string>> original;
  std::vector<std::string *> saved;

  memgraph::slk::Loopback loopback;
  auto builder = loopback.GetBuilder();
  memgraph::slk::Save<std::shared_ptr<std::string>>(
      original, builder, [&saved](const auto &item, auto *builder) { Save(item, builder, &saved); });

  std::optional<std::shared_ptr<std::string>> decoded;
  std::vector<std::shared_ptr<std::string>> loaded;

  auto reader = loopback.GetReader();
  memgraph::slk::Load<std::shared_ptr<std::string>>(
      &decoded, reader, [&loaded](auto *item, auto *reader) { Load(item, reader, &loaded); });

  ASSERT_EQ(decoded, std::nullopt);

  ASSERT_EQ(saved.size(), 0);
  ASSERT_EQ(loaded.size(), 0);
}
