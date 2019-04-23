#include <gtest/gtest.h>

#include "communication/rpc/serialization.hpp"

#define CREATE_PRIMITIVE_TEST(primitive_type, original_value, decoded_value) \
  {                                                                          \
    ASSERT_NE(original_value, decoded_value);                                \
    primitive_type original = original_value;                                \
    slk::Builder builder;                                                    \
    slk::Save(original, &builder);                                           \
    ASSERT_EQ(builder.size(), sizeof(primitive_type));                       \
    primitive_type decoded = decoded_value;                                  \
    slk::Reader reader(builder.data(), builder.size());                      \
    slk::Load(&decoded, &reader);                                            \
    ASSERT_EQ(original, decoded);                                            \
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
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(), sizeof(uint64_t) + original.size());
  std::string decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, VectorPrimitive) {
  std::vector<int> original{1, 2, 3, 4, 5};
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(), sizeof(uint64_t) + original.size() * sizeof(int));
  std::vector<int> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, VectorString) {
  std::vector<std::string> original{"hai hai hai", "nandare!"};
  slk::Builder builder;
  slk::Save(original, &builder);
  uint64_t size = sizeof(uint64_t);
  for (const auto &item : original) {
    size += sizeof(uint64_t) + item.size();
  }
  ASSERT_EQ(builder.size(), size);
  std::vector<std::string> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, SetPrimitive) {
  std::set<int> original{1, 2, 3, 4, 5};
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(), sizeof(uint64_t) + original.size() * sizeof(int));
  std::set<int> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, SetString) {
  std::set<std::string> original{"hai hai hai", "nandare!"};
  slk::Builder builder;
  slk::Save(original, &builder);
  uint64_t size = sizeof(uint64_t);
  for (const auto &item : original) {
    size += sizeof(uint64_t) + item.size();
  }
  ASSERT_EQ(builder.size(), size);
  std::set<std::string> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, MapPrimitive) {
  std::map<int, int> original{{1, 2}, {3, 4}, {5, 6}};
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(),
            sizeof(uint64_t) + original.size() * sizeof(int) * 2);
  std::map<int, int> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, MapString) {
  std::map<std::string, std::string> original{{"hai hai hai", "nandare!"}};
  slk::Builder builder;
  slk::Save(original, &builder);
  uint64_t size = sizeof(uint64_t);
  for (const auto &item : original) {
    size += sizeof(uint64_t) + item.first.size();
    size += sizeof(uint64_t) + item.second.size();
  }
  ASSERT_EQ(builder.size(), size);
  std::map<std::string, std::string> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, UnorderedMapPrimitive) {
  std::unordered_map<int, int> original{{1, 2}, {3, 4}, {5, 6}};
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(),
            sizeof(uint64_t) + original.size() * sizeof(int) * 2);
  std::unordered_map<int, int> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, UnorderedMapString) {
  std::unordered_map<std::string, std::string> original{
      {"hai hai hai", "nandare!"}};
  slk::Builder builder;
  slk::Save(original, &builder);
  uint64_t size = sizeof(uint64_t);
  for (const auto &item : original) {
    size += sizeof(uint64_t) + item.first.size();
    size += sizeof(uint64_t) + item.second.size();
  }
  ASSERT_EQ(builder.size(), size);
  std::unordered_map<std::string, std::string> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, UniquePtrEmpty) {
  std::unique_ptr<std::string> original;
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(), sizeof(bool));
  std::unique_ptr<std::string> decoded =
      std::make_unique<std::string>("nandare!");
  ASSERT_NE(decoded.get(), nullptr);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(decoded.get(), nullptr);
}

TEST(SlkCore, UniquePtrFull) {
  std::unique_ptr<std::string> original =
      std::make_unique<std::string>("nandare!");
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(),
            sizeof(bool) + sizeof(uint64_t) + original.get()->size());
  std::unique_ptr<std::string> decoded;
  ASSERT_EQ(decoded.get(), nullptr);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_NE(decoded.get(), nullptr);
  ASSERT_EQ(*original.get(), *decoded.get());
}

TEST(SlkCore, OptionalPrimitiveEmpty) {
  std::optional<int> original;
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(), sizeof(bool));
  std::optional<int> decoded = 5;
  ASSERT_NE(decoded, std::nullopt);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(decoded, std::nullopt);
}

TEST(SlkCore, OptionalPrimitiveFull) {
  std::optional<int> original = 5;
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(), sizeof(bool) + sizeof(int));
  std::optional<int> decoded;
  ASSERT_EQ(decoded, std::nullopt);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_NE(decoded, std::nullopt);
  ASSERT_EQ(*original, *decoded);
}

TEST(SlkCore, OptionalStringEmpty) {
  std::optional<std::string> original;
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(), sizeof(bool));
  std::optional<std::string> decoded = "nandare!";
  ASSERT_NE(decoded, std::nullopt);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(decoded, std::nullopt);
}

TEST(SlkCore, OptionalStringFull) {
  std::optional<std::string> original = "nandare!";
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(), sizeof(bool) + sizeof(uint64_t) + original->size());
  std::optional<std::string> decoded;
  ASSERT_EQ(decoded, std::nullopt);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_NE(decoded, std::nullopt);
  ASSERT_EQ(*original, *decoded);
}

TEST(SlkCore, Pair) {
  std::pair<std::string, int> original{"nandare!", 5};
  slk::Builder builder;
  slk::Save(original, &builder);
  ASSERT_EQ(builder.size(),
            sizeof(uint64_t) + original.first.size() + sizeof(int));
  std::pair<std::string, int> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, SharedPtrEmpty) {
  std::shared_ptr<std::string> original;
  std::vector<std::string *> saved;
  slk::Builder builder;
  slk::Save(original, &builder, &saved);
  ASSERT_EQ(builder.size(), sizeof(bool));
  std::shared_ptr<std::string> decoded =
      std::make_shared<std::string>("nandare!");
  std::vector<std::shared_ptr<std::string>> loaded;
  ASSERT_NE(decoded.get(), nullptr);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader, &loaded);
  ASSERT_EQ(decoded.get(), nullptr);
  ASSERT_EQ(saved.size(), 0);
  ASSERT_EQ(loaded.size(), 0);
}

TEST(SlkCore, SharedPtrFull) {
  std::shared_ptr<std::string> original =
      std::make_shared<std::string>("nandare!");
  std::vector<std::string *> saved;
  slk::Builder builder;
  slk::Save(original, &builder, &saved);
  ASSERT_EQ(builder.size(),
            sizeof(bool) * 2 + sizeof(uint64_t) + original.get()->size());
  std::shared_ptr<std::string> decoded;
  std::vector<std::shared_ptr<std::string>> loaded;
  ASSERT_EQ(decoded.get(), nullptr);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader, &loaded);
  ASSERT_NE(decoded.get(), nullptr);
  ASSERT_EQ(*original.get(), *decoded.get());
  ASSERT_EQ(saved.size(), 1);
  ASSERT_EQ(loaded.size(), 1);
}

TEST(SlkCore, SharedPtrMultiple) {
  std::shared_ptr<std::string> ptr1 = std::make_shared<std::string>("nandare!");
  std::shared_ptr<std::string> ptr2;
  std::shared_ptr<std::string> ptr3 =
      std::make_shared<std::string>("hai hai hai");
  std::vector<std::string *> saved;

  slk::Builder builder;
  slk::Save(ptr1, &builder, &saved);
  slk::Save(ptr2, &builder, &saved);
  slk::Save(ptr3, &builder, &saved);
  slk::Save(ptr1, &builder, &saved);
  slk::Save(ptr3, &builder, &saved);

  // clang-format off
  ASSERT_EQ(builder.size(),
            sizeof(bool) * 2 + sizeof(uint64_t) + ptr1.get()->size() +
            sizeof(bool) +
            sizeof(bool) * 2 + sizeof(uint64_t) + ptr3.get()->size() +
            sizeof(bool) * 2 + sizeof(uint64_t) +
            sizeof(bool) * 2 + sizeof(uint64_t));
  // clang-format on

  std::shared_ptr<std::string> dec1, dec2, dec3, dec4, dec5;
  std::vector<std::shared_ptr<std::string>> loaded;

  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&dec1, &reader, &loaded);
  slk::Load(&dec2, &reader, &loaded);
  slk::Load(&dec3, &reader, &loaded);
  slk::Load(&dec4, &reader, &loaded);
  slk::Load(&dec5, &reader, &loaded);

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
}

TEST(SlkCore, Complex) {
  std::unique_ptr<std::vector<std::optional<std::string>>> original =
      std::make_unique<std::vector<std::optional<std::string>>>();
  original.get()->push_back("nandare!");
  original.get()->push_back(std::nullopt);
  original.get()->push_back("hai hai hai");

  slk::Builder builder;
  slk::Save(original, &builder);

  // clang-format off
  ASSERT_EQ(builder.size(),
            sizeof(bool) +
              sizeof(uint64_t) +
                sizeof(bool) + sizeof(uint64_t) + (*original.get())[0]->size() +
                sizeof(bool) +
                sizeof(bool) + sizeof(uint64_t) + (*original.get())[2]->size());
  // clang-format on

  std::unique_ptr<std::vector<std::optional<std::string>>> decoded;
  ASSERT_EQ(decoded.get(), nullptr);
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_NE(decoded.get(), nullptr);
  ASSERT_EQ(*original.get(), *decoded.get());
}

struct Foo {
  std::string name;
  std::optional<int> value;
};

bool operator==(const Foo &a, const Foo &b) {
  return a.name == b.name && a.value == b.value;
}

namespace slk {
void Save(const Foo &obj, Builder *builder) {
  Save(obj.name, builder);
  Save(obj.value, builder);
}

void Load(Foo *obj, Reader *reader) {
  Load(&obj->name, reader);
  Load(&obj->value, reader);
}
}  // namespace slk

TEST(SlkCore, VectorStruct) {
  std::vector<Foo> original;
  original.push_back({"hai hai hai", 5});
  original.push_back({"nandare!", std::nullopt});

  slk::Builder builder;
  slk::Save(original, &builder);

  // clang-format off
  ASSERT_EQ(builder.size(),
            sizeof(uint64_t) +
              sizeof(uint64_t) + original[0].name.size() + sizeof(bool) + sizeof(int) +
              sizeof(uint64_t) + original[1].name.size() + sizeof(bool));
  // clang-format on

  std::vector<Foo> decoded;
  slk::Reader reader(builder.data(), builder.size());
  slk::Load(&decoded, &reader);
  ASSERT_EQ(original, decoded);
}

TEST(SlkCore, VectorSharedPtr) {
  std::shared_ptr<std::string> ptr1 = std::make_shared<std::string>("nandare!");
  std::shared_ptr<std::string> ptr2;
  std::shared_ptr<std::string> ptr3 =
      std::make_shared<std::string>("hai hai hai");

  std::vector<std::shared_ptr<std::string>> original;
  std::vector<std::string *> saved;

  original.push_back(ptr1);
  original.push_back(ptr2);
  original.push_back(ptr3);
  original.push_back(ptr1);
  original.push_back(ptr3);

  slk::Builder builder;
  slk::Save<std::shared_ptr<std::string>>(
      original, &builder, [&saved](const auto &item, auto *builder) {
        Save(item, builder, &saved);
      });

  std::vector<std::shared_ptr<std::string>> decoded;
  std::vector<std::shared_ptr<std::string>> loaded;

  slk::Reader reader(builder.data(), builder.size());
  slk::Load<std::shared_ptr<std::string>>(
      &decoded, &reader,
      [&loaded](auto *item, auto *reader) { Load(item, reader, &loaded); });

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
  std::optional<std::shared_ptr<std::string>> original =
      std::make_shared<std::string>("nandare!");
  std::vector<std::string *> saved;

  slk::Builder builder;
  slk::Save<std::shared_ptr<std::string>>(
      original, &builder, [&saved](const auto &item, auto *builder) {
        Save(item, builder, &saved);
      });

  std::optional<std::shared_ptr<std::string>> decoded;
  std::vector<std::shared_ptr<std::string>> loaded;

  slk::Reader reader(builder.data(), builder.size());
  slk::Load<std::shared_ptr<std::string>>(
      &decoded, &reader,
      [&loaded](auto *item, auto *reader) { Load(item, reader, &loaded); });

  ASSERT_NE(decoded, std::nullopt);

  ASSERT_EQ(saved.size(), 1);
  ASSERT_EQ(loaded.size(), 1);

  ASSERT_EQ(*decoded->get(), *original->get());
}
