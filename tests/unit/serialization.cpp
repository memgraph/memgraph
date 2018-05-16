#include <experimental/optional>
#include <sstream>

#include "gtest/gtest.h"

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "capnp/message.h"
#include "utils/serialization.hpp"

using std::experimental::optional;
using std::string_literals::operator""s;

TEST(Serialization, Optional) {
  std::stringstream ss;

  optional<int> x1 = {};
  optional<int> x2 = 42;
  optional<int> y1, y2;

  {
    boost::archive::binary_oarchive ar(ss);
    ar << x1;
    ar << x2;
  }

  {
    boost::archive::binary_iarchive ar(ss);
    ar >> y1;
    ar >> y2;
  }

  EXPECT_EQ(x1, y1);
  EXPECT_EQ(x2, y2);
}

TEST(Serialization, Tuple) {
  std::stringstream ss;

  auto x1 = std::make_tuple("foo"s, 42, std::experimental::make_optional(3.14));
  auto x2 = std::make_tuple();
  auto x3 = std::make_tuple(1, 2, 3, 4, 5);

  decltype(x1) y1;
  decltype(x2) y2;
  decltype(x3) y3;

  {
    boost::archive::binary_oarchive ar(ss);
    ar << x1;
    ar << x2;
    ar << x3;
  }

  {
    boost::archive::binary_iarchive ar(ss);
    ar >> y1;
    ar >> y2;
    ar >> y3;
  }

  EXPECT_EQ(x1, y1);
  EXPECT_EQ(x2, y2);
  EXPECT_EQ(x3, y3);
}

void CheckOptionalInt(const std::experimental::optional<int> &x1) {
  ::capnp::MallocMessageBuilder message;
  std::experimental::optional<int> y1;
  {
    auto builder =
        message.initRoot<utils::capnp::Optional<utils::capnp::BoxInt32>>();
    auto save = [](utils::capnp::BoxInt32::Builder *builder, int value) {
      builder->setValue(value);
    };
    utils::SaveOptional<utils::capnp::BoxInt32, int>(x1, &builder, save);
  }

  {
    auto reader =
        message.getRoot<utils::capnp::Optional<utils::capnp::BoxInt32>>();
    auto load = [](const utils::capnp::BoxInt32::Reader &reader) -> int {
      return reader.getValue();
    };
    y1 = utils::LoadOptional<utils::capnp::BoxInt32, int>(reader, load);
  }

  EXPECT_EQ(x1, y1);
}

TEST(Serialization, CapnpOptional) {
  std::experimental::optional<int> x1 = {};
  std::experimental::optional<int> x2 = 42;

  CheckOptionalInt(x1);
  CheckOptionalInt(x2);
}

TEST(Serialization, CapnpOptionalNonCopyable) {
  std::experimental::optional<std::unique_ptr<int>> data =
      std::make_unique<int>(5);
  ::capnp::MallocMessageBuilder message;
  {
    auto builder = message.initRoot<utils::capnp::Optional<
        utils::capnp::UniquePtr<utils::capnp::BoxInt32>>>();
    auto save = [](auto *ptr_builder, const auto &data) {
      auto save_int = [](auto *int_builder, int value) {
        int_builder->setValue(value);
      };
      utils::SaveUniquePtr<utils::capnp::BoxInt32, int>(data, ptr_builder,
                                                        save_int);
    };
    utils::SaveOptional<utils::capnp::UniquePtr<utils::capnp::BoxInt32>,
                        std::unique_ptr<int>>(data, &builder, save);
  }
  std::experimental::optional<std::unique_ptr<int>> element;
  {
    auto reader = message.getRoot<utils::capnp::Optional<
        utils::capnp::UniquePtr<utils::capnp::BoxInt32>>>();
    auto load = [](const auto &ptr_reader) {
      auto load_int = [](const auto &int_reader) {
        return new int(int_reader.getValue());
      };
      return utils::LoadUniquePtr<utils::capnp::BoxInt32, int>(ptr_reader,
                                                               load_int);
    };
    element =
        utils::LoadOptional<utils::capnp::UniquePtr<utils::capnp::BoxInt32>,
                            std::unique_ptr<int>>(reader, load);
  }
  EXPECT_EQ(*element.value(), 5);
}

void CheckUniquePtrInt(const std::unique_ptr<int> &x1) {
  ::capnp::MallocMessageBuilder message;
  std::unique_ptr<int> y1;
  {
    auto builder =
        message.initRoot<utils::capnp::UniquePtr<utils::capnp::BoxInt32>>();
    auto save = [](utils::capnp::BoxInt32::Builder *builder, int value) {
      builder->setValue(value);
    };
    utils::SaveUniquePtr<utils::capnp::BoxInt32, int>(x1, &builder, save);
  }
  {
    auto reader =
        message.getRoot<utils::capnp::UniquePtr<utils::capnp::BoxInt32>>();
    auto load = [](const auto &int_reader) {
      return new int(int_reader.getValue());
    };
    y1 = utils::LoadUniquePtr<utils::capnp::BoxInt32, int>(reader, load);
  }
  if (!x1)
    EXPECT_EQ(y1, nullptr);
  else
    EXPECT_EQ(*x1, *y1);
}

TEST(Serialization, CapnpUniquePtr) {
  auto x1 = std::make_unique<int>(42);
  std::unique_ptr<int> x2;

  CheckUniquePtrInt(x1);
  CheckUniquePtrInt(x2);
}

TEST(Serialization, CapnpUniquePtrNonCopyable) {
  std::unique_ptr<std::unique_ptr<int>> data =
      std::make_unique<std::unique_ptr<int>>(std::make_unique<int>(5));
  ::capnp::MallocMessageBuilder message;
  {
    auto builder = message.initRoot<utils::capnp::UniquePtr<
        utils::capnp::UniquePtr<utils::capnp::BoxInt32>>>();
    auto save = [](auto *ptr_builder, const auto &data) {
      auto save_int = [](auto *int_builder, int value) {
        int_builder->setValue(value);
      };
      utils::SaveUniquePtr<utils::capnp::BoxInt32, int>(data, ptr_builder,
                                                        save_int);
    };
    utils::SaveUniquePtr<utils::capnp::UniquePtr<utils::capnp::BoxInt32>,
                         std::unique_ptr<int>>(data, &builder, save);
  }
  std::unique_ptr<std::unique_ptr<int>> element;
  {
    auto reader = message.getRoot<utils::capnp::UniquePtr<
        utils::capnp::UniquePtr<utils::capnp::BoxInt32>>>();
    auto load = [](const auto &ptr_reader) {
      auto load_int = [](const auto &int_reader) {
        return new int(int_reader.getValue());
      };
      return new std::unique_ptr<int>(
          utils::LoadUniquePtr<utils::capnp::BoxInt32, int>(ptr_reader,
                                                            load_int));
    };
    element =
        utils::LoadUniquePtr<utils::capnp::UniquePtr<utils::capnp::BoxInt32>,
                             std::unique_ptr<int>>(reader, load);
  }
  EXPECT_EQ(**element, 5);
}

TEST(Serialization, CapnpSharedPtr) {
  std::vector<int *> saved_pointers;
  auto p1 = std::make_shared<int>(5);
  std::shared_ptr<int> p2;
  std::vector<std::shared_ptr<int>> pointers{p1, p1, p2};
  ::capnp::MallocMessageBuilder message;
  {
    auto builders = message.initRoot<
        ::capnp::List<utils::capnp::SharedPtr<utils::capnp::BoxInt32>>>(
        pointers.size());
    auto save = [](utils::capnp::BoxInt32::Builder *builder, int value) {
      builder->setValue(value);
    };
    for (size_t i = 0; i < pointers.size(); ++i) {
      auto ptr_builder = builders[i];
      utils::SaveSharedPtr<utils::capnp::BoxInt32, int>(
          pointers[i], &ptr_builder, save, &saved_pointers);
    }
  }
  EXPECT_EQ(saved_pointers.size(), 1);
  std::vector<std::pair<uint64_t, std::shared_ptr<int>>> loaded_pointers;
  std::vector<std::shared_ptr<int>> elements;
  {
    auto reader = message.getRoot<
        ::capnp::List<utils::capnp::SharedPtr<utils::capnp::BoxInt32>>>();
    auto load = [](const auto &int_reader) {
      return new int(int_reader.getValue());
    };
    for (const auto ptr_reader : reader) {
      elements.emplace_back(utils::LoadSharedPtr<utils::capnp::BoxInt32, int>(
          ptr_reader, load, &loaded_pointers));
    }
  }
  EXPECT_EQ(loaded_pointers.size(), 1);
  EXPECT_EQ(elements.size(), 3);
  EXPECT_EQ(*elements[0], 5);
  EXPECT_EQ(*elements[0], *elements[1]);
  EXPECT_EQ(elements[2].get(), nullptr);
}

TEST(Serialization, CapnpSharedPtrNonCopyable) {
  std::shared_ptr<std::unique_ptr<int>> data =
      std::make_shared<std::unique_ptr<int>>(std::make_unique<int>(5));
  std::vector<std::unique_ptr<int> *> saved_pointers;
  ::capnp::MallocMessageBuilder message;
  {
    auto builder = message.initRoot<utils::capnp::SharedPtr<
        utils::capnp::UniquePtr<utils::capnp::BoxInt32>>>();
    auto save = [](auto *ptr_builder, const auto &data) {
      auto save_int = [](auto *int_builder, int value) {
        int_builder->setValue(value);
      };
      utils::SaveUniquePtr<utils::capnp::BoxInt32, int>(data, ptr_builder,
                                                        save_int);
    };
    utils::SaveSharedPtr<utils::capnp::UniquePtr<utils::capnp::BoxInt32>,
                         std::unique_ptr<int>>(data, &builder, save,
                                               &saved_pointers);
  }
  std::shared_ptr<std::unique_ptr<int>> element;
  std::vector<std::pair<uint64_t, std::shared_ptr<std::unique_ptr<int>>>>
      loaded_pointers;
  {
    auto reader = message.getRoot<utils::capnp::SharedPtr<
        utils::capnp::UniquePtr<utils::capnp::BoxInt32>>>();
    auto load = [](const auto &ptr_reader) {
      auto load_int = [](const auto &int_reader) {
        return new int(int_reader.getValue());
      };
      return new std::unique_ptr<int>(
          utils::LoadUniquePtr<utils::capnp::BoxInt32, int>(ptr_reader,
                                                            load_int));
    };
    element =
        utils::LoadSharedPtr<utils::capnp::UniquePtr<utils::capnp::BoxInt32>,
                             std::unique_ptr<int>>(reader, load,
                                                   &loaded_pointers);
  }
  EXPECT_EQ(**element, 5);
}

TEST(Serialization, CapnpVectorPrimitive) {
  std::vector<int> data{1, 2, 3};
  ::capnp::MallocMessageBuilder message;
  {
    auto list_builder = message.initRoot<::capnp::List<int>>(data.size());
    utils::SaveVector<int>(data, &list_builder);
  }
  std::vector<int> elements;
  {
    auto reader = message.getRoot<::capnp::List<int>>();
    utils::LoadVector<int>(&elements, reader);
  }
  EXPECT_EQ(elements.size(), 3);
  EXPECT_EQ(elements[0], 1);
  EXPECT_EQ(elements[1], 2);
  EXPECT_EQ(elements[2], 3);
}

TEST(Serialization, CapnpVector) {
  std::vector<int> data{1, 2, 3};
  ::capnp::MallocMessageBuilder message;
  {
    auto list_builder =
        message.initRoot<::capnp::List<utils::capnp::BoxInt32>>(data.size());
    auto save = [](utils::capnp::BoxInt32::Builder *builder, int value) {
      builder->setValue(value);
    };
    utils::SaveVector<utils::capnp::BoxInt32, int>(data, &list_builder, save);
  }
  std::vector<int> elements;
  {
    auto reader = message.getRoot<::capnp::List<utils::capnp::BoxInt32>>();
    auto load = [](const utils::capnp::BoxInt32::Reader &reader) -> int {
      return reader.getValue();
    };
    utils::LoadVector<utils::capnp::BoxInt32, int>(&elements, reader, load);
  }
  EXPECT_EQ(elements.size(), 3);
  EXPECT_EQ(elements[0], 1);
  EXPECT_EQ(elements[1], 2);
  EXPECT_EQ(elements[2], 3);
}

TEST(Serialization, CapnpVectorNonCopyable) {
  std::vector<std::unique_ptr<int>> data;
  data.emplace_back(std::make_unique<int>(5));
  data.emplace_back(std::make_unique<int>(10));
  ::capnp::MallocMessageBuilder message;
  {
    auto list_builder = message.initRoot<
        ::capnp::List<utils::capnp::UniquePtr<utils::capnp::BoxInt32>>>(
        data.size());
    auto save = [](auto *ptr_builder, const auto &data) {
      auto save_int = [](auto *int_builder, int value) {
        int_builder->setValue(value);
      };
      utils::SaveUniquePtr<utils::capnp::BoxInt32, int>(data, ptr_builder,
                                                        save_int);
    };
    utils::SaveVector<utils::capnp::UniquePtr<utils::capnp::BoxInt32>,
                      std::unique_ptr<int>>(data, &list_builder, save);
  }
  std::vector<std::unique_ptr<int>> elements;
  {
    auto reader = message.getRoot<
        ::capnp::List<utils::capnp::UniquePtr<utils::capnp::BoxInt32>>>();
    auto load = [](const auto &ptr_reader) {
      auto load_int = [](const auto &int_reader) {
        return new int(int_reader.getValue());
      };
      return utils::LoadUniquePtr<utils::capnp::BoxInt32, int>(ptr_reader,
                                                               load_int);
    };
    utils::LoadVector<utils::capnp::UniquePtr<utils::capnp::BoxInt32>,
                      std::unique_ptr<int>>(&elements, reader, load);
  }
  EXPECT_EQ(elements.size(), 2);
  EXPECT_EQ(*elements[0], 5);
  EXPECT_EQ(*elements[1], 10);
}
