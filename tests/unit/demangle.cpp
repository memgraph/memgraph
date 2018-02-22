#include "glog/logging.h"
#include "gtest/gtest.h"

#include "utils/demangle.hpp"

using utils::Demangle;

struct DummyStruct {};

template <typename T>
class DummyClass {};

TEST(Demangle, Demangle) {
  int x;
  char *s;
  DummyStruct t;
  DummyClass<int> c;

  EXPECT_EQ(*Demangle(typeid(x).name()), "int");
  EXPECT_EQ(*Demangle(typeid(s).name()), "char*");
  EXPECT_EQ(*Demangle(typeid(t).name()), "DummyStruct");
  EXPECT_EQ(*Demangle(typeid(c).name()), "DummyClass<int>");
}
