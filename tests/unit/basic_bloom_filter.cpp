#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "utils/command_line/arguments.hpp"
#include "utils/hashing/fnv64.hpp"

#include "data_structures/bloom/basic_bloom_filter.hpp"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wwritable-strings"

using StringHashFunction = std::function<uint64_t(const std::string&)>;
 
TEST_CASE("BasicBloomFilter Test") {
  StringHashFunction hash1 = fnv64<std::string>;
  StringHashFunction hash2 = fnv1a64<std::string>;

  auto c = [](auto x) -> int {
    return x % 4;
  } ;
  std::vector<StringHashFunction> funcs = {
    hash1, hash2
  };

  BasicBloomFilter<std::string, 64> bloom(funcs);

  std::string test = "test";
  std::string kifla = "pizda";

  std::cout << hash1(test) << std::endl;
  std::cout << hash2(test) << std::endl;
  
  std::cout << hash1(kifla) << std::endl;
  std::cout << hash2(kifla) << std::endl;

  std::cout << bloom.contains(test) << std::endl;
  bloom.insert(test);
  std::cout << bloom.contains(test) << std::endl;

  std::cout << bloom.contains(kifla) << std::endl;
  bloom.insert(kifla);
  std::cout << bloom.contains(kifla) << std::endl;
}

#pragma clang diagnostic pop
