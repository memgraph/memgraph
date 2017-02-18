#include <cstdint>
#include <iostream>

#include <byteswap.h>

char b[8] = {1, 2, 3, 4, 0, 0, 0, 1};

int64_t safe_int64(const char* b) {
  return int64_t(b[0]) << 56 | int64_t(b[1]) << 48 | int64_t(b[2]) << 40 |
         int64_t(b[3]) << 32 | int64_t(b[4]) << 24 | int64_t(b[5]) << 16 |
         int64_t(b[6]) << 8 | int64_t(b[7]);
}

int64_t unsafe_int64(const char* b) {
  auto i = reinterpret_cast<const int64_t*>(b);
  return __bswap_64(*i);
}

int32_t safe_int32(const char* b) {
  return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
}

int32_t unsafe_int32(const char* b) {
  auto i = reinterpret_cast<const int32_t*>(b);
  return __bswap_32(*i);
}

[[clang::optnone]] void test(uint64_t n) {
  for (uint64_t i = 0; i < n; ++i) unsafe_int64(b);
}

uint8_t f[8] = {0x3F, 0xF1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A};

double ff = 1.1;

double get_double(const uint8_t* b) {
  auto v = __bswap_64(*reinterpret_cast<const uint64_t*>(b));
  return *reinterpret_cast<const double*>(&v);
}

void print_hex(const char* buf, size_t n) {
  for (size_t i = 0; i < n; ++i) printf("%02X ", (unsigned char)buf[i]);
}

void print_hex(const uint8_t* buf, size_t n) { print_hex((const char*)buf, n); }

int main(void) {
  auto dd = get_double(f);

  print_hex(f, 8);

  std::cout << std::endl;
  print_hex((const uint8_t*)(&ff), 8);

  std::cout << std::endl;
  print_hex((const uint8_t*)(&dd), 8);

  std::cout << "dd: " << dd << std::endl;
  std::cout << "Safe: " << safe_int64(b) << std::endl;
  std::cout << unsafe_int64(b) << std::endl;

  test(1000000ull);

  return 0;
}
