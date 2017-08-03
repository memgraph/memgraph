#include <iostream>

int main() {
  int* array = new int[16];
  for (int i = 0; i < 16; i++)
    *(array+i) = i;
  std::cout << *(array+5);
  return 0;
}
