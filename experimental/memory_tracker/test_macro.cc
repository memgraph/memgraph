#include "macro_override.h"

int main () {
  int* num = new int;
  delete num;
  print_memory();

  ALLOCATED = 0;

  int* nums = new int[16];
  delete nums;
  print_memory();


  return 0;
}
