#include <stdlib.h>
#include <memory>
#include <map>
#include <iostream>

size_t ALLOCATED = 0;

std::map<void*, size_t> TRACKER;

void* operator new(size_t size, const char* filename, int line_number) {
  std::cout << filename << ":" << line_number << " Allocating" << size
            << "bytes." << std::endl;

  void* block = malloc(size);
  TRACKER[block] = size; 
  ALLOCATED += size;
  return block;
}

void operator delete(void* block) {
  TRACKER[block] = 0;
  free(block);
}

void *operator new[](size_t size, const char* filename, int line_number) {
  std::cout << filename << ":" << line_number << " Allocating" << size
            << "bytes." << std::endl;

  void* block = malloc(size);
  TRACKER[block] = size;
  ALLOCATED += size;
  return block;
}

void operator delete[] (void* block) {
  TRACKER[block] = 0;
  free(block);
}

void print_memory() {
  std::cout << "Total bytes allocated: " << ALLOCATED << std::endl;

  for (const auto& el : TRACKER) {
    std::cout << el.first << " " << el.second << std::endl;
  }

}
#define new new (__FILE__, __LINE__)
