#include <iostream>
#include <algorithm>
#include <cstdio>
#include <vector>
#include <string>
#include <cstdlib>
#include "bitflags.h"
#include <atomic>

using namespace std;

template <typename T>
struct node_t {
  //atomic<T> data;
  T data;
  atomic<node_t<T>* > next;
  atomic<int> ref_count;
  long long timestamp;
  node_t<T> (const T& data_, node_t<T>* next_) {
    //data.store(data_);
    timestamp = -1;
    data = data_;
    next.store(next_);
    ref_count.store(1);
  }
};
