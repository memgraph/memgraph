#include <cassert>
#include <iostream>
#include <thread>

#include "data_structures/linked_list.hpp"

using std::cout;
using std::endl;

template <typename list_type>
void test_concurrent_list_access(list_type &list, std::size_t size)
{
  // test concurrent access
  for (int i = 0; i < 1000000; ++i) {

    std::thread t1([&list] {
      list.push_front(1);
      list.pop_front();
    });

    std::thread t2([&list] {
      list.push_front(2);
      list.pop_front();
    });

    t1.join();
    t2.join();

    assert(list.size() == size);
  }
}

int main()
{
  LinkedList<int> list;

  // push & pop operations
  list.push_front(10);
  list.push_front(20);
  auto a = list.front();
  assert(a == 20);
  list.pop_front();
  a = list.front();
  assert(a == 10);
  list.pop_front();
  assert(list.size() == 0);

  // concurrent test
  LinkedList<int> concurrent_list;
  concurrent_list.push_front(1);
  concurrent_list.push_front(1);
  std::list<int> no_concurrent_list;
  no_concurrent_list.push_front(1);
  no_concurrent_list.push_front(1);

  test_concurrent_list_access(concurrent_list, 2);
  // test_concurrent_list_access(no_concurrent_list, 2);

  return 0;
}
