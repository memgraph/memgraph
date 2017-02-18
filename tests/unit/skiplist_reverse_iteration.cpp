/**
  @date: 2017-01-2017
  @authors: Sandi Fatic

  @brief
  These tests are used to test the functionality of the reverse() function in
  a single threaded scenario. For more concurrent tests look in the concurrent
  testing folder.

  @todo
  Concurrent tests are missing for now.
*/

#include <algorithm>

#include "gtest/gtest.h"

#include "data_structures/concurrent/skiplist.hpp"
#include "logging/default.cpp"
#include "utils/random/random_generator.hpp"

using utils::random::NumberGenerator;
using IntegerGenerator = NumberGenerator<std::uniform_int_distribution<int>,
                                         std::default_random_engine, int>;

/*
  Tests Skiplist rbegin() and rend() iterators on a sequential dataset.
*/
TEST(SkipListReverseIteratorTest, SequentialIteratorsBeginToEnd) {
  SkipList<int> skiplist;

  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  for (int i = 0; i < number_of_elements; i++) accessor.insert(std::move(i));

  auto rbegin = accessor.rbegin();
  auto rend = accessor.rend();

  while (rbegin != rend) {
    ASSERT_EQ(number_of_elements - 1, *rbegin);
    rbegin++;
    number_of_elements--;
  }
}

/*
  Tests Skiplist rbegin() and rend() iterators on a random dataset.
*/
TEST(SkipListReverseIteratorTest, RandomIteratorsBeginToEnd) {
  SkipList<int> skiplist;

  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  IntegerGenerator generator(0, 1000000000);
  std::set<int> elems_set =
      utils::random::generate_set(generator, number_of_elements);

  int end = elems_set.size();
  std::vector<int> elems_descending(end);

  for (auto el : elems_set) {
    end--;
    accessor.insert(std::move(el));
    elems_descending[end] = el;
  }

  auto rbegin = accessor.rbegin();
  auto rend = accessor.rend();

  while (rbegin != rend) {
    ASSERT_EQ(elems_descending[end], *rbegin);
    end++;
    rbegin++;
  }
}

/*
  Tests Skiplist reverse() when element exists. The skiplist uses a sequential
  dataset and the element provided exists is in range of the dataset. The
  reverse function should return an std::pair<iterator_to_element_before, true>.
*/
TEST(SkipListReverseIteratorTest, SequentialIteratorsElementExists) {
  SkipList<int> skiplist;
  auto accessor = skiplist.access();

  int number_of_elements = 1024 * 16;

  for (int i = 0; i < number_of_elements; i++) accessor.insert(std::move(i));

  int element = 1024 * 8;
  auto reverse_pair = accessor.reverse(element);

  ASSERT_EQ(reverse_pair.second, true);

  auto rbegin = reverse_pair.first;
  auto rend = accessor.rend();

  while (rbegin != rend) {
    ASSERT_EQ(element - 1, *rbegin);
    rbegin++;
    element--;
  }
}

/*
  Tests Skiplist reverse() when element exists. The skiplist uses a random
  dataset and the element provide exists in the random dataset. The reverse
  function should return an  std::pair<iterator_to_element_before, true>.
*/
TEST(SkipListReverseIteratorTest, RandomIteratorsElementExists) {
  SkipList<int> skiplist;

  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  IntegerGenerator generator(0, 1000000000);
  std::set<int> elems_set =
      utils::random::generate_set(generator, number_of_elements);

  int end = elems_set.size();
  std::vector<int> elems_descending(end);

  for (auto el : elems_set) {
    end--;
    accessor.insert(std::move(el));
    elems_descending[end] = el;
  }

  int middle = end / 2;
  auto reverse_pair = accessor.reverse(elems_descending[middle]);

  ASSERT_EQ(reverse_pair.second, true);

  auto rbegin = reverse_pair.first;
  auto rend = accessor.rend();

  while (rbegin != rend) {
    ASSERT_EQ(elems_descending[middle + 1], *rbegin);
    middle++;
    rbegin++;
  }
}

/*
  Tests Skiplist reverse() when element exists and the element is the first one
  in the skiplist. The skiplist uses a sequential dataset. The reverse function
  should return an std::pair<rend, true>.
*/
TEST(SkipListReverseIteratorTest, SequentialIteratorsMinimumElement) {
  SkipList<int> skiplist;

  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  for (int i = 0; i < number_of_elements; i++) accessor.insert(std::move(i));

  auto reverse_pair = accessor.reverse(0);

  ASSERT_EQ(reverse_pair.second, true);

  auto rbegin = reverse_pair.first;
  auto rend = accessor.rend();

  ASSERT_EQ(rbegin, rend);
}

/*
  Tests Skiplist reverse() when element exists and the element is the first one
  in the skiplist. The skiplist uses a random dataset. The reverse function
  should return an std::pair<rend, true>.
*/
TEST(SkipListReverseIteratorTest, RandomIteratorsMinimumElement) {
  SkipList<int> skiplist;

  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  IntegerGenerator generator(0, 1000000000);
  std::set<int> elems_set =
      utils::random::generate_set(generator, number_of_elements);

  auto min_el = std::min_element(elems_set.begin(), elems_set.end());
  for (auto el : elems_set) accessor.insert(std::move(el));

  auto reverse_pair = accessor.reverse(*min_el);

  ASSERT_EQ(reverse_pair.second, true);

  auto rbegin = reverse_pair.first;
  auto rend = accessor.rend();

  ASSERT_EQ(rbegin, rend);
}

/*
  Tests Skiplist reverse() when element exists and the element is the last one
  in the skiplist. The skiplist uses a sequential dataset. The reverse function
  should return an std::pair<iterator_to_smaller_element, true>.
*/
TEST(SkipListReverseIteratorTest, SequentialIteratorsMaximumElement) {
  SkipList<int> skiplist;

  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  for (int i = 0; i < number_of_elements; i++) accessor.insert(std::move(i));

  auto reverse_pair = accessor.reverse(number_of_elements - 1);

  ASSERT_EQ(reverse_pair.second, true);

  auto rbegin = reverse_pair.first;
  auto rbeing_real = accessor.rbegin();
  rbeing_real++;

  ASSERT_EQ(rbegin, rbeing_real);
}

/*
  Tests Skiplist reverse() when element exists and the element is the last one
  in the skiplist. the skiplist uses a random dataset. The reverse function
  should return and std::pair,iterator_to_smaller_element, true>.
*/
TEST(SkipListReverseIteratorTest, RandomIteratorsMaximumElement) {
  SkipList<int> skiplist;

  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  IntegerGenerator generator(0, 1000000000);
  std::set<int> elems_set =
      utils::random::generate_set(generator, number_of_elements);

  auto max_el = std::max_element(elems_set.begin(), elems_set.end());
  for (auto el : elems_set) accessor.insert(std::move(el));

  auto reverse_pair = accessor.reverse(*max_el);

  ASSERT_EQ(reverse_pair.second, true);

  auto rbegin = reverse_pair.first;
  auto rbeing_real = accessor.rbegin();
  rbeing_real++;

  ASSERT_EQ(rbegin, rbeing_real);
}

/*
  Tests Skipslist reverse() when element out of bounds. The skiplist uses a
  sequential dataset and the element provided is bigger then the last element
  in skiplist. Reverse function should return an std::pair<rend, false>.
*/
TEST(SkipListReverseIteratorTest, SequentialIteratorsElementBiggerThanLast) {
  SkipList<int> skiplist;
  auto accessor = skiplist.access();

  int number_of_elements = 1024 * 16;

  for (int i = 0; i < number_of_elements; i++) accessor.insert(std::move(i));

  auto reverse_pair = accessor.reverse(number_of_elements + 1);

  ASSERT_EQ(reverse_pair.second, false);

  auto rbegin = reverse_pair.first;
  auto rend = accessor.rend();

  ASSERT_EQ(rend, rbegin);
}

/*
  Tests Skipslist reverse() when element out of bounds. The skiplist uses a
  random dataset and the element provide is bigger then the last element in the
  skiplist. Reverse function should return an std::pair<rend, false>.
*/
TEST(SkipListReverseIteratorTest, RandomIteratorsElementBiggerThanLast) {
  SkipList<int> skiplist;
  auto accessor = skiplist.access();

  int number_of_elements = 1024 * 16;

  IntegerGenerator generator(0, 1000000000);
  std::set<int> elems_set =
      utils::random::generate_set(generator, number_of_elements);

  auto max_el = std::max_element(elems_set.begin(), elems_set.end());
  for (auto el : elems_set) accessor.insert(std::move(el));

  auto reverse_pair = accessor.reverse(*max_el + 1);

  ASSERT_EQ(reverse_pair.second, false);

  auto rbegin = reverse_pair.first;
  auto rend = accessor.rend();

  ASSERT_EQ(rend, rbegin);
}

/*
  Tests Skipslist reverse() when element out of bounds.
  The skiplist uses a sequential dataset and the element provided is lower
  then the first element in skiplist. Reverse function should return an
  std::pair<rend, false>.
*/
TEST(SkipListReverseIteratorTest, SequentialIteratorsElementLowerThanFirst) {
  SkipList<int> skiplist;
  auto accessor = skiplist.access();

  int number_of_elements = 1024 * 16;

  for (int i = 0; i < number_of_elements; i++) accessor.insert(std::move(i));

  auto reverse_pair = accessor.reverse(-1);

  ASSERT_EQ(reverse_pair.second, false);

  auto rbegin = reverse_pair.first;
  auto rend = accessor.rend();

  ASSERT_EQ(rend, rbegin);
}

/*
  Tests Skiplist reverse() when element out of bounds. The skiplist uses a
  random dataset and the element provided is lower then the first element in
  skiplist. Reverse function should return an std::pair<rend, false>.
*/
TEST(SkipListReverseIteratorTest, RandomIteratorsElementLowerThanFirst) {
  SkipList<int> skiplist;
  auto accessor = skiplist.access();

  int number_of_elements = 1024 * 16;

  IntegerGenerator generator(0, 1000000000);
  std::set<int> elems_set =
      utils::random::generate_set(generator, number_of_elements);

  auto min_el = std::min_element(elems_set.begin(), elems_set.end());
  for (auto el : elems_set) accessor.insert(std::move(el));

  auto reverse_pair = accessor.reverse(*min_el - 1);

  ASSERT_EQ(reverse_pair.second, false);

  auto rbegin = reverse_pair.first;
  auto rend = accessor.rend();

  ASSERT_EQ(rend, rbegin);
}

/*
  Tests Skiplist ReverseIterator when concurrently inserting an element while
  iterating. The inserted element should also be traversed.
*/
TEST(SkipListReverseIteratorTest, InsertWhileIteratingTest) {
  SkipList<int> skiplist;
  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  for (int i = 1; i < number_of_elements; i += 2) accessor.insert(std::move(i));

  auto rbegin = accessor.rbegin();
  auto rend = accessor.rend();

  for (int i = 0; i < number_of_elements; i += 2) accessor.insert(std::move(i));

  int element = number_of_elements - 1;
  while (rbegin != rend) {
    ASSERT_EQ(element, *rbegin);
    rbegin++;
    element--;
  }
}

/*
  Tests Skiplist ReverseIterator when concurrently deleting an element while
  iterating. The deleted element shouldn't be traversed except if the element
  is deleted while pointing to the element.
*/
TEST(SkipListReverseIteratorTest, DeleteWhileIteratingTest) {
  SkipList<int> skiplist;
  auto accessor = skiplist.access();
  int number_of_elements = 1024 * 16;

  for (int i = 0; i < number_of_elements; i++) accessor.insert(std::move(i));

  auto rbegin = accessor.rbegin();
  auto rend = accessor.rend();

  int element = number_of_elements - 2;
  // check element which will be deleted
  rbegin++;
  ASSERT_EQ(element, *rbegin);

  // delete elements
  for (int i = 0; i < number_of_elements; i += 2) accessor.remove(i);

  // check if still points to the same after delete
  ASSERT_EQ(element, *rbegin);
  rbegin++;

  // check all deleted elements after
  while (rbegin != rend && element > 0) {
    ASSERT_EQ(element - 1, *rbegin);
    rbegin++;
    element -= 2;
  }
}

int main(int argc, char** argv) {
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
