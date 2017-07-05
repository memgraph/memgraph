#include <ctime>
#include <iostream>
#include <limits>
#include <vector>

#include <fmt/format.h>

#include "data_structures/concurrent/skiplist.hpp"

/** Calculates the mean of a given vector of numbers */
template <typename TNumber>
auto mean(const std::vector<TNumber> &values) {
  TNumber r_val = 0;
  for (const auto &value : values) r_val += value;
  return r_val / values.size();
}

/** Logging helper function */
template <typename... TArgs>
void log(const std::string &format, TArgs &&... args) {
  std::cout << fmt::format(format, std::forward<TArgs>(args)...) << std::endl;
}

/** Creates a skiplist containing all ints in range [0, size) */
std::unique_ptr<SkipList<int>> make_sl(int size) {
  auto sl = std::make_unique<SkipList<int>>();
  auto access = sl->access();
  for (int i = 0; i < size; i++) access.insert(i);
  return sl;
}

/**
 * Performs testing of the position_and_count function
 * of a skiplist. Looks for three positions in the skiplist,
 * those at 1/4, 1/2 and 3/4 values. Prints out results
 * to stdout, does not do any automated checks if the
 * results are valid.
 *
 * @param size - size of the skiplist to test with
 * @param iterations - number of iterations of each test.
 * @param granulation - How many sequential ints should be
 *  considered equal in testing by the custom `less`
 *  function.
 */
void test(int size, int iterations = 20, int granulation = 1) {
  auto less = [granulation](const int &a, const int &b) {
    return a / granulation < b / granulation;
  };
  log("\nTesting skiplist size {} with granulation {}", size, granulation);

  // test at 1/4, 1/2 and 3/4 points
  std::vector<int> test_positions({size / 4, size / 2, size * 3 / 4});

  std::vector<std::vector<int>> position(3);
  std::vector<std::vector<int>> count(3);
  std::vector<std::vector<double>> time(3);
  for (int iteration = 0; iteration < iterations; iteration++) {
    auto sl = make_sl(size);

    for (auto pos : {0, 1, 2}) {
      clock_t start_time = clock();
      auto pos_and_count =
          sl->access().position_and_count(test_positions[pos], less);
      auto t = double(clock() - start_time) / CLOCKS_PER_SEC;

      position[pos].push_back(pos_and_count.first);
      count[pos].push_back(pos_and_count.second);
      time[pos].push_back(t);
    }
  }

  // convert values to errors
  for (auto pos_index : {0, 1, 2}) {
    auto test_position = test_positions[pos_index];
    log("\tPosition {}", test_position);
    for (auto &position_elem : position[pos_index])
      position_elem = std::abs(position_elem - test_position);
    log("\t\tMean position error: {}", mean(position[pos_index]));
    for (auto &count_elem : count[pos_index])
      count_elem = std::abs(count_elem - granulation);
    log("\t\tMean count error: {}", mean(count[pos_index]));
    log("\t\tMean time (ms): {}", mean(time[pos_index]) * 1000);
  }
}

int main(int argc, char *argv[]) {
  log("Skiplist position and count testing");

  int size = 1000;
  int iterations = 10;
  if (argc > 1) size = (int)std::stoi(argv[1]);
  if (argc > 2) iterations = (int)std::stoi(argv[2]);

  std::vector<int> granulations;
  for (int i = 1; i < size; i *= 100) granulations.push_back(i);
  for (auto granulation : granulations) test(size, iterations, granulation);

  return 0;
}
