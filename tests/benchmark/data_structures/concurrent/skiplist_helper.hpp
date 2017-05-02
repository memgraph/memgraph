#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "data_structures/concurrent/skiplist.hpp"

/**
 * Helper functions for skiplist. This functions are used to insert
 * concurrently into skiplist.
 */
class SkipListHelper {
 public:
  /**
   * Inserts into a skiplist concurrently. Tries to synchronize all threads to
   * start and end in the same time. This function should only be used to
   * benchmark skiplist in a way that doesn't give thread a chance to consume
   * more than the (end - start) / num_of_threads elements in the allocated
   * time, since that would make the measurement inaccurate. Also shuffles the
   * data to avoid consecutive value inserts.
   *
   * @param skiplist - skiplist instance
   * @param start - value_range start
   * @param end - value_range end (exclusive)
   * @param num_of_threads - number of threads to insert with
   * @param duration - duration of thread time in microseconds
   * @return number of inserted elements
   */
  static int InsertConcurrentSkiplistTimed(
      SkipList<int> *skiplist, const int start, const int end,
      const int num_of_threads, const std::chrono::microseconds &duration) {
    std::vector<int> V(end - start);
    for (int i = start; i < end; ++i) V[i] = i;
    std::random_shuffle(V.begin(), V.end());
    std::vector<std::thread> threads;

    std::atomic<bool> stopped{1};
    std::atomic<int> count{0};
    for (int i = 0; i < num_of_threads; ++i) {
      const int part = (end - start) / num_of_threads;
      threads.emplace_back(std::thread(
          [&V, &stopped, &count](SkipList<int> *skiplist, int start, int end) {
            while (stopped)
              ;
            auto accessor = skiplist->access();
            for (int i = start; i < end && !stopped; ++i) {
              while (accessor.insert(V[i]).second == false)
                ;
              ++count;
            }
          },
          skiplist, start + i * part, start + (i + 1) * part));
    }
    stopped = false;
    std::this_thread::sleep_for(duration);
    stopped = true;
    for (auto &x : threads) x.join();
    return count;
  }

  /**
   * Insert into skiplist concurrently. With the hardware maximal number of
   * threads an instance will allow.
   *
   * @param skiplist - skiplist instance
   * @param start - starting value to insert
   * @param end - ending value to insert
   */
  static void InsertConcurrentSkiplist(SkipList<int> *skiplist, int start,
                                       int end) {
    int number_of_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;

    for (int i = 0; i < number_of_threads; i++) {
      const int part = (end - start) / number_of_threads;
      threads.emplace_back(std::thread(
          [](SkipList<int> *skiplist, int start, int end) {
            auto accessor = skiplist->access();

            for (; start < end; start++) {
              while (!accessor.insert(std::move(start)).second)
                ;
            }
          },
          skiplist, start + i * part, start + (i + 1) * part));
    }

    for (auto &thread : threads) thread.join();
  }

 private:
};
