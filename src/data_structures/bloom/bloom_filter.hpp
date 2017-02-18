#pragma once

#include <bitset>
#include <iostream>
#include <vector>

/**
 * Implementation of a generic Bloom Filter.
 *     Read more about bloom filters here:
 *         http://en.wikipedia.org/wiki/Bloom_filter
 *         http://www.jasondavies.com/bloomfilter/
 *
 * Type specifies the type of data stored
 */
template <class Type, int BucketSize = 8>
class BloomFilter {
 private:
  using HashFunction = std::function<uint64_t(const Type &)>;
  using CompresionFunction = std::function<int(uint64_t)>;

  std::bitset<BucketSize> filter_;
  std::vector<HashFunction> hashes_;
  CompresionFunction compression_;
  std::vector<int> buckets;

  int default_compression(uint64_t hash) { return hash % BucketSize; }

  void get_buckets(const Type &data) {
    for (int i = 0; i < hashes_.size(); i++)
      buckets[i] = compression_(hashes_[i](data));
  }

  void print_buckets(std::vector<uint64_t> &buckets) {
    for (int i = 0; i < buckets.size(); i++) {
      std::cout << buckets[i] << " ";
    }
    std::cout << std::endl;
  }

 public:
  BloomFilter(std::vector<HashFunction> funcs,
              CompresionFunction compression = {})
      : hashes_(funcs) {
    if (!compression)
      compression_ = std::bind(&BloomFilter::default_compression, this,
                               std::placeholders::_1);
    else
      compression_ = compression;

    buckets.resize(hashes_.size());
  }

  bool contains(const Type &data) {
    get_buckets(data);
    bool contains_element = true;

    for (int i = 0; i < buckets.size(); i++)
      contains_element &= filter_[buckets[i]];

    return contains_element;
  }

  void insert(const Type &data) {
    get_buckets(data);

    for (int i = 0; i < buckets.size(); i++) filter_[buckets[i]] = true;
  }
};
