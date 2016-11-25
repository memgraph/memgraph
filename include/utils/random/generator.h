#include <random>
#include <vector>

// namespace ::utils
namespace utils {

// namespace utils::random
namespace random {

template <class Distribution, class Generator>
class RandomGenerator {
 private:
  std::random_device device_;

 protected:
  Generator gen_;
  Distribution dist_;

 public:
  RandomGenerator(Distribution dist) : gen_(device_()), dist_(dist) {}
};

class StringGenerator
    : public RandomGenerator<std::uniform_int_distribution<int>,
                             std::default_random_engine> {
 private:
  int size_;

 public:
  StringGenerator(int size)
      : RandomGenerator(std::uniform_int_distribution<int>(32, 126)),
        size_(size) {}

  std::string next(int size) {
    std::string random_string;
    random_string.reserve(size);

    for (int i = 0; i < size; i++) random_string += (dist_(gen_) + '\0');

    return random_string;
  }

  std::string next() { return next(size_); }
};

template <class Distribution, class Generator, class DistributionRangeType>
class NumberGenerator : public RandomGenerator<Distribution, Generator> {
 public:
  NumberGenerator(DistributionRangeType start, DistributionRangeType end)
      : RandomGenerator<Distribution, Generator>(Distribution(start, end)) {}

  auto next() { return this->dist_(this->gen_); }
};

template <class FirstGenerator, class SecondGenerator>
class PairGenerator {
 private:
  FirstGenerator *first_;
  SecondGenerator *second_;

 public:
  PairGenerator(FirstGenerator *first, SecondGenerator *second)
      : first_(first), second_(second) {}
  auto next() { return std::make_pair(first_->next(), second_->next()); }
};

template <class RandomGenerator>
auto generate_vector(RandomGenerator &gen, int size) {
  std::vector<decltype(gen.next())> elements(size);
  for (int i = 0; i < size; i++) elements[i] = gen.next();
  return elements;
}

};  // namespace utils::random
};  // namespace ::utils
