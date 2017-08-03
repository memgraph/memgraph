#include <fstream>
#include <streambuf>

#include "cereal/archives/binary.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"  // utility has to be included because of std::pair
#include "cereal/types/vector.hpp"

struct BasicSerializable {
  int64_t x_;
  std::string y_;

  BasicSerializable() = default;
  BasicSerializable(int64_t x, std::string y) : x_(x), y_(y) {}

  template <class Archive>
  void serialize(Archive &ar) {
    ar(x_, y_);
  }

  template <typename Archive>
  static void load_and_construct(
      Archive &ar, cereal::construct<BasicSerializable> &construct) {
    int64_t x;
    std::string y;
    ar(x, y);
    construct(x, y);
  }
};

struct ComplexSerializable {
  using VectorT = std::vector<float>;
  using VectorPairT = std::vector<std::pair<std::string, BasicSerializable>>;

  BasicSerializable x_;
  VectorT y_;
  VectorPairT z_;

  ComplexSerializable(const BasicSerializable &x, const VectorT &y,
                      const VectorPairT &z)
      : x_(x), y_(y), z_(z) {}

  template <typename Archive>
  void serialize(Archive &ar) {
    ar(x_, y_, z_);
  }

  template <typename Archive>
  static void load_and_construct(
      Archive &ar, cereal::construct<ComplexSerializable> &construct) {
    BasicSerializable x;
    VectorT y;
    VectorPairT z;
    ar(x, y, z);
    construct(x, y, z);
  }
};

class DummyStreamBuf : public std::basic_streambuf<char> {
 protected:
  std::streamsize xsputn(const char *data, std::streamsize count) override {
    for (std::streamsize i = 0; i < count; ++i) {
      data_.push_back(data[i]);
    }
    return count;
  }
  std::streamsize xsgetn(char *data, std::streamsize count) override {
    if (count < 0) return 0;
    if (static_cast<size_t>(position_ + count) > data_.size()) {
      count = data_.size() - position_;
      position_ = data_.size();
    }
    memcpy(data, data_.data() + position_, count);
    position_ += count;
    return count;
  }

 private:
  std::vector<char> data_;
  std::streamsize position_{0};
};

int main() {
  DummyStreamBuf sb;
  std::iostream iostream(&sb);

  // serialization
  cereal::BinaryOutputArchive oarchive{iostream};
  std::unique_ptr<BasicSerializable const> const basic_serializable_object{
      new BasicSerializable{100, "Test"}};
  std::unique_ptr<ComplexSerializable const> const complex_serializable_object{
      new ComplexSerializable{
          {100, "test"},
          {3.4, 3.4},
          {{"first", {10, "Basic1"}}, {"second", {20, "Basic2"}}}}};
  oarchive(basic_serializable_object);
  oarchive(complex_serializable_object);

  // deserialization
  cereal::BinaryInputArchive iarchive{iostream};
  std::unique_ptr<BasicSerializable> basic_deserialized_object{nullptr};
  std::unique_ptr<ComplexSerializable> complex_deserialized_object{nullptr};
  iarchive(basic_deserialized_object);
  iarchive(complex_deserialized_object);

  // output
  std::cout << "Basic Deserialized: " << basic_deserialized_object->x_ << "; "
            << basic_deserialized_object->y_ << std::endl;
  auto x = complex_deserialized_object->x_;
  auto y = complex_deserialized_object->y_;
  auto z = complex_deserialized_object->z_;
  std::cout << "Complex Deserialized" << std::endl;
  std::cout << "    x_ -> " << x.x_ << "; " << x.y_ << std::endl;
  std::cout << "    y_ -> ";
  for (const auto v_item : y) std::cout << v_item << " ";
  std::cout << std::endl;
  std::cout << "    z_ -> ";
  for (const auto v_item : z)
    std::cout << v_item.first << " | Pair: (" << v_item.second.x_ << ", "
              << v_item.second.y_ << ")"
              << "::";
  std::cout << std::endl;

  return 0;
}
