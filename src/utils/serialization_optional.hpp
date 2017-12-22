#include <experimental/optional>

#include "boost/serialization/split_free.hpp"

namespace boost::serialization {

template <class TArchive, class T>
inline void serialize(TArchive &ar, std::experimental::optional<T> &opt,
                      unsigned int version) {
  split_free(ar, opt, version);
}

template <class TArchive, class T>
void save(TArchive &ar, const std::experimental::optional<T> &opt,
          unsigned int) {
  ar << static_cast<bool>(opt);
  if (opt) {
    ar << *opt;
  }
}

template <class TArchive, class T>
void load(TArchive &ar, std::experimental::optional<T> &opt, unsigned int) {
  bool has_value;
  ar >> has_value;
  if (has_value) {
    T tmp;
    ar >> tmp;
    opt = std::move(tmp);
  } else {
    opt = std::experimental::nullopt;
  }
}

}  // boost::serialization
