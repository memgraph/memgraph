#pragma once

#include <cmath>
#include <limits>

#include "point.hpp"

namespace kd {
namespace math {

using byte = unsigned char;

// returns the squared distance between two points
template <class T>
T distance_sq(const Point<T>& a, const Point<T>& b) {
  auto dx = a.longitude - b.longitude;
  auto dy = a.latitude - b.latitude;
  return dx * dx + dy * dy;
}

// returns the distance between two points
template <class T>
T distance(const Point<T>& a, const Point<T>& b) {
  return std::sqrt(distance_sq(a, b));
}

// returns the distance between two points looking at a specific axis
// \param axis 0 if abscissa else 1 if ordinate
template <class T>
T axial_distance(const Point<T>& a, const Point<T>& b, byte axis) {
  return axis == 0 ? a.longitude - b.longitude : a.latitude - b.latitude;
}
}
}
