#ifndef GEOAPI_KDTREE_POINT_HPP
#define GEOAPI_KDTREE_POINT_HPP

#include <ostream>

namespace kd {

template <class T>
class Point
{
public:
    Point(T latitude, T longitude)
        : latitude(latitude), longitude(longitude) {}

    // latitude
    //    y
    //    ^
    //    |
    //    0---> x longitude

    T latitude;
    T longitude;

    /// nice stream formatting with the standard << operator
    friend std::ostream& operator<< (std::ostream& stream, const Point& p) {
        return stream << "(lat: " << p.latitude
                      << ", lng: " << p.longitude << ')';
    }
};

}

#endif
