#pragma once

#include <memory>

#include "point.hpp"

namespace kd {

template <class T, class U>
class KdNode
{
public:
    KdNode(const U& data)
        : axis(0), coord(Point<T>(0, 0)), left(nullptr), right(nullptr), data(data) { }

    KdNode(const Point<T>& coord, const U& data)
        : axis(0), coord(coord), left(nullptr), right(nullptr), data(data) { }

    KdNode(unsigned char axis, const Point<T>& coord, const U& data)
        : axis(axis), coord(coord), left(nullptr), right(nullptr), data(data) { }

    KdNode(unsigned char axis, const Point<T>& coord, KdNode<T, U>* left, KdNode<T, U>* right, const U& data)
        : axis(axis), coord(coord), left(left), right(right), data(data) { }

    ~KdNode();

    unsigned char axis;

    Point<T> coord;

    KdNode<T, U>* left;
    KdNode<T, U>* right;

    U data;
};

template <class T, class U>
KdNode<T, U>::~KdNode()
{
    delete left;
    delete right;
}

}
