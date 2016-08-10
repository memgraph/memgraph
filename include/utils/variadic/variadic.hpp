#pragma once

#include <iostream>

// variadic argument printer

template<class Head>
void _print_vargs(std::ostream& s, Head&& head)
{
    s << std::forward<Head>(head);
}
template<class Head, class ...Tail>
void _print_vargs(std::ostream& s, Head&& head, Tail&& ...tail)
{
    s << std::forward<Head>(head);
    _print_vargs(s, std::forward<Tail>(tail)...);
}
template<class ...Args>
void print_vargs(Args&&... args)
{
    _print_vargs(std::cout, std::forward<Args>(args)...);
}

// value equality with any of variadic argument
// example: value == varg[0] OR value == varg[1] OR ...

template<class Value, class Head>
bool _or_vargs(Value&& value, Head&& head)
{
    return value == head;
}
template<class Value, class Head, class ...Tail>
bool _or_vargs(Value&& value, Head&& head, Tail&& ...tail)
{
    return value == head || _or_vargs(std::forward<Value>(value), tail...);
}
template<class Value, class ...Array>
bool or_vargs(Value&& value, Array&&... array)
{
   return _or_vargs(std::forward<Value>(value), std::forward<Array>(array)...);
}
