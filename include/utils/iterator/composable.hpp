#pragma once

#include "utils/crtp.hpp"
#include "utils/iterator/count.hpp"
#include "utils/option.hpp"

namespace iter
{

template <class I, class OP>
auto make_map(I &&iter, OP &&op);

template <class I, class OP>
auto make_filter(I &&iter, OP &&op);

template <class I, class C>
void for_all(I &&iter, C &&consumer);

template <class I, class OP>
auto make_flat_map(I &&iter, OP &&op);

template <class I, class OP>
auto make_inspect(I &&iter, OP &&op);

template <class I, class OP>
auto make_limited_map(I &&iter, OP &&op);

template <class I, class OP>
auto make_virtual(I &&iter);

template <class IT1, class IT2>
auto make_combined(IT1 &&iter1, IT2 &&iter2);

// Class for creating easy composable iterators fo querying.
//
// Derived - type of derived class
// T - return type
template <class T, class Derived>
class Composable : public Crtp<Derived>
{
    // Moves self
    Derived &&move() { return std::move(this->derived()); }

public:
    auto virtualize() { return iter::make_virtual(move()); }

    template <class IT>
    auto combine(IT &&it)
    {
        return iter::make_combined<Derived, IT>(move(), std::move(it));
    }

    template <class OP>
    auto map(OP &&op)
    {
        return iter::make_map<Derived, OP>(move(), std::move(op));
    }

    template <class OP>
    auto filter(OP &&op)
    {
        return iter::make_filter<Derived, OP>(move(), std::move(op));
    }

    // Replaces every item with item taken from n if it exists.
    template <class R>
    auto replace(Option<R> &n)
    {
        return iter::make_limited_map<Derived>(
            move(), [&](auto v) mutable { return std::move(n); });
    }

    // For all items calls OP.
    template <class OP>
    void for_all(OP &&op)
    {
        iter::for_all(move(), std::move(op));
    }

    // All items must satisfy given predicate for this function to return true.
    // Otherwise stops calling predicate on firts false and returns fasle.
    template <class OP>
    bool all(OP &&op)
    {
        auto iter = move();
        auto e = iter.next();
        while (e.is_present()) {
            if (!op(e.take())) {
                return false;
            }
            e = iter.next();
        }
        return true;
    }

    // !! MEMGRAPH specific composable filters
    // TODO: isolate, but it is not trivial because this class
    // is a base for other generic classes

    // Maps with call to method to() and filters with call to fill.
    auto to()
    {
        return map([](auto er) { return er.to(); }).fill();
    }

    // Maps with call to method from() and filters with call to fill.
    auto from()
    {
        return map([](auto er) { return er.from(); }).fill();
    }

    // Combines out iterators into one iterator.
    auto out()
    {
        return iter::make_flat_map<Derived>(
            move(), [](auto vr) { return vr.out().fill(); });
    }

    auto in()
    {
        return iter::make_flat_map<Derived>(
            move(), [](auto vr) { return vr.in().fill(); });
    }

    // Filters with label on from vertex.
    template <class LABEL>
    auto from_label(LABEL const &label)
    {
        return filter([&](auto &ra) {
            auto va = ra.from();
            return va.fill() && va.has_label(label);
        });
    }

    // Calls update on values and returns result.
    auto update()
    {
        return map([](auto ar) { return ar.update(); });
    }

    // Filters with property under given key
    template <class KEY>
    auto has_property(KEY &key)
    {
        return filter([&](auto &va) { return !va.at(key).is_empty(); });
    }

    // Filters with property under given key
    template <class KEY, class PROP>
    auto has_property(KEY &key, PROP const &prop)
    {
        return filter([&](auto &va) { return va.at(key) == prop; });
    }

    // Copy-s pasing value to t before they are returned.
    auto clone_to(Option<const T> &t)
    {
        return iter::make_inspect<Derived>(
            move(), [&](auto &v) mutable { t = Option<const T>(v); });
    }

    // auto clone_to(Option<T> &t)
    // {
    //     return iter::make_inspect<Derived>(
    //         move(), [&](auto &e) mutable { t = Option<T>(e); });
    // }

    // Filters with call to method fill()
    auto fill()
    {
        return filter([](auto &ra) { return ra.fill(); });
    }

    // Filters with type
    template <class TYPE>
    auto type(TYPE const &type)
    {
        return filter([&](auto &ra) { return ra.edge_type() == type; });
    }

    // Filters with label.
    template <class LABEL>
    auto label(LABEL const &label)
    {
        return filter([&](auto &va) { return va.has_label(label); });
    }

    // Filters out vertices which are connected.
    auto isolated()
    {
        return filter([&](auto &element) { return element.isolated(); });
    }

    // filter elements based on properties (all properties have to match)
    // TRANSACTION -> transaction
    // PROPERTIES  -> [(name, property)]
    template <class TRANSACTION, class PROPERTIES>
    auto properties_filter(TRANSACTION &t, PROPERTIES& properties)
    {
        return filter([&](auto &element) {
            for (auto &name_value : properties) {
                auto property_key = t.vertex_property_key(
                    name_value.first,
                    name_value.second.key.flags());
                if (!element.properties().contains(property_key))
                    return false;
                auto vertex_property_value = element.at(property_key);
                if (!(vertex_property_value == name_value.second))
                    return false;
            }
            return true;
        });
    }
};

}
