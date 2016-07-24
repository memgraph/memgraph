#pragma once

#include <algorithm>
#include <limits>
#include <map>

// TODO: remove
#include "utils/underlying_cast.hpp"
#include <iostream>

// entities are nodes or relationship

namespace entity_search
{
// returns maximum value for given template argument (for give type)
template <typename T>
constexpr T max()
{
    return std::numeric_limits<uint64_t>::max();
}

using cost_t = uint64_t;

// TODO: rething
// at least load hard coded values from somewhere
constexpr cost_t internal_id_cost = 10;
constexpr cost_t property_cost = 100;
constexpr cost_t label_cost = 1000;
constexpr cost_t max_cost = max<cost_t>();

template <typename T>
class SearchCost
{
public:
    enum class SearchPlace : int
    {
        internal_id,
        label_index,
        property_index,
        main_storage
    };

    using costs_t = std::map<SearchPlace, T>;
    using cost_pair_t = std::pair<SearchPlace, T>;

    SearchCost()
    {
        costs[SearchPlace::internal_id] = max<T>();
        costs[SearchPlace::label_index] = max<T>();
        costs[SearchPlace::property_index] = max<T>();
        costs[SearchPlace::main_storage] = max<T>();
    }

    SearchCost(const SearchCost &other) = default;

    SearchCost(SearchCost &&other) : costs(std::move(other.costs)) {}

    void set(SearchPlace place, T cost) { costs[place] = cost; }

    T get(SearchPlace place) const { return costs.at(place); }

    SearchPlace min() const
    {
        auto min_pair = std::min_element(
            costs.begin(), costs.end(),
            [](const cost_pair_t &l, const cost_pair_t &r) -> bool {
                return l.second < r.second;
            });

        if (min_pair->second == max_cost) return SearchPlace::main_storage;

        return min_pair->first;
    }

private:
    costs_t costs;
};

using search_cost_t = SearchCost<cost_t>;

constexpr auto search_internal_id = search_cost_t::SearchPlace::internal_id;
constexpr auto search_label_index = search_cost_t::SearchPlace::label_index;
constexpr auto search_property_index =
    search_cost_t::SearchPlace::property_index;
constexpr auto search_main_storage = search_cost_t::SearchPlace::main_storage;
}

class CypherStateMachine
{
public:
    void init_cost(const std::string &entity)
    {
        entity_search::search_cost_t search_cost;
        _search_costs.emplace(entity, search_cost);
    }

    void search_cost(const std::string &entity,
                     entity_search::search_cost_t::SearchPlace search_place,
                     entity_search::cost_t cost)
    {
        if (_search_costs.find(entity) != _search_costs.end()) {
            entity_search::search_cost_t search_cost;
            _search_costs.emplace(entity, std::move(search_cost));
        }

        _search_costs[entity].set(search_place, cost);
    }

    entity_search::cost_t
    search_cost(const std::string &entity,
                entity_search::search_cost_t::SearchPlace search_place) const
    {
        return _search_costs.at(entity).get(search_place);
    }

    entity_search::search_cost_t::SearchPlace
    min(const std::string &entity) const
    {
        if (_search_costs.find(entity) == _search_costs.end())
            return entity_search::search_cost_t::SearchPlace::main_storage;

        return _search_costs.at(entity).min();
    }

private:
    std::map<std::string, entity_search::search_cost_t> _search_costs;
};
