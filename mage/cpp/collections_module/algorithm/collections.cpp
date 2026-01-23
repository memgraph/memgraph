// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "collections.hpp"
#include <algorithm>
#include <list>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <vector>

void Collections::SetResult(mgp::Result &result, const mgp::Value &value) {
  switch (value.Type()) {
    case mgp::Type::Bool:
      result.SetValue(value.ValueBool());
      break;
    case mgp::Type::Int:
      result.SetValue(value.ValueInt());
      break;
    case mgp::Type::Double:
      result.SetValue(value.ValueDouble());
      break;
    case mgp::Type::String:
      result.SetValue(value.ValueString());
      break;
    case mgp::Type::List:
      result.SetValue(value.ValueList());
      break;
    case mgp::Type::Map:
      result.SetValue(value.ValueMap());
      break;
    case mgp::Type::Node:
      result.SetValue(value.ValueNode());
      break;
    case mgp::Type::Relationship:
      result.SetValue(value.ValueRelationship());
      break;
    case mgp::Type::Path:
      result.SetValue(value.ValuePath());
      break;
    case mgp::Type::Date:
      result.SetValue(value.ValueDate());
      break;
    case mgp::Type::LocalTime:
      result.SetValue(value.ValueLocalTime());
      break;
    case mgp::Type::LocalDateTime:
      result.SetValue(value.ValueLocalDateTime());
      break;
    case mgp::Type::Duration:
      result.SetValue(value.ValueDuration());
      break;

    default:
      std::ostringstream oss;
      oss << value.Type();
      throw mgp::ValueException("No Result.SetValue for: " + oss.str());
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::SumLongs(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    int64_t sum{0};
    const auto list{arguments[0].ValueList()};

    for (const auto list_item : list) {
      if (!list_item.IsNumeric()) {
        std::ostringstream oss;
        oss << list_item.Type();
        throw mgp::ValueException("Unsupported type for this operation, received type: " + oss.str());
      }
      sum += static_cast<int64_t>(list_item.ValueNumeric());
    }
    result.SetValue(sum);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Avg(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    double average{0};
    const auto list{arguments[0].ValueList()};

    for (const auto list_item : list) {
      if (!list_item.IsNumeric()) {
        std::ostringstream oss;
        oss << list_item.Type();
        throw mgp::ValueException("Unsupported type for this operation, received type: " + oss.str());
      }
      average += list_item.ValueNumeric();
    }
    average /= static_cast<double>(list.Size());

    result.SetValue(average);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::ContainsAll(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto list1{arguments[0].ValueList()};
    std::unordered_set<mgp::Value> set(list1.begin(), list1.end());

    const auto list2{arguments[1].ValueList()};

    std::unordered_set<mgp::Value> values(list2.begin(), list2.end());

    result.SetValue(std::ranges::all_of(values, [&set](const auto &x) { return set.contains(x); }));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Intersection(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto list1{arguments[0].ValueList()};
    std::unordered_set<mgp::Value> set1(list1.begin(), list1.end());

    const auto list2{arguments[1].ValueList()};
    std::unordered_set<mgp::Value> set2(list2.begin(), list2.end());

    if (set1.size() > set2.size()) {
      std::swap(set1, set2);
    }

    mgp::List intersection{};
    for (const auto &element : set1) {
      if (set2.contains(element)) {
        intersection.AppendExtend(element);
      }
    }

    result.SetValue(intersection);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::RemoveAll(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    const auto input_list = arguments[0].ValueList();
    const auto to_remove_list = arguments[1].ValueList();

    std::unordered_multiset<mgp::Value> searchable(input_list.begin(), input_list.end());

    for (const auto key : to_remove_list) {
      while (true) {
        auto itr = searchable.find(key);
        if (itr == searchable.end()) {
          break;
        }
        searchable.erase(itr);
      }
    }

    mgp::List final_list = mgp::List();
    for (const auto &element : searchable) {
      final_list.AppendExtend(element);
    }

    result.SetValue(final_list);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Sum(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    double sum{0};
    const auto list = arguments[0].ValueList();

    for (const auto value : list) {
      if (!value.IsNumeric()) {
        throw std::invalid_argument("One of the list elements is not a number.");
      }
      sum += value.ValueNumeric();
    }

    result.SetValue(sum);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Union(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const auto list1 = arguments[0].ValueList();
    const auto list2 = arguments[1].ValueList();

    std::unordered_map<int64_t, std::vector<mgp::Value>> unionMap;

    for (const auto value : list1) {
      if (auto search = unionMap.find(static_cast<int64_t>(std::hash<mgp::Value>{}(value))); search != unionMap.end()) {
        if (std::find(search->second.begin(), search->second.end(), value) != search->second.end()) {
          continue;
        }
        search->second.push_back(value);
      }
      unionMap.insert({std::hash<mgp::Value>{}(value), std::vector<mgp::Value>{value}});
    }
    for (const auto value : list2) {
      if (auto search = unionMap.find(static_cast<int64_t>(std::hash<mgp::Value>{}(value))); search != unionMap.end()) {
        if (std::find(search->second.begin(), search->second.end(), value) != search->second.end()) {
          continue;
        }
        search->second.push_back(value);
      }
      unionMap.insert({std::hash<mgp::Value>{}(value), std::vector<mgp::Value>{value}});
    }

    mgp::List unionList;

    for (const auto &pair : unionMap) {
      for (const auto &value : pair.second) {
        unionList.AppendExtend(value);
      }
    }

    result.SetValue(unionList);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Sort(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const auto list = arguments[0].ValueList();
    std::vector<mgp::Value> sorted;

    for (const auto value : list) {
      sorted.push_back(value);
    }

    std::ranges::sort(sorted, [](const mgp::Value &a, const mgp::Value &b) { return a < b; });

    result.SetValue(mgp::List(std::move(sorted)));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::ContainsSorted(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    bool contains{false};
    const auto list = arguments[0].ValueList();
    const auto element = arguments[1];

    int left{0};
    int right{static_cast<int>(list.Size() - 1)};
    int check = 0;

    while (left <= right) {
      check = (left + right) / 2;
      if (list[check] == element) {
        contains = true;
        break;
      }
      if (element < list[check]) {
        right = check - 1;
      } else {
        left = check + 1;
      }
    }

    result.SetValue(contains);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Max(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const auto list = arguments[0].ValueList();

    if (list.Empty()) {
      throw mgp::ValueException("Empty input list.");
    }

    mgp::Value max = mgp::Value(list[0]);

    for (const auto value : list) {
      if (max < value) {  // this will throw an error in case values can't be compared
        max = value;
      }
    }

    SetResult(result, max);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Split(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};

  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto inputList = arguments[0].ValueList();
    const auto delimiter = arguments[1];

    if (inputList.Empty()) {
      auto record = record_factory.NewRecord();
      record.Insert(std::string(Collections::kResultSplit).c_str(), inputList);
      return;
    }

    mgp::List part = mgp::List();
    for (const auto value : inputList) {
      if (value != delimiter) {
        part.AppendExtend(value);
        continue;
      }
      if (part.Empty()) {
        continue;
      }
      auto record = record_factory.NewRecord();
      record.Insert(std::string(Collections::kResultSplit).c_str(), part);
      part = mgp::List();
    }
    if (part.Empty()) {
      return;
    }
    auto record = record_factory.NewRecord();
    record.Insert(std::string(Collections::kResultSplit).c_str(), part);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Pairs(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    mgp::List pairsList = mgp::List();

    const auto inputList = arguments[0].ValueList();

    if (inputList.Size() == 0) {
      result.SetValue(pairsList);
      return;
    }
    for (size_t i = 0; i < inputList.Size() - 1; i++) {
      mgp::List helper = mgp::List();
      helper.AppendExtend(inputList[i]);
      helper.AppendExtend(inputList[i + 1]);
      pairsList.AppendExtend(mgp::Value(std::move(helper)));
    }
    mgp::List helper = mgp::List();
    helper.AppendExtend(inputList[inputList.Size() - 1]);
    helper.AppendExtend(mgp::Value());
    pairsList.AppendExtend(mgp::Value(std::move(helper)));

    result.SetValue(pairsList);
  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Contains(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const mgp::List &list = arguments[0].ValueList();
    const mgp::Value &value = arguments[1];

    bool contains_value{false};

    if (list.Empty()) {
      result.SetValue(contains_value);
      return;
    }
    for (size_t i = 0; i < list.Size(); i++) {
      if (list[i] == value) {
        contains_value = true;
        break;
      }
    }
    result.SetValue(contains_value);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::UnionAll(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    mgp::List list1 = arguments[0].ValueList();
    mgp::List list2 = arguments[1].ValueList();

    for (size_t i = 0; i < list2.Size(); i++) {
      list1.AppendExtend(list2[i]);
    }
    result.SetValue(list1);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Min(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const mgp::List &list = arguments[0].ValueList();
    if (list.Empty()) {
      throw mgp::ValueException("Empty input list");
    }
    const mgp::Type &type = list[0].Type();

    if (type == mgp::Type::Map || type == mgp::Type::Path || type == mgp::Type::List) {
      std::ostringstream oss;
      oss << type;
      const std::string s = oss.str();
      throw mgp::ValueException("Unsuppported type for this operation, receieved type: " + s);
    }

    const bool isListNumeric = list[0].IsNumeric();
    mgp::Value min{list[0]};
    for (size_t i = 0; i < list.Size(); i++) {
      if (list[i].Type() != type && !(isListNumeric && list[i].IsNumeric())) {
        throw mgp::ValueException("All elements must be of the same type!");
      }

      if (list[i] < min) {
        min = list[i];
      }
    }

    SetResult(result, min);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::ToSet(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  auto result = mgp::Result(res);
  try {
    const mgp::List list = arguments[0].ValueList();
    const std::unordered_set<mgp::Value> set(list.begin(), list.end());

    mgp::List return_list;
    for (const auto &elem : set) {
      return_list.AppendExtend(elem);
    }
    result.SetValue(std::move(return_list));

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Partition(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::List input_list = arguments[0].ValueList();
    const int64_t partition_size = arguments[1].ValueInt();

    int64_t current_size = 0;
    mgp::List temp;
    const mgp::List result;
    for (const mgp::Value list_value : input_list) {
      if (current_size == 0) {
        temp = mgp::List();
      }
      temp.AppendExtend(list_value);
      current_size++;

      if (current_size == partition_size) {
        auto record = record_factory.NewRecord();
        record.Insert(std::string(kReturnValuePartition).c_str(), temp);
        current_size = 0;
      }
    }

    if (current_size != partition_size && current_size != 0) {
      auto record = record_factory.NewRecord();
      record.Insert(std::string(kReturnValuePartition).c_str(), temp);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

namespace {

// Helper function to recursively flatten a list
void FlattenHelper(const mgp::Value &value, mgp::List &result) {
  if (value.IsNull()) {
    // Skip null values
    return;
  }

  if (value.IsList()) {
    auto list = value.ValueList();
    for (const auto &item : list) {
      FlattenHelper(item, result);
    }
  } else {
    result.AppendExtend(value);
  }
}

}  // namespace

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::Flatten(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    if (arguments.Size() != 1) {
      throw mgp::ValueException("The procedure expects 1 argument, got " + std::to_string(arguments.Size()));
    }

    const auto &input = arguments[0];
    if (!input.IsList()) {
      throw mgp::ValueException("The argument must be a list");
    }

    // Create result list
    mgp::List flattened;

    // Directly flatten the input (handles null and lists recursively)
    FlattenHelper(input, flattened);

    result.SetValue(flattened);

  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Collections::FrequenciesAsMap(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  auto const arguments = mgp::List(args);
  auto result = mgp::Result(res);

  try {
    auto const input_list = arguments[0].ValueList();
    std::unordered_map<mgp::Value, int64_t> frequency_map;

    for (auto &&element : input_list) {
      frequency_map[element]++;
    }

    mgp::Map result_map;
    for (auto &&[element, count] : frequency_map) {
      auto const key = element.ToString();
      result_map.Insert(key, mgp::Value(count));
    }

    result.SetValue(std::move(result_map));

  } catch (std::exception const &e) {
    result.SetErrorMessage(e.what());
  }
}
