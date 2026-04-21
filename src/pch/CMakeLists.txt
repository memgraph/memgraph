# Precompiled header donor targets.
#
# Two donors for different consumer groups:
#   - mg-pch-base: the 17 stdlib/fmt/spdlog headers universally hot across
#     memgraph libraries (>=70% TU coverage in every consumer).
#   - mg-pch-heavy: base + absl/flat_hash_map.h + boost/container_hash/hash.hpp,
#     which are 69-88% hot in mg-query/mg-storage-v2/mg-dbms but rare elsewhere.
#
# Consumers attach via `target_precompile_headers(<target> REUSE_FROM mg-pch-*)`,
# sharing the compiled .pch binary instead of each building their own.

add_library(mg-pch-base STATIC pch_dummy.cpp)
target_precompile_headers(mg-pch-base PRIVATE
    <chrono>
    <filesystem>
    <format>
    <istream>
    <memory>
    <optional>
    <ostream>
    <ranges>
    <sstream>
    <string>
    <unordered_map>
    <vector>
    <fmt/chrono.h>
    <fmt/format.h>
    <fmt/ostream.h>
    <spdlog/common.h>
    <spdlog/spdlog.h>
)

add_library(mg-pch-heavy STATIC pch_dummy.cpp)
target_precompile_headers(mg-pch-heavy PRIVATE
    <chrono>
    <filesystem>
    <format>
    <istream>
    <memory>
    <optional>
    <ostream>
    <ranges>
    <sstream>
    <string>
    <unordered_map>
    <vector>
    <fmt/chrono.h>
    <fmt/format.h>
    <fmt/ostream.h>
    <spdlog/common.h>
    <spdlog/spdlog.h>
    <absl/container/flat_hash_map.h>
    <boost/container_hash/hash.hpp>
)

# PCH donors need to see the same include paths as consumers for fmt/spdlog/absl/boost.
find_package(fmt REQUIRED)
find_package(spdlog REQUIRED)
find_package(absl REQUIRED)
find_package(Boost REQUIRED)
target_link_libraries(mg-pch-base PRIVATE fmt::fmt spdlog::spdlog)
target_link_libraries(mg-pch-heavy PRIVATE fmt::fmt spdlog::spdlog abseil::abseil Boost::headers)
