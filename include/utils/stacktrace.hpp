#pragma once

#include <cxxabi.h>
#include <stdexcept>
#include <execinfo.h>

#include <fmt/format.h>
#include "utils/auto_scope.hpp"

class Stacktrace
{
public:
    class Line
    {
    public:
        Line(const std::string& original) : original(original) {}

        Line(const std::string& original, const std::string& function,
             const std::string& location)
            : original(original), function(function), location(location) {}

        std::string original, function, location;
    };

    static constexpr size_t stacktrace_depth = 128;

    Stacktrace()
    {
        void* addresses[stacktrace_depth];
        auto depth = backtrace(addresses, stacktrace_depth);

        // will this leak if backtrace_symbols throws?
        char** symbols = nullptr;
        Auto(free(symbols));

        symbols = backtrace_symbols(addresses, depth);

        // skip the first one since it will be Stacktrace::Stacktrace()
        for(int i = 1; i < depth; ++i)
            lines.emplace_back(format(symbols[i]));
    }

    auto begin() { return lines.begin(); }
    auto begin() const { return lines.begin(); }
    auto cbegin() const { return lines.cbegin(); }

    auto end() { return lines.end(); }
    auto end() const { return lines.end(); }
    auto cend() const { return lines.cend(); }

    const Line& operator[](size_t idx) const
    {
        return lines[idx];
    }

    size_t size() const
    {
        return lines.size();
    }

    void dump(std::ostream& stream) {
      std::string message;
      dump(message);
      stream << message;
    }
    
    void dump(std::string& message) {
      for (int i = 0; i < size(); i++) {
        message.append(fmt::format("at {} ({}) \n", lines[i].function, 
          lines[i].location));    
      }
    }

private:
    std::vector<Line> lines;

    Line format(const std::string& original)
    {
        using namespace abi;
        auto line = original;

        auto begin = line.find('(');
        auto end = line.find('+');

        if(begin == std::string::npos || end == std::string::npos)
            return {original};

        line[end] = '\0';

        int s;
        auto demangled = __cxa_demangle(line.data() + begin + 1, nullptr,
                                        nullptr, &s);

        auto location = line.substr(0, begin);

        auto function = demangled ? std::string(demangled)
            : fmt::format("{}()", original.substr(begin + 1, end - begin - 1));

        return {original, function, location};
    }
};
