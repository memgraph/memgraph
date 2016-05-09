#pragma once

#include <string>

struct Trace
{
    static std::string text;
    static constexpr unsigned level = 0;
};

struct Debug
{
    static std::string text;
    static constexpr unsigned level = 10;
};

struct Info
{
    static std::string text;
    static constexpr unsigned level = 20;
};

struct Warn
{
    static std::string text;
    static constexpr unsigned level = 30;
};

struct Error
{
    static std::string text;
    static constexpr unsigned level = 40;
};
