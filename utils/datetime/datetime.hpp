#pragma once


#include "utils/exceptions/basic_exception.hpp"

class Datetime
{
public:
    Datetime()
    {

    }

    Datetime(std::time_t time_point)
    {
        auto result = gmtime_r(&time_point, &time);

        if(result == nullptr)
            throw DatetimeError("Unable to construct from {}", time_point);
    }

    Datetime(const Datetime&) = default;
    Datetime(Datetime&&) = default;

    static Datetime now()
    {
        timespec

        return Datetime();
    }


private:
    std::tm time;
};
