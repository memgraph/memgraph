#pragma once

#include <vector>
#include <string>

#include "utils/datetime/timestamp.hpp"

class Logger;

class Log
{
public:
    using uptr = std::unique_ptr<Log>;

    class Record
    {
    public:
        using uptr = std::unique_ptr<Record>;

        Record() = default;
        virtual ~Record() = default;

        virtual const Timestamp& when() const = 0;
        virtual const std::string& where() const = 0;

        virtual unsigned level() const = 0;
        virtual const std::string& level_str() const = 0;

        virtual const std::string& text() const = 0;
    };

    class Stream
    {
    public:
        using uptr = std::unique_ptr<Stream>;

        Stream() = default;
        virtual ~Stream() = default;

        virtual void emit(const Record&) = 0;
    };

    virtual ~Log() = default;

    Logger logger(const std::string& name);

    void pipe(Stream::uptr&& stream)
    {
        streams.emplace_back(std::forward<Stream::uptr>(stream));
    }

    virtual std::string type() = 0;

protected:
    friend class Logger;

    virtual void emit(Record::uptr record) = 0;

    void dispatch(const Record& record)
    {
        for(auto& stream : streams)
            stream->emit(record);
    }

    std::vector<Stream::uptr> streams;
};
