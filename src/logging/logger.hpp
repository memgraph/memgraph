#pragma once

#include "log.hpp"
#include "levels.hpp"

class Logger
{
    template <class Level>
    class Message : public Log::Record
    {
    public:
        Message(Timestamp timestamp, std::string location, std::string message)
            : timestamp(timestamp), location(location), message(message) {}

        const Timestamp& when() const override
        {
            return timestamp;
        }

        const std::string& where() const override
        {
            return location;
        }

        unsigned level() const override
        {
            return Level::level;
        }

        const std::string& level_str() const override
        {
            return Level::text;
        }

        const std::string& text() const override
        {
            return message;
        }

    private:
        Timestamp timestamp;
        std::string location;
        std::string message;
    };

public:
    Logger(Log& log, const std::string& name) : log(log), name(name) {}

    template <class Level, class... Args>
    void emit(Args&&... args)
    {
        auto message = std::make_unique<Message<Level>>(
            Timestamp::now(), name, fmt::format(std::forward<Args>(args)...)
        );

        log.get().emit(std::move(message));
    }

    template <class... Args>
    void trace(Args&&... args)
    {
        emit<Trace>(std::forward<Args>(args)...);
    }

    template <class... Args>
    void debug(Args&&... args)
    {
        emit<Debug>(std::forward<Args>(args)...);
    }

    template <class... Args>
    void info(Args&&... args)
    {
        emit<Info>(std::forward<Args>(args)...);
    }

    template <class... Args>
    void warn(Args&&... args)
    {
        emit<Warn>(std::forward<Args>(args)...);
    }

    template <class... Args>
    void error(Args&&... args)
    {
        emit<Error>(std::forward<Args>(args)...);
    }

private:
    std::reference_wrapper<Log> log;
    std::string name;
};

