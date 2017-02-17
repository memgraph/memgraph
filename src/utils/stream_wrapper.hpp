#pragma once

// Wraps stream with convinient methods which need only one method:
// write (const char* s, n);
template <class STREAM>
class StreamWrapper
{
public:
    StreamWrapper() = delete;
    StreamWrapper(STREAM &s) : stream(s) {}

    void write(const unsigned char value)
    {
        stream.write(reinterpret_cast<const char *>(&value), 1);
    }

    void write(const unsigned char *value, size_t n)
    {
        stream.write(reinterpret_cast<const char *>(value), n);
    }

private:
    STREAM &stream;
};
