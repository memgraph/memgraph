#pragma once

class Label;

template <class Buffer>
class LabelsWriter
{
public:
    LabelsWriter(Buffer &buffer) : buffer_(buffer) {}

    void handle(const Label& label);
    
private:
    Buffer& buffer_; 
};
