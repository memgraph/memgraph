#pragma once

class Label;

template <class Buffer>
class LabelsWriter
{
public:
    LabelsWriter(Buffer &buffer) : buffer_(buffer) {}

    ~LabelsWriter() = default;

    LabelsWriter(const LabelsWriter &) = delete;
    LabelsWriter(LabelsWriter &&)      = delete;

    LabelsWriter &operator=(const LabelsWriter &) = delete;
    LabelsWriter &operator=(LabelsWriter &&) = delete;

    void handle(const Label& label);
    
private:
    Buffer& buffer_; 
};
