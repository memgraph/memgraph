#include "storage/label/labels_writer.hpp"

#include "storage/label/label.hpp"
#include "utils/string_buffer.hpp"

template <typename Buffer>
void LabelsWriter<Buffer>::handle(const Label& label)
{
    buffer_ << ':' << label.str();
}

// NOTE! template instantiations
template class LabelsWriter<utils::StringBuffer>;
