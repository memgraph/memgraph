#pragma once

#include <stdint.h>
#include <ostream>

#include "utils/total_ordering.hpp"
#include "utils/reference_wrapper.hpp"

class Label : public TotalOrdering<Label>
{
public:
    Label(const std::string& name);
    Label(std::string&& name);

    Label(const Label&) = default;
    Label(Label&&) = default;

    friend bool operator<(const Label& lhs, const Label& rhs);

    friend bool operator==(const Label& lhs, const Label& rhs);

    friend std::ostream& operator<<(std::ostream& stream, const Label& label);

    operator const std::string&() const;

private:
    std::string name;
};

using label_ref_t = ReferenceWrapper<const Label>;
