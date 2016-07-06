#pragma once

#include <stdint.h>
#include <ostream>

#include "utils/total_ordering.hpp"
#include "utils/reference_wrapper.hpp"

class EdgeType : public TotalOrdering<EdgeType>
{
public:
    EdgeType();
    EdgeType(const std::string& id);
    EdgeType(std::string&& id);

    friend bool operator<(const EdgeType& lhs, const EdgeType& rhs);

    friend bool operator==(const EdgeType& lhs, const EdgeType& rhs);

    friend std::ostream& operator<<(std::ostream& stream, const EdgeType& type);

    operator const std::string&() const;

private:
    std::string id;
};

using edge_type_ref_t = ReferenceWrapper<const EdgeType>;
