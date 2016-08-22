#pragma once

#include <ostream>
#include <stdint.h>

#include "utils/char_str.hpp"
#include "utils/reference_wrapper.hpp"
#include "utils/total_ordering.hpp"

class EdgeType : public TotalOrdering<EdgeType>
{
public:
    EdgeType();
    EdgeType(const std::string &id);
    EdgeType(const char *id);
    EdgeType(std::string &&id);

    friend bool operator<(const EdgeType &lhs, const EdgeType &rhs);

    friend bool operator==(const EdgeType &lhs, const EdgeType &rhs);

    friend std::ostream &operator<<(std::ostream &stream, const EdgeType &type);

    operator const std::string &() const;

    CharStr char_str() { return CharStr(&id[0]); }

private:
    std::string id;
};

using edge_type_ref_t = ReferenceWrapper<const EdgeType>;
