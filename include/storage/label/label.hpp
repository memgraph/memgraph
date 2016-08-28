#pragma once

#include <ostream>
#include <stdint.h>

#include "storage/indexes/impl/nonunique_unordered_index.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/char_str.hpp"
#include "utils/reference_wrapper.hpp"
#include "utils/total_ordering.hpp"
// #include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

using LabelIndexRecord = IndexRecord<TypeGroupVertex, std::nullptr_t>;

class Label : public TotalOrdering<Label>, TotalOrdering<CharStr, Label>
{
public:
    using label_index_t =
        NonUniqueUnorderedIndex<TypeGroupVertex, std::nullptr_t>;

    Label() = delete;

    Label(const char *name);

    Label(const Label &) = delete;
    Label(Label &&other) = default;

    friend bool operator<(const Label &lhs, const Label &rhs);

    friend bool operator==(const Label &lhs, const Label &rhs);

    friend bool operator<(const CharStr &lhs, const Label &rhs);

    friend bool operator==(const CharStr &lhs, const Label &rhs);

    friend std::ostream &operator<<(std::ostream &stream, const Label &label);

    operator const std::string &() const;

    CharStr char_str() const { return CharStr(name.c_str()); }

    label_index_t &index() const;

private:
    std::unique_ptr<label_index_t> index_v;
    std::string name;
};

using label_ref_t = ReferenceWrapper<const Label>;
