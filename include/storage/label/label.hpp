#pragma once

#include <ostream>
#include <stdint.h>

#include "storage/indexes/impl/nonunique_unordered_index.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/reference_wrapper.hpp"
#include "utils/total_ordering.hpp"
#include "utils/void.hpp"

using LabelIndexRecord = VertexIndexRecord<std::nullptr_t>;

class Label : public TotalOrdering<Label>
{
public:
    using label_index_t = NonUniqueUnorderedIndex<Vertex, std::nullptr_t>;

    Label() = delete;

    Label(const std::string &name);
    Label(std::string &&name);

    Label(const Label &) = delete;
    Label(Label &&other) = default;

    friend bool operator<(const Label &lhs, const Label &rhs);

    friend bool operator==(const Label &lhs, const Label &rhs);

    friend std::ostream &operator<<(std::ostream &stream, const Label &label);

    operator const std::string &() const;

    std::unique_ptr<label_index_t> index;

private:
    std::string name;
};

using label_ref_t = ReferenceWrapper<const Label>;
