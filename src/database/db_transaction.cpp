#include "database/db.hpp"
#include "database/db_transaction.hpp"

void DbTransaction::update_label_index(const Label &label,
                                       VertexIndexRecord &&index_record)
{
    db.graph.vertices.update_label_index(label, std::move(index_record));
}
