#pragma once

#include "storage/indexes/index_record.hpp"
#include "storage/label/label.hpp"
#include "transactions/transaction.hpp"

class Db;
class DbAccessor;

// Inner structures local to transaction can hold ref to this structure and use
// its methods.
class DbTransaction
{
    friend DbAccessor;

public:
    DbTransaction(Db &db, tx::Transaction &trans) : db(db), trans(trans) {}

    void update_label_index(const Label &label,
                            VertexIndexRecord &&index_record);
    // protected:
    // TRANSACTION METHODS
    tx::Transaction &trans;

    Db &db;
};
