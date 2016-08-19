#pragma once

#include "transactions/transaction.hpp"

class Db;
class DbAccessor;

// Inner structures local to transaction can hold ref to this structure and use
// its methods.
// Also serves as a barrier for calling methods defined public but meant for
// internal use. That kind of method should request DbTransaction&.
class DbTransaction
{
    friend DbAccessor;

public:
    DbTransaction(Db &db, tx::Transaction &trans) : db(db), trans(trans) {}

    // Global transactional algorithms,operations and general methods meant for
    // internal use should be here or should be routed through this object.
    // This should provide cleaner hierarchy of operations on database.
    // For example cleaner.

    tx::Transaction &trans;

    Db &db;
};
