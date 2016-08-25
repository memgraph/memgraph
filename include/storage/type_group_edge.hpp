#pragma once

class Edge;
class EdgeRecord;
class EdgeAccessor;

// Types for Edge side of database. Firstly there exists need for knowing the
// type of object to be able to efficently use it. Dependant classes can
// templetaze over such type, but this is anoying and error prone if there are
// multiple such types over which it is necessary to templetaze. The idea is to
// unify groups of logicaly tyed types into one type. That way depending classes
// can template over that one type.
class TypeGroupEdge
{
public:
    using record_t = Edge;
    using vlist_t = EdgeRecord;
    using accessor_t = EdgeAccessor;
};
