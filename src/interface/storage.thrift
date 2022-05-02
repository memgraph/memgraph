namespace cpp2 interface.storage
// https://stackoverflow.com/a/34234874/6639989

cpp_include "storage/v2/view.hpp"

typedef i64 VertexId
typedef i64 Gid

// TODO(antaljanosbenjamin): Use this after introducing 128 bit vertex ids
// struct VertexId {
//     1: i64 upper_half;
//     2: i64 lower_half;
// }

struct Label {
    1: i64 id;
}

struct EdgeType {
    1: binary name;
}

struct EdgeId {
    1: VertexId src;
    // QUESTION(antaljanosbenjamin): is it okay to have vertex based (edge id = vertex id + edge id inside vertex)?
    2: Gid gid;
}

struct Date {
    1: i16 year;
    2: byte month;
    3: byte day;
}

struct LocalTime {
    1: byte hour;
    2: byte minute;
    3: byte second;
    4: i16 millisecond;
    5: i16 microsecond;
}

struct LocalDateTime {
    1: Date date;
    2: LocalTime local_time;
}

struct Duration {
    1: i64 milliseconds;
}

union Value {
    1: Null null_v;
    2: bool bool_v;
    3: i64 int_v;
    4: double double_v;
    5: binary string_v;
    6: list<Value> list_v;
    7: map<binary, Value> (cpp.template = "std::unordered_map") map_v (cpp2.ref_type = "unique");
    8: Vertex vertex_v (cpp2.ref_type = "unique");
    9: Edge edge_v (cpp2.ref_type = "unique");
    10: Path path_v (cpp2.ref_type = "unique");
    11: Date date_v;
    12: LocalTime local_time_v;
    13: LocalDateTime local_date_time_v;
    14: Duration duration_v;
}

struct Null {
}

struct Vertex {
    1: VertexId id;
    // TODO(antaljanosbenjamin): Change to sperate primary and secondary labels when schema is implemented
    2: list<Label> labels;
}

struct Edge {
    1: VertexId src;
    2: VertexId dst;
    3: EdgeType type;
}

struct PathPart {
    1: Vertex dst;
    2: Edge edge;
}

struct Path {
    1: Vertex src;
    2: list<PathPart> parts;
}

struct ValuesMap {
    1: map<i64, Value> (cpp.template = "std::unordered_map") values_map;
}

struct MappedValues {
    1: list<ValuesMap> properties;
}

struct ListedValues {
    1: list<list<Value>> properties;
}

union Values {
    // This struct is necessary because depending on the request the response
    // has two different formats:
    // 1. When the request specifies the returned properties, then they are
    //    returned in that order, therefore no extra mapping is necessary.
    // 2. When the request doesn't specify the returned properties, then all
    //    of the properties are returned. In this case the `mapped` field is
    //    used. To extract the <key,value> pairs from this struct the
    //    mapping of i64 -> property name has to be used.
    1: ListedValues listed;
    2: MappedValues mapped;
}

struct Expression {
    1: binary alias;
    2: binary expression;
}

struct Filter {
    1: binary filter_expression;
}

enum OrderingDirection {
    ASCENDING = 1;
    DESCENDING = 2;
}

struct OrderBy {
    1: Expression expression;
    2: OrderingDirection direction;
}

struct Result {
    // Just placeholder data for now
    1: bool success;
}

enum View {
    OLD = 0,
    NEW = 1
}  (cpp.enum_strict, cpp.type = "memgraph::storage::View")

struct ScanVerticesRequest {
    1: i64 transaction_id;
    2: optional i64 start_id;
    // Special values are accepted:
    // * __mg__id (Vertex, but without labels)
    // * __mg__labels (Vertex, but without the id)
    // If both of them is specified, then it will result in a single, fully populated vertex
    // QUESTION(antaljanosbenjamin): Does the `__mg__labels` is necessary? What about passing the `labels` function
    //                               as an expression? Maybe it is an optimization. For communicating the vertex id
    //                               the Vertex struct is really handy.
    3: optional list<binary> props_to_return;
    4: list<Expression> expressions;
    5: optional i64 limit;
    6: View view;
    7: optional Filter filter;
}

struct ScanVerticesResponse {
    1: Result result;
    2: Values values;
    3: optional map<i64, binary> (cpp.template = "std::unordered_map") property_name_map;
    // contains the next start_id if there is any
    4: optional VertexId next_start_id;
}

union VertexOrEdgeIds {
    1: list<VertexId> vertex_ids;
    2: list<EdgeId> edge_ids;
}

struct GetPropertiesRequest {
    1:  i64 transaction_id;
    2:  VertexOrEdgeIds vertex_or_edge_ids;
    3:  list<binary> property_names;
    4:  list<Expression> expressions;
    5:  bool only_unique = false;
    6:  optional list<OrderBy> order_by;
    7:  optional i64 limit;
    8:  optional Filter filter;
}

struct GetPropertiesResponse {
    1: Values values;
    2: optional map<i64, binary> (cpp.template = "std::unordered_map") property_name_map;
}

enum EdgeDirection {
    OUT = 1;
    IN = 2;
    BOTH = 3;
}

struct ExpandOneRequest {
    1:  i64 transaction_id;
    2:  list<VertexId> src_vertices;
    3:  list<EdgeType> edge_types;
    4:  EdgeDirection direction;
    5:  bool only_unique_neighbor_rows = false;
    //  The empty optional means return all of the properties, while an empty
    //  list means do not return any properties
    //  TODO(antaljanosbenjamin): All of the special values should be communicated through a single vertex object
    //                            after schema is implemented
    //  Special values are accepted:
    //  * __mg__labels
    6:  optional list<binary> src_vertex_properties;
    //  TODO(antaljanosbenjamin): All of the special values should be communicated through a single vertex object
    //                            after schema is implemented
    //  Special values are accepted:
    //  * __mg__dst_id (Vertex, but without labels)
    //  * __mg__type (binary)
    7:  optional list<binary> edge_properties;
    //  QUESTION(antaljanosbenjamin): Maybe also add possibility to expressions evaluated on the source vertex?
    //  List of expressions evaluated on edges
    8:  list<Expression> expressions;
    9:  optional list<OrderBy> order_by;
    10:  optional i64 limit;
    11: optional Filter filter;
}

struct ExpandOneResultRow {
    // NOTE: This struct could be a single Values with columns something like this:
    // src_vertex(Vertex), vertex_prop1(Value), vertex_prop2(Value), edges(list<Value>)
    // where edges might be a list of:
    // 1. list<Value> if only a defined list of edge properties are returned
    // 2. map<binary, Value> if all of the edge properties are returned
    // The drawback of this is currently the key of the map is always interpreted as a string in Value, not as an
    // integer, which should be in case of mapped properties.
    1: Vertex src_vertex;
    2: optional Values src_vertex_properties;
    3: Values edges;
}

struct ExpandOneResponse {
    // This approach might not suit the expand with per shard parrallelization,
    // because the property_name_map has to be accessed from multiple threads
    // in order to avoid duplicated keys (two threads might map the same
    // property with different numbers) and multiple passes (to unify the
    // mapping amond result returned from different shards).
    1: list<ExpandOneResultRow> result;
    2: optional map<i64, binary>  (cpp.template = "std::unordered_map") property_name_map;
}

struct NewVertex {
    1: list<i64> label_ids;
    2: map<i64, Value> properties;
}

struct CreateVerticesRequest {
    1: required i64 transaction_id;
    2: map<i64, binary> (cpp.template = "std::unordered_map") labels_name_map;
    3: map<i64, binary> (cpp.template = "std::unordered_map") property_name_map;
    4: list<NewVertex> new_vertices;
}


service Storage {
    i64 startTransaction()
    Result commitTransaction(1: i64 transaction_id)
    void abortTransaction(1: i64 transaction_id)

    Result createVertices(1: CreateVerticesRequest req)
    ScanVerticesResponse scanVertices(1: ScanVerticesRequest req)
    GetPropertiesResponse getProperties(1: GetPropertiesRequest req)
    ExpandOneResponse expandOne(1: ExpandOneRequest req)

}