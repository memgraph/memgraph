namespace cpp2 memgraph.storage

typedef i64 VertexId
typedef i64 Gid

struct Label {
    1: binary name;
}

struct EdgeType {
    1: binary name;
}

struct EdgeId {
    1: VertexId src;
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
    2: list<Label> labels;
    3: Properties properties;
}

struct Edge {
    1: VertexId src;
    2: VertexId dst;
    3: EdgeType type;
    4: Properties properties;
}

struct Properties {
    1: map<binary, Value> (cpp.template = "std::unordered_map") values;
}

struct PathPart {
    1: Vertex dst;
    2: Edge edge;
}

struct Path {
    1: Vertex src;
    2: list<PathPart> parts;
}

struct DataSet {
    // If column names are not present, then
    1: optional list<binary> column_names;
    2: list<ReturnedValues> values;
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

struct ScanVerticesRequest {
    1: optional i64 start_id;
    2: optional list<binary> props_to_return;
    3: i64 limit;
    4: optional Filter filter;
}

struct ScanVerticesResponse {
    1: DataSet result;
    2: bool has_next;
    // contains the next start_id if has_next is true
    3: optional i64 next_start_id;
}

union VertexOrEdgeIds {
    1: list<VertexId> vertex_ids;
    2: list<EdgeId> edge_ids;
}

struct GetPropertiesRequest {
    // Only one of them is used, maybe put into an union?
    1: VertexOrEdgeIds vertex_or_edge_ids;
    2: list<binary> property_names;
    3: list<Expression> expressions;
    4: bool only_unique = false;
    5: optional list<OrderBy> order_by;
    6: optional i64 limit;
    7: optional Filter filter;
}

struct MappedValues {
    1: map<i64, Value> (cpp.template = "std::unordered_map") properties;
}

struct ListedValues {
    1: list<Value> properties;
}

union ReturnedValues {
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

struct GetPropertiesResponse {
    1: list<ReturnedValues> properties;
    2: optional map<i64, binary> (cpp.template = "std::unordered_map") property_name_map;
}

enum EdgeDirection {
    OUT = 1;
    IN = 2;
    BOTH = 3;
}

struct ExpandOneRequest {
    1: list<VertexId> src_vertices;
    2: list<EdgeType> edge_types;
    3: EdgeDirection direction;
    4: bool only_unique_neighbor_rows = false;
    // The empty optional means return all of the properties, while an empty
    // list means do not return any properties
    5: optional list<binary> src_vertex_properties;
    6: optional list<binary> edge_properties;
    7: optional list<OrderBy> order_by;
    8: optional i64 limit;
    9: optional Filter filter;
}

struct ExpandedEdgeInfo {
    1: VertexId dst_vertex;
    2: optional ReturnedValues properties;
}

struct ExpandOneResultRow {
    1: VertexId src_vertex;
    2: optional ReturnedValues src_vertex_properties;
    3: optional list<list<ExpandedEdgeInfo>> edge_types;
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

service Storage {
    ScanVerticesResponse scanVertices(1: ScanVerticesRequest req)
    GetPropertiesResponse getProperties(1: GetPropertiesRequest req)
    ExpandOneResponse expandOne(1: ExpandOneRequest req)
}