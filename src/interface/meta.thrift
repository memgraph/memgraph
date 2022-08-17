namespace cpp2 interface.meta

typedef i64 LabelId
typedef i64 IndexId

struct Result {
    1: bool success;
}

struct CreatePrimaryLabelRequest {
    1: binary name;
    2: list<binary> primary_keys;
}

struct CreateLabelResponse {
    1: Result result;
    2: LabelId label_id;
}

struct GetLabelInfosRequest {
    //  The empty list means return all of the label infos
    1: list<binary> label_names;
}

struct LabelInfo {
    1: binary name;
    2: LabelId label_id;
    3: list<binary> primary_keys;
}

struct GetLabelInfoResponse {
    1: Result result;
    2: LabelInfo label_info;
}

struct GetLabelInfosResponse {
    1: Result result;
    2: list<LabelInfo> label_infos;
}

struct CreateIndexRequest {
    1: LabelId label_id;
    2: list<binary> property_names;
}

struct CreateIndexResponse {
    1: Result result;
    2: IndexId index_id;
}

struct IndexInfo {
    1: LabelId label_id;
    2: list<binary> property_names;
}

struct GetIndexInfosResponse {
    1: Result result;
    2: list<IndexInfo> index_infos;
}


service Meta {
    CreateLabelResponse createPrimaryLabel(1: CreatePrimaryLabelRequest req);
    CreateLabelResponse createSecondaryLabel(1: binary label_name);
    Result dropLabel(1: binary label_name);
    GetLabelInfoResponse getLabelInfo(1:binary label_name);
    GetLabelInfosResponse getLabelInfos(1: list<binary> label_names);

    CreateIndexResponse createIndex(1: CreateIndexRequest req);
    // Don't have index names, and the label doesn't identify the index uniquely, therefore
    Result dropIndex(1: IndexId index_id);
    GetIndexInfosResponse getIndexInfos();
    GetIndexInfosResponse getIndexInfosForLabel(1: LabelId label_id);
}
