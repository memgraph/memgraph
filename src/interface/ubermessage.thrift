//include "address.thrift"

// TODO(gvolfing) remove this once the include problem is resolved
struct Address{
    1: string unique_id;
    2: string last_known_ip;
    3: i32 last_known_port;
}

struct HeartbeatRequest {
  1: bool test;
}

struct HeartbeatResponse {
  1: bool test;
}

struct ScanAllRequest{
  1: bool test;
}

struct ScanAllResponse {
  1: bool test;
}

union ToStorageEngine{
  1: ScanAllRequest scan_all_request;
  2: HeartbeatRequest heartbeat_request;
}

union ToQueryEngine {
  1: ScanAllResponse scan_all_response;
}

union ToCoordinator {
  1: HeartbeatResponse heartbeat_response;
}

union HighLevelUnion {
    1: ToStorageEngine to_storage_engine;
    2: ToQueryEngine to_query_engine;
    3: ToCoordinator to_coordinator;
}

struct UberMessage {
    //1: address.Address to_address;
    //2: address.Address from_address;
    1: Address to_address;
    2: Address from_address;
    3: i64 request_id;
    4: HighLevelUnion high_level_union;
}

service UberServer {
    oneway void ReceiveUberMessage(1: UberMessage uber_message)
}
