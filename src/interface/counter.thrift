namespace cpp2 interface.counter
// https://stackoverflow.com/a/34234874/6639989

struct GetLatestReqeuest {
  1: optional i64 proposed_value;
}

struct GetLatestResponse {
  1: i64 value;
}

union CounterRequest {
  1: GetLatestReqeuest get_latest;
}

union CounterResponse {
  1: GetLatestResponse get_latest;
}

service Counter {
  CounterResponse Request(1: CounterRequest req)
}
