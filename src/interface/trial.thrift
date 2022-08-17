struct Ping {
    1: binary message;
}

struct Pong{
    1: binary message;
}

struct ValueToAdd{
    1:  i64 val;
}

struct ValueToAddResopnse{
    1:  i64 new_val;
}

service PingPong {
    Pong ping(1: Ping req)
    ValueToAddResopnse AddValue(1: ValueToAdd req)
}
