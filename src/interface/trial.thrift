struct Ping {
    1: binary message;
}

struct Pong{
    1: binary message;
}

service PingPong {
    Pong ping(1: Ping req)
}