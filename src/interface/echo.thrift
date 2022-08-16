struct EchoMessage {
    1: binary message;
}

service Echo {
    oneway void ReceiveSend(1: EchoMessage m)
}
