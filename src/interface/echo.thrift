struct EchoMessage {
    1: binary message;
}

//struct Address{
//    1: string unique_id;
//    2: string last_known_ip;
//    3: i32 last_known_port;
//}

struct CompoundMessage{
//    1: Address to_address
//    2: Address from_address
    3: binary message
}

service Echo {
    oneway void ReceiveSend(1: EchoMessage m)
    oneway void RecieveCompoundThriftMessage(1: CompoundMessage m)
}
