MATCH (n) DETACH DELETE n;
CREATE (a: Node {id: 0, message: "say \"something 'nice'\" here"});
CREATE (a: Node {id: 1, message: "say \"something 'else'\" here"});
