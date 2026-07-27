CREATE (d:Dog {name: "Rex", id: 0})-[h:HUNTS {id: 0}]->(c:Cat {name: "Tom", id: 1})-[catc:CATCHES {id:1}]->(m:Mouse {name: "Squiggles", id: 2});
MATCH (d:Dog) CREATE (d)-[bff:BEST_FRIENDS {id:2}]->(c:Cat {name: "Rinko", id: 3});
MATCH (c:Cat {name: "Tom"}) CREATE (h:Human {name: "Matija", id: 4})-[o:OWNS {id:3}]->(c);
MATCH (d:Dog {name: "Rex"}),(h:Human {name:"Matija"}) CREATE (h)-[o:PLAYS_WITH {id:4}]->(d);
MATCH (d:Dog {name: "Rex"}) CREATE (d)-[l:LIVES {id:5}]->(z:Zadar {id: 5});
MATCH (m:Mouse {name: "Squiggles"}), (z:Zadar) CREATE (m)-[r:RUNS_THROUGH {id:6}]->(z);
MATCH (z:Zadar) CREATE (h:Human {name: "Dena", id: 6})-[g:GOES_TO {id:7}]->(z);
