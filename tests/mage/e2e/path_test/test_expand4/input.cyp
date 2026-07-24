CREATE (d:Dog {name: "Rex"})-[h:HUNTS]->(c:Cat {name: "Tom"})-[catc:CATCHES]->(m:Mouse {name: "Squiggles"});
MATCH (d:Dog) CREATE (d)-[bff:BEST_FRIENDS]->(c:Cat {name: "Rinko"});
MATCH (c:Cat {name: "Tom"}) CREATE (h:Human {name: "Matija"})-[o:OWNS]->(c);
MATCH (d:Dog {name: "Rex"}),(h:Human {name:"Matija"}) CREATE (h)-[o:PLAYS_WITH]->(d);
MATCH (d:Dog {name: "Rex"}) CREATE (d)-[l:LIVES]->(z:Zadar);
MATCH (m:Mouse {name: "Squiggles"}), (z:Zadar) CREATE (m)-[r:RUNS_THROUGH]->(z);
MATCH (z:Zadar) CREATE (h:Human {name: "Dena"})-[g:GOES_TO]->(z);
