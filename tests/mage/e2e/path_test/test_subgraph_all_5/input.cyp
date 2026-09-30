CREATE (w:Wolf)-[ca:CATCHES]->(d:Dog), (c:Cat), (m:Mouse), (h:Human);
MATCH (w:Wolf), (d:Dog), (c:Cat), (m:Mouse), (h:Human) WITH w, d, c, m, h CREATE (d)-[:CATCHES]->(c) CREATE (c)-[:CATCHES]->(m) CREATE (d)-[:FRIENDS_WITH]->(m) CREATE (h)-[:OWNS]->(d) CREATE (h)-[:HUNTS]->(w) CREATE (h)-[:HATES]->(m);
