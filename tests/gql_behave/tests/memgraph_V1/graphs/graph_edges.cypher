CREATE (:label1 {id: 1})-[:type1 {id:1}]->(:label2 {id: 2})-[:type1 {id: 2}]->(:label3 {id: 3})-[:type1 {id: 3}]->(:label4 {id: 4});
MATCH (n :label1), (m :label3) CREATE (n)-[:type2 {id: 10}]->(m);
MATCH (n :label1) CREATE (n)-[:type3 {id: 20}]->(:label5 { id: 5 });
MATCH (n :label1) CREATE (n)-[:same {id: 30}]->(n);
