CREATE (r:RonnieColeman {id: 1})-[w:WALKS]->(g: Gym {id: 2, property: "Yeaaah Buddyyyy!!"});
MATCH (g: Gym) CREATE (g)-[h:HAS]->(w: Weight {id: 3, prop : "SOLID 800 POUNDS"});
CREATE (k: KyriakosGrizzly {id: 4})-[w:WALKS]->(g2: Gym_in_Greece{id: 5, property: "AAAAAAA!!!"});
MATCH (g2:Gym_in_Greece) CREATE (g2)-[h:HAS]->(u:UnconventionalExercise {id: 6});
