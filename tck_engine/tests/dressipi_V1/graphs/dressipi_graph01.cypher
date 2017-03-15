CREATE (g:garment {garment_id: 1234, garment_category_id: 1, conceals: 30});
MATCH(g:garment {garment_id: 1234}) SET g:AA;
MATCH(g:garment {garment_id: 1234}) SET g:BB;
MATCH(g:garment {garment_id: 1234}) SET g:EE;

CREATE (g:garment {garment_id: 2345, garment_category_id: 6, reveals: 10});
MATCH(g:garment {garment_id: 2345}) SET g:CC;
MATCH(g:garment {garment_id: 2345}) SET g:DD;

CREATE (g:garment {garment_id: 3456, garment_category_id: 8});
MATCH(g:garment {garment_id: 3456}) SET g:CC;
MATCH(g:garment {garment_id: 3456}) SET g:DD;

CREATE (g:garment {garment_id: 4567, garment_category_id: 15});
MATCH(g:garment {garment_id: 4567}) SET g:AA;
MATCH(g:garment {garment_id: 4567}) SET g:BB;
MATCH(g:garment {garment_id: 4567}) SET g:DD;

CREATE (g:garment {garment_id: 5678, garment_category_id: 19});
MATCH(g:garment {garment_id: 5678}) SET g:BB;
MATCH(g:garment {garment_id: 5678}) SET g:CC;
MATCH(g:garment {garment_id: 5678}) SET g:EE;

CREATE (g:garment {garment_id: 6789, garment_category_id: 3});
MATCH(g:garment {garment_id: 6789}) SET g:AA;
MATCH(g:garment {garment_id: 6789}) SET g:DD;
MATCH(g:garment {garment_id: 6789}) SET g:EE;

CREATE (g:garment {garment_id: 7890, garment_category_id: 25});
MATCH(g:garment {garment_id: 7890}) SET g:AA;
MATCH(g:garment {garment_id: 7890}) SET g:BB;
MATCH(g:garment {garment_id: 7890}) SET g:CC;
MATCH(g:garment {garment_id: 7890}) SET g:EE;

MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 4567}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 5678}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 6789}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2);

MATCH (g1:garment {garment_id: 4567}), (g2:garment {garment_id: 6789}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 4567}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 4567}), (g2:garment {garment_id: 5678}) CREATE (g1)-[r:default_outfit]->(g2);

MATCH (g1:garment {garment_id: 6789}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2);

MATCH (g1:garment {garment_id: 5678}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2);

MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 3456}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 5678}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 6789}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 4567}) CREATE (g1)-[r:default_outfit]->(g2);

MATCH (g1:garment {garment_id: 3456}), (g2:garment {garment_id: 5678}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 3456}), (g2:garment {garment_id: 6789}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 3456}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2);
MATCH (g1:garment {garment_id: 3456}), (g2:garment {garment_id: 4567}) CREATE (g1)-[r:default_outfit]->(g2);

CREATE (p:profile {profile_id: 111, partner_id: 55, reveals: 30});
CREATE (p:profile {profile_id: 112, partner_id: 55});
CREATE (p:profile {profile_id: 112, partner_id: 77, conceals: 10});

MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 1234}) CREATE (p)-[s:score]->(g) SET s.score=1500;
MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 2345}) CREATE (p)-[s:score]->(g) SET s.score=1200;
MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 3456}) CREATE (p)-[s:score]->(g) SET s.score=1000;
MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 4567}) CREATE (p)-[s:score]->(g) SET s.score=1000;
MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 6789}) CREATE (p)-[s:score]->(g) SET s.score=1500;
MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 7890}) CREATE (p)-[s:score]->(g) SET s.score=1800;

MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 1234}) CREATE (p)-[s:score]->(g) SET s.score=2000;
MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 4567}) CREATE (p)-[s:score]->(g) SET s.score=1500;
MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 5678}) CREATE (p)-[s:score]->(g) SET s.score=1000;
MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 6789}) CREATE (p)-[s:score]->(g) SET s.score=1600;
MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 7890}) CREATE (p)-[s:score]->(g) SET s.score=1900;

MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 1234}) CREATE (p)-[s:score]->(g) SET s.score=1500;
MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 2345}) CREATE (p)-[s:score]->(g) SET s.score=1300;
MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 3456}) CREATE (p)-[s:score]->(g) SET s.score=1300;
MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 5678}) CREATE (p)-[s:score]->(g) SET s.score=1200;
MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 6789}) CREATE (p)-[s:score]->(g) SET s.score=1700;
MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 7890}) CREATE (p)-[s:score]->(g) SET s.score=1900;

MATCH (p:profile {profile_id: 112, partner_id: 77})-[s:score]-(g:garment {garment_id: 1234}) SET s.score = 3137;
MATCH (p:profile {profile_id: 112, partner_id: 77})-[s:score]-(g:garment {garment_id: 1234}) SET s.score = 1500;




















