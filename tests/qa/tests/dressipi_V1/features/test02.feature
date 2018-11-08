Feature: Test02

	Scenario: First test from graph_queries
		Given graph "dressipi_graph01"
		When executing query:
		"""
		MATCH (a:garment)-[:default_outfit]-(b:garment)-[:default_outfit]-(c:garment)-[:default_outfit]-(d:garment)-[:default_outfit]-(a:garment)-[:default_outfit]-(c:garment), (b:garment)-[:default_outfit]-(d:garment) WHERE a.garment_id=1234 RETURN a.garment_id,b.garment_id,c.garment_id,d.garment_id 
		"""
		Then the result should be
		| a.garment_id     | b.garment_id     | c.garment_id     | d.garment_id     |
		| 1234             | 5678             | 4567             | 7890             |
		| 1234             | 6789             | 4567             | 7890             |
		| 1234             | 4567             | 5678             | 7890             |
		| 1234             | 4567             | 6789             | 7890             |
		| 1234             | 7890             | 4567             | 6789             |
		| 1234             | 4567             | 7890             | 6789             |
		| 1234             | 7890             | 4567             | 5678             |
		| 1234             | 4567             | 7890             | 5678             |
		| 1234             | 5678             | 7890             | 4567             |
		| 1234             | 7890             | 5678             | 4567             |
		| 1234             | 7890             | 6789             | 4567             |
		| 1234             | 6789             | 7890             | 4567             |


	Scenario: Second test from graph_queries
		Given graph "dressipi_graph01"
		When executing query:
		"""
		MATCH (a:garment)-[:default_outfit]-(b:garment)-[:default_outfit]-(c:garment)-[:default_outfit]-(d:garment)-[:default_outfit]-(a:garment)-[:default_outfit]-(c:garment), (b:garment)-[:default_outfit]-(d:garment), (e:profile {profile_id: 112, partner_id: 55})-[s1:score]-(a:garment),(e:profile {profile_id: 112, partner_id: 55})-[s2:score]-(b:garment), (e:profile {profile_id: 112, partner_id: 55})-[s3:score]-(c:garment), (e:profile {profile_id: 112, partner_id: 55})-[s4:score]-(d:garment) WHERE a.garment_id=1234 RETURN a.garment_id,b.garment_id,c.garment_id,d.garment_id, s1.score+s2.score+s3.score+s4.score ORDER BY s1.score+s2.score+s3.score+s4.score DESC LIMIT 10
		"""
		Then the result should be, in order
		| a.garment_id | b.garment_id | c.garment_id | d.garment_id | s1.score+s2.score+s3.score+s4.score |
		| 1234         | 7890         | 4567         | 6789         | 7000|
		| 1234         | 7890         | 6789         | 4567         | 7000|
		| 1234         | 6789         | 4567         | 7890         | 7000|
		| 1234         | 6789         | 7890         | 4567         | 7000|
		| 1234         | 4567         | 6789         | 7890         | 7000|
		| 1234         | 4567         | 7890         | 6789         | 7000|
		| 1234         | 7890         | 4567         | 5678         | 6400|
		| 1234         | 7890         | 5678         | 4567         | 6400|
		| 1234         | 5678         | 4567         | 7890         | 6400|
		| 1234         | 5678         | 7890         | 4567         | 6400|

