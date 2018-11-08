Feature: Test01

	Scenario: Tests from graph_queries
		Given an empty graph
		
		When executing query:
		"""
		CREATE (g:garment {garment_id: 1234, garment_category_id: 1, conceals: 30}) RETURN g
		"""
		Then the result should be
		| g                                                                   |
		| (:garment {garment_id: 1234, garment_category_id: 1, conceals: 30}) |
		
		When executing query:
		"""
		MATCH(g:garment {garment_id: 1234}) SET g:AA RETURN g
		"""
		Then the result should be
		| g                                                                      |
		| (:garment:AA {garment_id: 1234, garment_category_id: 1, conceals: 30}) |
		
		When executing query:
		"""
		MATCH(g:garment {garment_id: 1234}) SET g:BB RETURN g
		"""
		Then the result should be
		| g                                                                         |
		| (:garment:AA:BB {garment_id: 1234, garment_category_id: 1, conceals: 30}) |
		
		When executing query:
		"""
		MATCH(g:garment {garment_id: 1234}) SET g:EE RETURN g
		"""
		Then the result should be
		| g                                                                            |
		| (:garment:AA:BB:EE {garment_id: 1234, garment_category_id: 1, conceals: 30}) |

		

		
		When executing query:
		"""
		CREATE (g:garment {garment_id: 2345, garment_category_id: 6, reveals: 10}) RETURN g
		"""
		Then the result should be
		| g                                                                  |
		| (:garment {garment_id: 2345, garment_category_id: 6, reveals: 10}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 2345}) SET g:CC RETURN g
		"""
		Then the result should be
		| g                                                                  |
		| (:garment:CC {garment_id: 2345, garment_category_id: 6, reveals: 10}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 2345}) SET g:DD RETURN g
		"""
		Then the result should be
		| g                                                                        |
		| (:garment:CC:DD {garment_id: 2345, garment_category_id: 6, reveals: 10}) |




		When executing query:
		"""
		CREATE (g:garment {garment_id: 3456, garment_category_id: 8}) RETURN g
		"""
		Then the result should be
		| g                                                     |
		| (:garment {garment_id: 3456, garment_category_id: 8}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 3456}) SET g:CC RETURN g
		"""
		Then the result should be
		| g                                                        |
		| (:garment:CC {garment_id: 3456, garment_category_id: 8}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 3456}) SET g:DD RETURN g
		"""
		Then the result should be
		| g                                                           |
		| (:garment:CC:DD {garment_id: 3456, garment_category_id: 8}) |




		When executing query:
		"""
		CREATE (g:garment {garment_id: 4567, garment_category_id: 15}) RETURN g
		"""
		Then the result should be
		| g                                                      |
		| (:garment {garment_id: 4567, garment_category_id: 15}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 4567}) SET g:AA RETURN g
		"""
		Then the result should be
		| g                                                         |
		| (:garment:AA {garment_id: 4567, garment_category_id: 15}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 4567}) SET g:BB RETURN g
		"""
		Then the result should be
		| g                                                            |
		| (:garment:AA:BB {garment_id: 4567, garment_category_id: 15}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 4567}) SET g:DD RETURN g
		"""
		Then the result should be
		| g                                                               |
		| (:garment:AA:BB:DD {garment_id: 4567, garment_category_id: 15}) |




		When executing query:
		"""
		CREATE (g:garment {garment_id: 5678, garment_category_id: 19}) RETURN g
		"""
		Then the result should be
		| g                                                      |
		| (:garment {garment_id: 5678, garment_category_id: 19}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 5678}) SET g:BB RETURN g
		"""
		Then the result should be
		| g                                                         |
		| (:garment:BB {garment_id: 5678, garment_category_id: 19}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 5678}) SET g:CC RETURN g
		"""
		Then the result should be
		| g                                                            |
		| (:garment:CC:BB {garment_id: 5678, garment_category_id: 19}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 5678}) SET g:EE RETURN g
		"""
		Then the result should be
		| g                                                               |
		| (:garment:CC:BB:EE {garment_id: 5678, garment_category_id: 19}) |



		When executing query:
		"""
		CREATE (g:garment {garment_id: 6789, garment_category_id: 3}) RETURN g
		"""
		Then the result should be
		| g                                                      |
		| (:garment {garment_id: 6789, garment_category_id: 3}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 6789}) SET g:AA RETURN g
		"""
		Then the result should be
		| g                                                         |
		| (:garment:AA {garment_id: 6789, garment_category_id: 3}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 6789}) SET g:DD RETURN g
		"""
		Then the result should be
		| g                                                            |
		| (:garment:AA:DD {garment_id: 6789, garment_category_id: 3})  |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 6789}) SET g:EE RETURN g
		"""
		Then the result should be
		| g                                                               |
		| (:garment:AA:DD:EE {garment_id: 6789, garment_category_id: 3})  |



		When executing query:
		"""
		CREATE (g:garment {garment_id: 7890, garment_category_id: 25}) RETURN g
		"""
		Then the result should be
		| g                                                      |
		| (:garment {garment_id: 7890, garment_category_id: 25}) |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 7890}) SET g:AA RETURN g
		"""
		Then the result should be
		| g                                                          |
		| (:garment:AA {garment_id: 7890, garment_category_id: 25})  |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 7890}) SET g:BB RETURN g
		"""
		Then the result should be
		| g                                                             |
		| (:garment:AA:BB {garment_id: 7890, garment_category_id: 25})  |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 7890}) SET g:CC RETURN g
		"""
		Then the result should be
		| g                                                                |
		| (:garment:AA:BB:CC {garment_id: 7890, garment_category_id: 25})  |

		When executing query:
		"""
		MATCH(g:garment {garment_id: 7890}) SET g:EE RETURN g
		"""
		Then the result should be
		| g                                                                   |
		| (:garment:AA:BB:CC:EE {garment_id: 7890, garment_category_id: 25})  |







		When executing query:
		"""
		MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 4567}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 5678}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 6789}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 1234}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |



		When executing query:
		"""
		MATCH (g1:garment {garment_id: 4567}), (g2:garment {garment_id: 6789}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 4567}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 4567}), (g2:garment {garment_id: 5678}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 	
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		


		When executing query:
		"""
		MATCH (g1:garment {garment_id: 6789}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 	
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |




		When executing query:
		"""
		MATCH (g1:garment {garment_id: 5678}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 	
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |




		When executing query:
		"""
		MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 3456}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 5678}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 6789}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 	
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 2345}), (g2:garment {garment_id: 4567}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |




		When executing query:
		"""
		MATCH (g1:garment {garment_id: 3456}), (g2:garment {garment_id: 5678}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 3456}), (g2:garment {garment_id: 6789}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 	
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 3456}), (g2:garment {garment_id: 7890}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |

		When executing query:
		"""
		MATCH (g1:garment {garment_id: 3456}), (g2:garment {garment_id: 4567}) CREATE (g1)-[r:default_outfit]->(g2) RETURN r 
		"""
		Then the result should be
		| r                 |
		| [:default_outfit] |







		When executing query:
		"""
		CREATE (p:profile {profile_id: 111, partner_id: 55, reveals: 30}) RETURN p 
		"""
		Then the result should be
		| p                                                         |
		| (:profile {profile_id: 111, partner_id: 55, reveals: 30}) |

		When executing query:
		"""
		CREATE (p:profile {profile_id: 112, partner_id: 55}) RETURN p 
		"""
		Then the result should be
		| p                                            |
		| (:profile {profile_id: 112, partner_id: 55}) |

		When executing query:
		"""
		CREATE (p:profile {profile_id: 112, partner_id: 77, conceals: 10}) RETURN p 
		"""
		Then the result should be
		| p                                                          |
		| (:profile {profile_id: 112, partner_id: 77,  conceals: 10}) |




		When executing query:
		"""
		MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 1234}) CREATE (p)-[s:score]->(g) SET s.score=1500 RETURN s  
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1500}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 2345}) CREATE (p)-[s:score]->(g) SET s.score=1200 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1200}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 3456}) CREATE (p)-[s:score]->(g) SET s.score=1000 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1000}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 4567}) CREATE (p)-[s:score]->(g) SET s.score=1000 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1000}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 6789}) CREATE (p)-[s:score]->(g) SET s.score=1500 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1500}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 111, partner_id: 55}), (g:garment {garment_id: 7890}) CREATE (p)-[s:score]->(g) SET s.score=1800 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1800}] |




		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 1234}) CREATE (p)-[s:score]->(g) SET s.score=2000 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 2000}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 4567}) CREATE (p)-[s:score]->(g) SET s.score=1500 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1500}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 5678}) CREATE (p)-[s:score]->(g) SET s.score=1000 RETURN s
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1000}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 6789}) CREATE (p)-[s:score]->(g) SET s.score=1600 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1600}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 55}), (g:garment {garment_id: 7890}) CREATE (p)-[s:score]->(g) SET s.score=1900 RETURN s
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1900}] |




		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 1234}) CREATE (p)-[s:score]->(g) SET s.score=1500 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1500}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 2345}) CREATE (p)-[s:score]->(g) SET s.score=1300 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1300}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 3456}) CREATE (p)-[s:score]->(g) SET s.score=1300 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1300}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 5678}) CREATE (p)-[s:score]->(g) SET s.score=1200 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1200}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 6789}) CREATE (p)-[s:score]->(g) SET s.score=1700 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1700}] |

		When executing query:
		"""
		MATCH  (p:profile {profile_id: 112, partner_id: 77}), (g:garment {garment_id: 7890}) CREATE (p)-[s:score]->(g) SET s.score=1900 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1900}] |




		When executing query:
		"""
		MATCH (g:garment {garment_id: 1234}) RETURN g 
		"""
		Then the result should be
		| g                                                                           |
		| (:garment:AA:BB:EE {garment_id: 1234, conceals:30, garment_category_id: 1}) |

		When executing query:
		"""
		MATCH (p:profile {profile_id: 112, partner_id: 77}) RETURN p 
		"""
		Then the result should be
		| p                      |
		| (:profile {profile_id: 112, partner_id: 77, conceals:10}) |

		When executing query:
		"""
		MATCH (p:profile {profile_id: 112, partner_id: 77})-[s:score]-(g:garment {garment_id: 1234}) RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1500}] |

		When executing query:
		"""
		MATCH (p:profile {profile_id: 112, partner_id: 77})-[s:score]-(g:garment {garment_id: 1234}) SET s.score = 3137 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 3137}] |

		When executing query:
		"""
		MATCH (p:profile {profile_id: 112, partner_id: 77})-[s:score]-(g:garment {garment_id: 1234}) SET s.score = 1500 RETURN s 
		"""
		Then the result should be
		| s                      |
		| [:score {score: 1500}] |

		

		
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

