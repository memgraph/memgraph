Feature: European Road Network Example

  Scenario: List all countries in the dataset

    Given graph "europe"
    When executing query:
            """
            MATCH (c:Country) RETURN c.name ORDER BY c.name
            """
    Then the result should be:
	    | c.name                    |
	    | 'Albania'                 |
	    | 'Austria'                 |
	    | 'Belarus'                 |
	    | 'Belgium'                 |
	    | 'Bosnia and Herzegovina'  |
	    | 'Bulgaria'                |
	    | 'Croatia'                 |
	    | 'Cyprus'                  |
	    | 'Czechia'                 |
	    | 'Denmark'                 |
	    | 'Estonia'                 |
	    | 'Finland'                 |
	    | 'France'                  |
	    | 'Germany'                 |
	    | 'Greece'                  |
	    | 'Hungary'                 |
	    | 'Iceland'                 |
	    | 'Ireland'                 |
	    | 'Italy'                   |
	    | 'Kosovo'                  |
	    | 'Latvia'                  |
	    | 'Lithuania'               |
	    | 'Macedonia'               |
	    | 'Moldova'                 |
	    | 'Montenegro'              |
	    | 'Netherlands'             |
	    | 'Norway'                  |
	    | 'Poland'                  |
	    | 'Portugal'                |
	    | 'Romania'                 |
	    | 'Russia'                  |
	    | 'Serbia'                  |
	    | 'Slovakia'                |
	    | 'Slovenia'                |
	    | 'Spain'                   |
	    | 'Sweden'                  |
	    | 'Switzerland'             |
	    | 'Ukraine'                 |
	    | 'United Kingdom'          |


  Scenario: Find all Croatian cities in the road network
    Given graph "europe"
    When executing query:
            """
            MATCH (c:City)-[:In_]->(:Country {name: "Croatia"})
            RETURN c.name ORDER BY c.name
            """
    Then the result should be:
            | c.name      |
            | 'Osijek'    |
            | 'Rijeka'    |
            | 'Split'     |
            | 'Zagreb'    |


  Scenario: Find cities less than 200 km from Zagreb
    Given graph "europe"
    When executing query:
            """
            MATCH (:City {name: "Zagreb"})-[r:Road]->(c:City)
            WHERE r.length < 200
            RETURN c.name ORDER BY c.name;
            """
    Then the result should be:
            | c.name                            |
            | 'Banja Luka'                      |
            | 'Graz'                            |
            | 'Ljubljana'                       |
            | 'Maribor'                         |
            | 'Rijeka'                          |


  Scenario: Shortest path from Zagreb to Paris (BFS)
    Given graph "europe"
    When executing query:
            """
            MATCH p = (:City {name: "Zagreb"})
                      -[:Road * bfs]->
                      (:City {name: "Paris"})
            RETURN nodes(p);
            """
    Then the result should be:
	    | nodes(p)                                                                                                     |
	    | [(:City {name: 'Zagreb'}), (:City {name: 'Bolzano'}), (:City {name: 'Mulhouse'}), (:City {name: 'Paris'})]   |

  Scenario: Shortest path from Zagreb to Paris (BFS <= 200km)
    Given graph "europe"
    When executing query:
            """
            MATCH p = (:City {name: "Zagreb"})
                      -[:Road * bfs (e, v | e.length <= 200)]->
                      (:City {name: "Paris"})
            RETURN nodes(p);
            """
    Then the result should be:
            | nodes(p)                                                                                                                                                                                                                                                                                            |
            | [(:City{name:'Zagreb'}),(:City{name:'Graz'}),(:City{name:'Vienna'}),(:City{name:'Linz'}),(:City{name:'Salzburg'}),(:City{name:'Munich'}),(:City{name:'Augsburg'}),(:City{name:'Esslingen'}),(:City{name:'Ludwigshafen am Rhein'}),(:City{name:'Metz'}),(:City{name:'Reims'}),(:City{name:'Paris'})] |


  Scenario: Shortest path from Zagreb to Paris (BFS <= 200km, no Vienna)
    Given graph "europe"
    When executing query:
            """
            MATCH p = (:City {name: "Zagreb"})
                      -[:Road * bfs (e, v | e.length <= 200 AND v.name != "Vienna")]->
                      (:City {name: "Paris"})
            RETURN nodes(p);
            """
    Then the result should be:
            | nodes(p)                                                                                                                                                                                                                                                                                                                            |
            | [(:City{name:'Zagreb'}),(:City{name:'Ljubljana'}),(:City{name:'Trieste'}),(:City{name:'Mestre'}),(:City{name:'Trento'}),(:City{name:'Innsbruck'}),(:City{name:'Munich'}),(:City{name:'Augsburg'}),(:City{name:'Esslingen'}),(:City{name:'Ludwigshafen am Rhein'}),(:City{name:'Metz'}),(:City{name:'Reims'}),(:City{name:'Paris'})] |


  Scenario: Shortest path from Zagreb to Paris (Dijkstra)
    Given graph "europe"
    When executing query:
            """
            MATCH p = (:City {name: "Zagreb"})
                      -[:Road * wShortest (e, v | e.length) total_weight]->
                      (:City {name: "Paris"})
            RETURN nodes(p) as cities;
            """
    Then the result should be:
            | cities                                                                                                                                         |
            | [(:City{name:'Zagreb'}),(:City{name:'Ljubljana'}),(:City{name:'Bolzano'}),(:City{name:'Basel'}),(:City{name:'Créteil'}),(:City{name:'Paris'})] |


  Scenario: Shortest path from Zagreb to Paris (Dijkstra <= 200km)
    Given graph "europe"
    When executing query:
            """
            MATCH p = (:City {name: "Zagreb"})
                      -[:Road * wShortest (e, v | e.length) total_weight (e, v | e.length <= 200)]->
                      (:City {name: "Paris"})
            RETURN nodes(p) as cities;
            """
    Then the result should be:
            | cities                                                                                                                                                                                                                                                                                    |
            | [(:City{name:'Zagreb'}),(:City{name:'Graz'}),(:City{name:'Vienna'}),(:City{name:'Linz'}),(:City{name:'Salzburg'}),(:City{name:'Munich'}),(:City{name:'Augsburg'}),(:City{name:'Pforzheim'}),(:City{name:'Saarbrücken'}),(:City{name:'Metz'}),(:City{name:'Reims'}),(:City{name:'Paris'})] |


  Scenario: Ten cities furthest away from Zagreb
    Given graph "europe"
    When executing query:
            """
            MATCH (:City {name: "Zagreb"})
                  -[:Road * wShortest (e, v | e.length) total_weight]->
                  (c:City)
            RETURN c.name
            ORDER BY total_weight DESC LIMIT 10;
            """
    Then the result should be:
            |  c.name                    |
            | 'Norilsk'                  |
            | 'Vorkuta'                  |
            | 'Novyy Urengoy'            |
            | 'Noyabrsk'                 |
            | 'Ukhta'                    |
            | 'Severodvinsk'             |
            | 'Arkhangel’sk'             |
            | 'Khabarovsk'               |
            | 'Blagoveshchensk'          |
            | 'Petropavlovsk-Kamchatsky' |
