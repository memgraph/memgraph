Feature: Movies Example

  Scenario: List first 10 movies sorted by title

    Given graph "movies_graph"
    When executing query:
            """
            MATCH (m :Movie) RETURN m.title ORDER BY m.title LIMIT 10;
            """
    Then the result should be:
            | m.title                        |
            | '2 Days in the Valley'         |
            | '20,000 Leagues Under the Sea' |
            | '2001: A Space Odyssey'        |
            | '6'                            |
            | '8 Seconds'                    |
            | 'A Boy Called Hate'            |
            | 'A Bronx Tale'                 |
            | 'A Close Shave'                |
            | 'A Family Thing'               |
            | 'A Farewell to Arms'           |



  Scenario: List last 15 users sorted by name
    Given graph "movies_graph"
    When executing query:
            """
            MATCH (u: User) RETURN u.id as id, u.name ORDER BY u.name DESC, id LIMIT 15;
            """
    Then the result should be:
            | id        | u.name      |
            | 564       | 'Winston'   |
            | 562       | 'Winny'     |
            | 568       | 'Winnifred' |
            | 561       | 'Winnie'    |
            | 197       | 'Winifred'  |
            | 567       | 'Winifred'  |
            | 194       | 'Winfred'   |
            | 565       | 'Winfred'   |
            | 566       | 'Winford'   |
            | 563       | 'Winfield'  |
            | 559       | 'Wilson'    |
            | 553       | 'Willy'     |
            | 558       | 'Willis'    |
            | 552       | 'Willie'    |
            | 64        | 'Williams'  |



  Scenario: List 10 movies those have Comedy and Action genres and sorted by movie title
    Given graph "movies_graph"
    When executing query:
            """
            MATCH (m :Movie)-[:ofGenre]->(:Genre {name:"Action"}), (m)-[:ofGenre]->(:Genre {name:"Comedy"}) RETURN m.title ORDER BY m.title LIMIT 10;
            """
    Then the result should be:
            | m.title                  |
            | 'A Low Down Dirty Shame' |
            | 'Another Stakeout'       |
            | 'Bad Boys'               |
            | 'Bad Girls'              |
            | 'Beat the Devil'         |
            | 'Beverly Hills Cop III'  |
            | 'Bulletproof'            |
            | 'Bullets Over Broadway'  |
            | 'Bushwhacked'            |
            | 'Canadian Bacon'         |

  Scenario: Average score for Star Wars movie:
    Given graph "movies_graph"
    When executing query:
            """
            MATCH (u :User)-[r :Rating]->(m :Movie {title:"Star Wars"}) RETURN toInteger(AVG(r.score)) as score;
            """
    Then the result should be:
            | score   |
            | 3       |

  Scenario: Average scores for first 10 movies
    Given graph "movies_graph"
    When executing query:
            """
            MATCH (u :User)-[r :Rating]->(m:Movie) RETURN m.title as title, toInteger(AVG(r.score)) as score ORDER BY score DESC, title LIMIT 10;
            """
    Then the result should be:
            | title                             | score                           |
            | 'Around the World in Eighty Days' | 5                               |
            | 'Nick of Time'                    | 5                               |
            | 'Ninotchka'                       | 5                               |
            | 'Sense and Sensibility'           | 5                               |
            | 'Singin' in the Rain'             | 5                               |
            | 'The Specialist'                  | 5                               |
            | 'A Pyromaniac's Love Story'       | 4                               |
            | 'Addams Family Values'            | 4                               |
            | 'Basquiat'                        | 4                               |
            | 'Batman'                          | 4                               |

  Scenario: Recommendation for user
    Given graph "movies_graph"
    When executing query:
            """
            MATCH (u:User{id:130})-[r:Rating]-(m:Movie)-[otherR:Rating]-(other:User)
            WITH other.id as otherId, AVG(ABS(r.score-otherR.score)) as similarity, COUNT(*) as similarUserCount
            WHERE similarUserCount > 2
            WITH otherId ORDER BY similarity LIMIT 10
            WITH COLLECT(otherId) as similarUserSet
            MATCH (someMovie: Movie)-[fellowRate:Rating]-(fellowUser:User)
            WHERE fellowUser.id in similarUserSet
            WITH someMovie, toInteger(AVG(fellowRate.score)) as predictionScore
            RETURN someMovie.title as Title, predictionScore ORDER BY predictionScore desc, someMovie.title;
            """
    Then the result should be:
            | Title                          | predictionScore              |
            | 'Beverly Hills Cop III'        | 5                            |
            | 'A Time to Kill'               | 4                            |
            | 'Once Were Warriors'           | 4                            |
            | 'Space Jam'                    | 4                            |
            | 'The 39 Steps'                 | 4                            |
            | 'True Romance'                 | 4                            |
            | '2001: A Space Odyssey'        | 3                            |
            | 'Breakfast at Tiffany's'       | 3                            |
            | 'Casino'                       | 3                            |
            | 'Jurassic Park'                | 3                            |
            | 'Mr. Holland's Opus'           | 3                            |
            | 'Pretty Woman'                 | 3                            |
            | 'Rebecca'                      | 3                            |
            | 'Sleepless in Seattle'         | 3                            |
            | 'Trois couleurs : Rouge'       | 3                            |
            | 'Vertigo'                      | 3                            |
            | 'Beyond Rangoon'               | 2                            |
            | 'To Be or Not to Be'           | 2                            |
            | '20,000 Leagues Under the Sea' | 1                            |
            | 'Four Rooms'                   | 1                            |
