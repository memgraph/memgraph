Feature: Restaurant Recommendations

    Scenario: Match all disputed transactions
        Given graph "restaurant_recommendations"
        When executing query:
            """
            MATCH (philip:Person {name:"Philip"})-[:IS_FRIEND_OF]-(person) RETURN person.name AS person ORDER BY person ASC
            """
        Then the result should be:
            | person    |
            | 'Andreas' |
            | 'Emil'    |
            | 'Michael' |

    Scenario: Restaurants in NYC and their cusines
        Given graph "restaurant_recommendations"
        When executing query:
            """
            MATCH (nyc:City {name:"New York"})<-[:LOCATED_IN]-(restaurant)-[:SERVES]->(cusine) RETURN nyc, restaurant, cusine
            """
        Then the result should be:
            |nyc |restaurant|  cusine|
            |(:City {name:'New York'}) | (:Restaurant{name:'Zushi Zam'}) | (:Cuisine{name:'Sushi'}) |
            |(:City {name:'New York'}) | (:Restaurant{name:'iSushi'})    | (:Cuisine{name:'Sushi'}) |

    Scenario: Graph Search Recommendation
        Given graph "restaurant_recommendations"
        When executing query:
            """
            MATCH (philip:Person {name:"Philip"}), (philip)-[:IS_FRIEND_OF]-(friend), (restaurant:Restaurant)-[:LOCATED_IN]->(:City {name:"New York"}), (restaurant)-[:SERVES]->(:Cuisine {name:"Sushi"}), (friend)-[:LIKES]->(restaurant) RETURN restaurant.name AS restaurant, collect(friend.name) as likers, count(*) as occurence ORDER BY occurence DESC
            """
        Then the result should be:
            | restaurant  | likers                 | occurence |
            | 'iSushi'    | ['Andreas', 'Michael'] | 2         |
            | 'Zushi Zam' | ['Andreas']            | 1         |

