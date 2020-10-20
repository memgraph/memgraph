Feature: European Backpacking Index Example
  Scenario: List the top 10 cheapest hotels in the dataset
    Given graph "backpacking"
    When executing query:
            """
            MATCH (n:City)
            RETURN n.name, n.cheapest_hostel, n.cost_per_night_USD, n.hostel_url
            ORDER BY n.cost_per_night_USD LIMIT 10;
            """
    Then the result should be:
            | n.name                                              | n.cheapest_hostel                                   | n.cost_per_night_USD                              | n.hostel_url                                        |
            | 'Kiev'                                              | 'ZigZag Hostel'                                     | 5.7                                               | 'https://www.priceoftravel.com/AKievHostel'         |
            | 'Belgrade'                                          | 'Hostel Inn Downtown'                               | 6.1628                                            | 'https://www.priceoftravel.com/ABelgradeHostel'     |
            | 'Saint Petersburg'                                  | 'Plosaty Hostel'                                    | 7.14                                              | 'https://www.priceoftravel.com/AStPetersburgHostel' |
            | 'Santorini'                                         | 'Youth Hostel Anna'                                 | 7.44                                              | 'https://www.priceoftravel.com/ASantoriniHostel'    |
            | 'Istanbul'                                          | 'Chambers of the Boheme'                            | 7.9075                                            | 'https://www.priceoftravel.com/AIstanbulHostel'     |
            | 'Riga'                                              | 'Central Hostel Riga'                               | 8.06                                              | 'https://www.priceoftravel.com/ARigaHostel'         |
            | 'Krakow'                                            | 'Pillows Party Hostel'                              | 8.236                                             | 'https://www.priceoftravel.com/AKrakowHostel'       |
            | 'Budapest'                                          | 'Sziget City Hostel'                                | 8.936                                             | 'https://www.priceoftravel.com/ABudapestHostel'     |
            | 'Zagreb'                                            | 'Hostel Temza'                                      | 9.18                                              | 'https://www.priceoftravel.com/AZagrebHostel'       |
            | 'Bucharest'                                         | 'Little Bucharest Bar & Hostel'                     | 9.315                                             | 'https://www.priceoftravel.com/ABucharestHostel'    |


  Scenario: List the cheapest hotels in Croatia sorted by total price
    Given graph "backpacking"
    When executing query:
            """
            MATCH (c:City)-[:Inside]->(:Country {name: "Croatia"})
            RETURN c.name, c.cheapest_hostel, c.total_USD
            ORDER BY c.total_USD;
            """
    Then the result should be:
            | c.name                         | c.cheapest_hostel              | c.total_USD                  |
            | 'Zagreb'                       | 'Hostel Temza'                 | 38.08                        |
            | 'Split'                        | 'CroParadise Green Hostel'     | 45.9799                      |
            | 'Dubrovnik'                    | 'Hostel City Central Old Town' | 80.036                       |


  Scenario: Sort the countries by number of cheap hotels
    Given graph "backpacking"
    When executing query:
            """
            MATCH (n:Country)<-[:Inside]-(m:City)
            RETURN n.name AS CountryName, COUNT(m) AS HostelCount
            ORDER BY HostelCount DESC, CountryName LIMIT 10;
            """
    Then the result should be:
            | CountryName      | HostelCount    |
            | 'Italy'          | 5              |
            | 'Spain'          | 4              |
            | 'Croatia'        | 3              |
            | 'Germany'        | 3              |
            | 'Austria'        | 2              |
            | 'Belgium'        | 2              |
            | 'Czech Republic' | 2              |
            | 'France'         | 2              |
            | 'Greece'         | 2              |
            | 'Norway'         | 2              |

  Scenario: Shortest path from Spain to Russia (BFS)
    Given graph "backpacking"
    When executing query:
            """
            MATCH p = (n:Country {name: "Spain"})
                      -[r:Borders * bfs]-
                      (m:Country {name: "Russia"})
            UNWIND (nodes(p)) AS rows
            RETURN rows.name;
            """
    Then the result should be:
            | rows.name   |
            | 'Spain'     |
            | 'France'    |
            | 'Germany'   |
            | 'Poland'    |
            | 'Russia'    |

  Scenario: Shortest path from Bratislava to Madrid with Euro as local currency
    Given graph "backpacking"
    When executing query:
            """
            MATCH p = (:City {name: "Bratislava"})
                      -[:CloseTo * bfs (e, v | v.local_currency = "Euro")]-
                      (:City {name: "Madrid"})
            UNWIND (nodes(p)) AS rows
            RETURN rows.name;
            """
    Then the result should be:
            | rows.name    |
            | 'Bratislava' |
            | 'Salzburg'   |
            | 'Milan'      |
            | 'Nice'       |
            | 'Madrid'     |


  Scenario: Cheapest trip from Brussels to Athens with no EU border crossings
    Given graph "backpacking"
    When executing query:
            """
            MATCH p = (:City {name: "Brussels"})
                      -[:CloseTo * wShortest(e, v | v.cost_per_night_USD) total_cost (e, v | e.eu_border=FALSE)]-
                      (:City {name: "Athens"})
            WITH extract(city in nodes(p) | city.name) AS trip, total_cost
            RETURN trip, total_cost;
            """
    Then the result should be:
| trip                                                                           | total_cost                                                                     |
| ['Brussels', 'Berlin', 'Salzburg', 'Budapest', 'Bucharest', 'Sofia', 'Athens'] | 97.5275                                                                        |


  Scenario: Party trip from Madrid to Vienna, sightseeing to Belgrade
    Given graph "backpacking"
    When executing query:
            """
            MATCH p = (:City {name: "Madrid"})
                      -[:CloseTo * wShortest(e, v | v.cost_per_night_USD + v.drinks_USD) cost1]-
                      (:City {name: "Vienna"})
                      -[:CloseTo * wShortest(e, v | v.cost_per_night_USD + v.attractions_USD) cost2]-
                      (:City {name: "Belgrade"})
            WITH extract(city in nodes(p) | city.name) AS trip, cost1, cost2
            RETURN trip, cost1 + cost2 AS total_cost;
            """
    Then the result should be:
| trip                                                           | total_cost                                                     |
| ['Madrid', 'Nice', 'Naples', 'Vienna', 'Budapest', 'Belgrade'] | 143.918                                                        |


    Scenario: Top 10 cheapest routes from Paris to Zagreb sorted by total price and city count
      Given graph "backpacking"
      When executing query:
            """
            MATCH path = (n:City {name: "Paris"})-[:CloseTo *3..5]-(m:City {name: "Zagreb"}) 
            WITH nodes(path) AS trip
            WITH extract(city in trip | [city, trip]) AS lst
            UNWIND lst AS rows
            WITH rows[0] AS city, extract(city in rows[1] | city.name) AS trip
            RETURN trip,
                   toInteger(sum(city.total_USD)) AS trip_cost_USD,
                   count(trip) AS city_count
            ORDER BY trip_cost_USD, city_count DESC LIMIT 10;
            """
      Then the result should be:
| trip                                                         | trip_cost_USD                                                | city_count                                                   |
| ['Paris', 'Naples', 'Ljubljana', 'Zagreb']                   | 240                                                          | 4                                                            |
| ['Paris', 'Florence', 'Ljubljana', 'Zagreb']                 | 255                                                          | 4                                                            |
| ['Paris', 'Rome', 'Ljubljana', 'Zagreb']                     | 263                                                          | 4                                                            |
| ['Paris', 'Milan', 'Ljubljana', 'Zagreb']                    | 266                                                          | 4                                                            |
| ['Paris', 'Naples', 'Ljubljana', 'Budapest', 'Zagreb']       | 275                                                          | 5                                                            |
| ['Paris', 'Hamburg', 'Krakow', 'Kiev', 'Budapest', 'Zagreb'] | 290                                                          | 6                                                            |
| ['Paris', 'Florence', 'Ljubljana', 'Budapest', 'Zagreb']     | 291                                                          | 5                                                            |
| ['Paris', 'Berlin', 'Krakow', 'Kiev', 'Budapest', 'Zagreb']  | 293                                                          | 6                                                            |
| ['Paris', 'Rome', 'Ljubljana', 'Budapest', 'Zagreb']         | 298                                                          | 5                                                            |
| ['Paris', 'Naples', 'Salzburg', 'Budapest', 'Zagreb']        | 298                                                          | 5                                                            |
