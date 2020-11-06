Feature: Video Game Sales Example

  Scenario: Find the most frequent game consoles in the database
    Given graph "vg_sales"
    When executing query:
            """
            MATCH (g:Game)-[:Sold]->(c:Console)
            RETURN c.name as Console, COUNT(g) AS NumGames
            ORDER BY NumGames DESC
            LIMIT 10;
            """
    Then the result should be:
            | Console  | NumGames |
            | 'Wii'      | 15       |
            | 'X360'     | 14       |
            | 'DS'       | 13       |
            | 'PS3'      | 9        |
            | '3DS'      | 7        |
            | 'PS2'      | 6        |
            | 'PS4'      | 6        |
            | 'GB'       | 6        |
            | 'PS'       | 5        |
            | 'SNES'     | 4        |



  Scenario: Find a Pokemon franchise, sort them by age and rating if available
    Given graph "vg_sales"
    When executing query:
            """
            MATCH (g:Game)
            WITH g.name AS name, g.year as year, g.critic_score as score
            WHERE toupper(name) CONTAINS  'POKEMON'
            RETURN name AS GameName, year AS Year, ROUND(score) AS CriticScore
            ORDER BY year, score
            LIMIT 10;
            """
    Then the result should be:
            | GameName                                    | Year                                      | CriticScore                               |
            | 'Pokemon Red/Pokemon Blue'                  | 1996                                      | null                                      |
            | 'Pokemon Gold/Pokemon Silver'               | 1999                                      | null                                      |
            | 'Pokemon Ruby/Pokemon Sapphire'             | 2002                                      | null                                      |
            | 'Pokemon FireRed/Pokemon LeafGreen'         | 2004                                      | null                                      |
            | 'Pokemon Diamond/Pokemon Pearl'             | 2006                                      | null                                      |
            | 'Pokemon HeartGold/Pokemon SoulSilver'      | 2009                                      | null                                      |
            | 'Pokemon Black/Pokemon White'               | 2010                                      | null                                      |
            | 'Pokemon Black 2/Pokemon White 2'           | 2012                                      | null                                      |
            | 'Pokemon X/Pokemon Y'                       | 2013                                      | null                                      |
            | 'Pokemon Omega Ruby/Pokemon Alpha Sapphire' | 2014                                      | null                                      |



  Scenario: Add attribute game.TOT and order games by newly constructed argument
    Given graph "vg_sales"
    When executing query:
            """
            MATCH (g:Game)-[s:Sold]-(c:Console)
            WITH g AS game, ROUND(SUM(s.EU + s.NA +s.JP + s.OT) * 100) / 100 AS soldUnits
            SET game.TOT = soldUnits
            RETURN game.name AS Name, game.TOT AS SoldUnits
            ORDER BY SoldUnits DESC
            LIMIT 10;
            """
    Then the result should be:
          | Name                        | SoldUnits                 |
          | 'Wii Sports'                | 82.54                     |
          | 'Grand Theft Auto V'        | 49.94                     |
          | 'Super Mario Bros.'         | 40.24                     |
          | 'Mario Kart Wii'            | 35.52                     |
          | 'Wii Sports Resort'         | 32.77                     |
          | 'Pokemon Red/Pokemon Blue'  | 31.38                     |
          | 'Tetris'                    | 30.26                     |
          | 'New Super Mario Bros.'     | 29.8                      |
          | 'Wii Play'                  | 28.91                     |
          | 'New Super Mario Bros. Wii' | 28.32                     |



  Scenario: Find publishers that published only on single console
    Given graph "vg_sales"
    When executing query:
            """
            MATCH (c:Console)<-[s:Sold]-(g:Game)-[:PublishedBy]->(p:Publisher)
            WHERE p.name IS NOT NULL
            WITH p, COLLECT(c.name) AS consoles, SUM(g.TOT) AS total
            WHERE SIZE(consoles) = 1
            UNWIND consoles AS c
            RETURN p.name AS PublisherName, total AS TotalSales, c AS ConsoleName
            ORDER BY total DESC
            LIMIT 10;
            """
    Then the result should be:
            | PublisherName        | TotalSales         | ConsoleName          |
            | 'Bethesda Softworks' | 8.79               | 'X360'               |
            | 'Sega'               | 8                  | 'Wii'                |
            | 'SquareSoft'         | 7.86               | 'PS'                 |
            | 'Atari'              | 7.81               | '2600'               |



  Scenario: Find sales across different years, print games sold in that year descending by number of sold units
    Given graph "vg_sales"
    When executing query:
            """
            MATCH (g:Game)
            WITH  g, g.TOT AS total
            ORDER BY total DESC
            WITH g.year AS year, COLLECT(g) AS games, ROUND(SUM(total)* 100) / 100 as yearSale
            ORDER BY year
            RETURN year AS Year, EXTRACT(game in games | {game: game.name, sale: game.TOT}) as GameSales, yearSale AS TotalYearSale
            LIMIT 5;
            """
    Then the result should be:
            | Year                                                                                    | GameSales                                                                               | TotalYearSale                                                                           |
            | 1982                                                                                    | [{game: 'Pac-Man', sale: 7.81}]                                                         | 7.81                                                                                    |
            | 1984                                                                                    | [{game: 'Duck Hunt', sale: 28.31}]                                                      | 28.31                                                                                   |
            | 1985                                                                                    | [{game: 'Super Mario Bros.', sale: 40.24}]                                              | 40.24                                                                                   |
            | 1988                                                                                    | [{game: 'Super Mario Bros. 3', sale: 17.28}, {game: 'Super Mario Bros. 2', sale: 7.46}] | 24.74                                                                                   |
            | 1989                                                                                    | [{game: 'Tetris', sale: 30.26}, {game: 'Super Mario Land', sale: 18.14}]                | 48.4                                                                                    |

  Scenario: Find developers by largest critic score, and list their games
    Given graph "vg_sales"
    When executing query:
            """
            MATCH (g:Game)-[:DevelopedBy]->(d:Developer)
            WHERE g.critic_score IS NOT NULL
            WITH d.name AS Developer, AVG(g.critic_score) AS Score, COUNT(g) AS GamesCount, COLLECT(g.name) AS DevelopedGames
            ORDER BY Developer
            WHERE GamesCount >= 2
            RETURN Developer, ROUND(Score * 100) / 100 AS Score, DevelopedGames
            ORDER BY Score DESC
            LIMIT 5;
            """
    Then the result should be:
            | Developer                                                                                                       | Score                                                                                                         | DevelopedGames                                                                                                |
            | 'Rockstar North'                                                                                                | 96.25                                                                                                         | ['Grand Theft Auto IV', 'Grand Theft Auto V', 'Grand Theft Auto: San Andreas', 'Grand Theft Auto: Vice City'] |
            | 'Polyphony Digital'                                                                                             | 91.4                                                                                                          | ['Gran Turismo', 'Gran Turismo 2', 'Gran Turismo 3: A-Spec', 'Gran Turismo 4', 'Gran Turismo 5']              |
            | 'SquareSoft'                                                                                                    | 91.33                                                                                                         | ['Final Fantasy VII', 'Final Fantasy VIII', 'Final Fantasy X']                                                |
            | 'Infinity Ward'                                                                                                 | 87.0                                                                                                            | ['Call of Duty 4: Modern Warfare', 'Call of Duty: Ghosts', 'Call of Duty: Modern Warfare 2']                  |
            | 'Treyarch'                                                                                                      | 85.0                                                                                                            | ['Call of Duty: Black Ops', 'Call of Duty: Black Ops II']                                                     |

  Scenario: Find games that are mostly sold in Japan
    Given graph "vg_sales"
    When executing query:
            """
            MATCH (g:Game)-[s:Sold]->(:Console)
            WITH g AS game, SUM(s.JP) AS jp_sales, SUM(s.NA + s.EU + s.OT) AS other_sales
            WITH game, jp_sales - other_sales AS sales_difference, jp_sales, other_sales
            WHERE sales_difference > 0
            RETURN game.name AS GameName, ROUND(sales_difference * 100) / 100 AS SaleDifference ,ROUND(jp_sales* 100) / 100  AS JapanSales, ROUND(other_sales* 100) / 100  AS OtherSales
            ORDER BY sales_difference DESC
            LIMIT 10;
            """
    Then the result should be:
            | GameName                             | SaleDifference                       | JapanSales                           | OtherSales                           |


  Scenario: Find developers that had increase of sales in each of subsequent games
    Given graph "vg_sales"
    When executing query:
            """
            MATCH (c:Console)<-[s:Sold]-(g:Game)
            WITH  g as game, g.TOT AS sale
            ORDER BY game.year ASC
            MATCH (game)-[:DevelopedBy]->(d:Developer)
            WITH d as developer, COLLECT(game.name) AS games, COLLECT(sale) AS sales
            WHERE SIZE(games) >= 2
            WITH developer, games, sales, EXTRACT(i IN RANGE(1, SIZE(games) - 1) | sales[i] - sales[i-1]) AS difference
            WHERE ALL(diff IN difference WHERE diff > 0)
            RETURN developer.name AS Developer, games AS Games, sales AS Sales;
            """
    Then the result should be:
            | Developer                        | Games                            | Sales                            |
            | 'Ubisoft'                          | ['Just Dance 2', 'Just Dance 3'] | [9.44, 10.12]                    |