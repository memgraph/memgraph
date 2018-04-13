Feature: New Football Example

  Scenario: Find all supported leagues

    Given graph "football"
    When executing query:

      """
      MATCH (n:Game)
      RETURN DISTINCT n.league AS League
      ORDER BY League;
      """
    Then the result should be:

      |          League         |
      | 'BEL-Jupiler League'    |
      | 'ENG-Championship'      |
      | 'ENG-Conference'        |
      | 'ENG-League 1'          |
      | 'ENG-League 2'          |
      | 'ENG-Premier League'    |
      | 'ESP-La Liga'           |
      | 'ESP-La Liga 2'         |
      | 'FRA-Ligue 1'           |
      | 'FRA-Ligue 2'           |
      | 'GER-Bundesliga'        |
      | 'GER-Bundesliga 2'      |
      | 'GRE-Ethniki Katigoria' |
      | 'ITA-Serie A'           |
      | 'ITA-Serie B'           |
      | 'NED-Eredivisie'        |
      | 'POR-Liga 1'            |
      | 'SCO-Division 1'        |
      | 'SCO-Division 2'        |
      | 'SCO-Division 3'        |
      | 'SCO-Premiership'       |
      | 'TUR-Ligi 1'            |

  Scenario: Season interval for supported leagues

    Given graph "football"
    When executing query:

      """
      MATCH (n:Game)
      RETURN DISTINCT n.league AS League, MIN(n.season) AS Oldest, MAX(n.season) AS Newest
      ORDER BY League;
      """
    Then the result should be:

      |         League          | Oldest | Newest |
      | 'BEL-Jupiler League'    | 2007   | 2016   |
      | 'ENG-Championship'      | 2007   | 2016   |
      | 'ENG-Conference'        | 2007   | 2016   |
      | 'ENG-League 1'          | 2007   | 2016   |
      | 'ENG-League 2'          | 2007   | 2016   |
      | 'ENG-Premier League'    | 2007   | 2016   |
      | 'ESP-La Liga'           | 2007   | 2016   |
      | 'ESP-La Liga 2'         | 2007   | 2016   |
      | 'FRA-Ligue 1'           | 2007   | 2016   |
      | 'FRA-Ligue 2'           | 2007   | 2016   |
      | 'GER-Bundesliga'        | 2007   | 2016   |
      | 'GER-Bundesliga 2'      | 2007   | 2016   |
      | 'GRE-Ethniki Katigoria' | 2007   | 2016   |
      | 'ITA-Serie A'           | 2007   | 2016   |
      | 'ITA-Serie B'           | 2007   | 2016   |
      | 'NED-Eredivisie'        | 2007   | 2016   |
      | 'POR-Liga 1'            | 2007   | 2016   |
      | 'SCO-Division 1'        | 2007   | 2016   |
      | 'SCO-Division 2'        | 2007   | 2016   |
      | 'SCO-Division 3'        | 2007   | 2016   |
      | 'SCO-Premiership'       | 2007   | 2016   |
      | 'TUR-Ligi 1'            | 2007   | 2016   |

  Scenario: Chronological order of Chelsea and Arsenal games.

    Given graph "football"
    When executing query:

      """
      MATCH (n:Team {name: "Chelsea"})-[e:Played]->(w:Game)<-[f:Played]-(m:Team {name: "Arsenal"})
      RETURN w.date AS Date, e.side AS Chelsea, f.side AS Arsenal,
             w.FT_home_score AS home_score, w.FT_away_score AS away_score
      ORDER BY Date;
      """
    Then the result should be:

      | Date         | Chelsea   | Arsenal   | home_score | away_score |
      | '2007-12-16' | 'away'    | 'home'    | 1          | 0          |
      | '2008-03-23' | 'home'    | 'away'    | 2          | 1          |
      | '2008-11-30' | 'home'    | 'away'    | 1          | 2          |
      | '2009-05-10' | 'away'    | 'home'    | 1          | 4          |
      | '2009-11-29' | 'away'    | 'home'    | 0          | 3          |
      | '2010-02-07' | 'home'    | 'away'    | 2          | 0          |
      | '2010-10-03' | 'home'    | 'away'    | 2          | 0          |
      | '2010-12-27' | 'away'    | 'home'    | 3          | 1          |
      | '2011-10-29' | 'home'    | 'away'    | 3          | 5          |
      | '2012-04-21' | 'away'    | 'home'    | 0          | 0          |
      | '2012-09-29' | 'away'    | 'home'    | 1          | 2          |
      | '2013-01-20' | 'home'    | 'away'    | 2          | 1          |
      | '2013-12-23' | 'away'    | 'home'    | 0          | 0          |
      | '2014-03-22' | 'home'    | 'away'    | 6          | 0          |
      | '2014-10-05' | 'home'    | 'away'    | 2          | 0          |
      | '2015-04-26' | 'away'    | 'home'    | 0          | 0          |
      | '2015-09-19' | 'home'    | 'away'    | 2          | 0          |
      | '2016-01-24' | 'away'    | 'home'    | 0          | 1          |
      | '2016-09-24' | 'away'    | 'home'    | 3          | 0          |
      | '2017-02-04' | 'home'    | 'away'    | 3          | 1          |

  Scenario: Chronological order of Chelsea and Arsenal games which Chelsea won.

    Given graph "football"
    When executing query:

      """
      MATCH (n:Team {name: "Chelsea"})-[e:Played {outcome: "won"}]->
            (w:Game)<-[f:Played]-(m:Team {name: "Arsenal"})
      RETURN w.date AS Date, e.side AS Chelsea, f.side AS Arsenal,
             w.FT_home_score AS home_score, w.FT_away_score AS away_score
      ORDER BY Date;
      """

    Then the result should be:

      | Date         | Chelsea   | Arsenal   | home_score | away_score |
      | '2008-03-23' | 'home'    | 'away'    | 2          | 1          |
      | '2009-05-10' | 'away'    | 'home'    | 1          | 4          |
      | '2009-11-29' | 'away'    | 'home'    | 0          | 3          |
      | '2010-02-07' | 'home'    | 'away'    | 2          | 0          |
      | '2010-10-03' | 'home'    | 'away'    | 2          | 0          |
      | '2012-09-29' | 'away'    | 'home'    | 1          | 2          |
      | '2013-01-20' | 'home'    | 'away'    | 2          | 1          |
      | '2014-03-22' | 'home'    | 'away'    | 6          | 0          |
      | '2014-10-05' | 'home'    | 'away'    | 2          | 0          |
      | '2015-09-19' | 'home'    | 'away'    | 2          | 0          |
      | '2016-01-24' | 'away'    | 'home'    | 0          | 1          |
      | '2017-02-04' | 'home'    | 'away'    | 3          | 1          |

  Scenario: Number of home defeats during 2016/17 EPL season per team.

    Given graph "football"
    When executing query:

      """
      MATCH (n:Team)-[:Played {side: "home", outcome: "lost"}]->
            (w:Game {league: "ENG-Premier League", season: 2016})
      RETURN n.name AS Team, count(w) AS home_defeats
      ORDER BY home_defeats, Team;
      """

    Then the result should be:

      | Team                   | home_defeats |
      | 'Manchester City'      | 1            |
      | 'Manchester United'    | 1            |
      | 'Arsenal'              | 2            |
      | 'Chelsea'              | 2            |
      | 'Everton'              | 2            |
      | 'Liverpool'            | 2            |
      | 'Leicester City'       | 5            |
      | 'AFC Bournemouth'      | 6            |
      | 'Burnley'              | 6            |
      | 'Stoke City'           | 6            |
      | 'Hull City'            | 7            |
      | 'Southampton'          | 7            |
      | 'Watford'              | 7            |
      | 'Swansea City'         | 8            |
      | 'West Bromwich Albion' | 8            |
      | 'West Ham United'      | 8            |
      | 'Middlesbrough'        | 9            |
      | 'Crystal Palace'       | 11           |
      | 'Sunderland'           | 11           |

  Scenario: Number of points Chelsea had at the end of 2016/17 EPL season.

    Given graph "football"
    When executing query:

      """
      MATCH (n:Team {name: "Chelsea"})-[:Played {outcome: "drew"}]->(w:Game {season: 2016})
      WITH n, COUNT(w) AS draw_points
      MATCH (n)-[:Played {outcome: "won"}]->(w:Game {season: 2016})
      RETURN draw_points + 3 * COUNT(w) AS total_points;
      """

    Then the result should be:

      | total_points |
      | 93           |

  Scenario: EPL 2016/17 table

    Given graph "football"
    When executing query:

      """
      MATCH (n)-[:Played {outcome: "drew"}]->(w:Game {league: "ENG-Premier League", season: 2016})
      WITH n, COUNT(w) AS draw_points
      MATCH (n)-[:Played {outcome: "won"}]->(w:Game {league: "ENG-Premier League", season: 2016})
      RETURN n.name AS Team, draw_points + 3 * COUNT(w) AS total_points
      ORDER BY total_points DESC;
      """
    Then the result should be:

      |          Team          | total_points |
      | 'Chelsea'              | 93           |
      | 'Tottenham Hotspur'    | 86           |
      | 'Manchester City'      | 78           |
      | 'Liverpool'            | 76           |
      | 'Arsenal'              | 75           |
      | 'Manchester United'    | 69           |
      | 'Everton'              | 61           |
      | 'AFC Bournemouth'      | 46           |
      | 'Southampton'          | 46           |
      | 'West Bromwich Albion' | 45           |
      | 'West Ham United'      | 45           |
      | 'Leicester City'       | 44           |
      | 'Stoke City'           | 44           |
      | 'Crystal Palace'       | 41           |
      | 'Swansea City'         | 41           |
      | 'Burnley'              | 40           |
      | 'Watford'              | 40           |
      | 'Hull City'            | 34           |
      | 'Middlesbrough'        | 28           |
      | 'Sunderland'           | 24           |

  Scenario: Number of goals per league (not AVG due to precision issues with tck_engine]

    Given graph "football"
    When executing query:

      """
      MATCH (w:Game {season: 2016})
      RETURN w.league AS League, SUM(w.FT_home_score) + SUM(w.FT_away_score) AS Goals, COUNT(w) AS Games
      ORDER BY Goals DESC;
      """

    Then the result should be:

      |          League         | Goals | Games |
      | 'ENG-Conference'        | 1504  | 552   |
      | 'ENG-League 2'          | 1465  | 552   |
      | 'ENG-Championship'      | 1441  | 552   |
      | 'ENG-League 1'          | 1417  | 552   |
      | 'ITA-Serie A'           | 1123  | 380   |
      | 'ESP-La Liga'           | 1118  | 380   |
      | 'ENG-Premier League'    | 1064  | 380   |
      | 'ESP-La Liga 2'         | 1041  | 462   |
      | 'ITA-Serie B'           | 1021  | 462   |
      | 'FRA-Ligue 1'           | 994   | 380   |
      | 'FRA-Ligue 2'           | 943   | 380   |
      | 'NED-Eredivisie'        | 884   | 306   |
      | 'GER-Bundesliga'        | 877   | 306   |
      | 'TUR-Ligi 1'            | 828   | 306   |
      | 'GER-Bundesliga 2'      | 761   | 306   |
      | 'POR-Liga 1'            | 728   | 306   |
      | 'BEL-Jupiler League'    | 658   | 240   |
      | 'SCO-Premiership'       | 628   | 228   |
      | 'GRE-Ethniki Katigoria' | 556   | 240   |
      | 'SCO-Division 3'        | 531   | 180   |
      | 'SCO-Division 2'        | 507   | 180   |
      | 'SCO-Division 1'        | 469   | 180   |

  Scenario: Number of comebacks during 2016/17 season across all leagues

    Given graph "football"
    When executing query:

      """
      MATCH (g:Game) WHERE
      (g.HT_result = "H" AND g.FT_result = "A") OR
      (g.HT_result = "A" AND g.FT_result = "H")
      RETURN g.league AS League, count(g) AS Comebacks
      ORDER BY Comebacks DESC;
      """

    Then the result should be:

      |         League          | Comebacks |
      | 'ENG-League 1'          | 267       |
      | 'ENG-Conference'        | 264       |
      | 'ENG-League 2'          | 257       |
      | 'ENG-Championship'      | 250       |
      | 'ITA-Serie B'           | 179       |
      | 'ESP-La Liga 2'         | 172       |
      | 'ITA-Serie A'           | 166       |
      | 'ESP-La Liga'           | 164       |
      | 'FRA-Ligue 2'           | 157       |
      | 'NED-Eredivisie'        | 155       |
      | 'ENG-Premier League'    | 151       |
      | 'FRA-Ligue 1'           | 151       |
      | 'TUR-Ligi 1'            | 149       |
      | 'GER-Bundesliga'        | 136       |
      | 'GER-Bundesliga 2'      | 136       |
      | 'BEL-Jupiler League'    | 128       |
      | 'SCO-Division 3'        | 110       |
      | 'POR-Liga 1'            | 107       |
      | 'SCO-Division 2'        | 92        |
      | 'SCO-Premiership'       | 88        |
      | 'SCO-Division 1'        | 84        |
      | 'GRE-Ethniki Katigoria' | 83        |

  Scenario: Cyclic victories in EPL 2016/2017 (A beats B beats C beats A).

    Given graph "football"
    When executing query:

      """
      MATCH (a)-[:Played {outcome: "won"}]->(p:Game {league: "ENG-Premier League", season: 2016})<--
            (b)-[:Played {outcome: "won"}]->(q:Game {league: "ENG-Premier League", season: 2016})<--
            (c)-[:Played {outcome: "won"}]->(r:Game {league: "ENG-Premier League", season: 2016})<--(a)
      WHERE p.date < q.date AND q.date < r.date
      RETURN a.name AS Team1, b.name AS Team2, c.name AS Team3;
      """

    Then the result should be:

      | Team1                          | Team2                          | Team3                          |
      | 'Burnley'                      | 'Liverpool'                    | 'Leicester City'               |
      | 'West Ham United'              | 'AFC Bournemouth'              | 'West Bromwich Albion'         |
      | 'Swansea City'                 | 'Burnley'                      | 'Liverpool'                    |
      | 'West Ham United'              | 'AFC Bournemouth'              | 'Everton'                      |
      | 'Stoke City'                   | 'Sunderland'                   | 'AFC Bournemouth'              |
      | 'Leicester City'               | 'Burnley'                      | 'Watford'                      |
      | 'Burnley'                      | 'Liverpool'                    | 'West Bromwich Albion'         |
      | 'Crystal Palace'               | 'Stoke City'                   | 'Swansea City'                 |
      | 'Southampton'                  | 'West Ham United'              | 'Crystal Palace'               |
      | 'Southampton'                  | 'Burnley'                      | 'Crystal Palace'               |
      | 'Southampton'                  | 'Swansea City'                 | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Arsenal'                      | 'AFC Bournemouth'              |
      | 'AFC Bournemouth'              | 'West Bromwich Albion'         | 'Burnley'                      |
      | 'AFC Bournemouth'              | 'Stoke City'                   | 'Burnley'                      |
      | 'Manchester City'              | 'Manchester United'            | 'Leicester City'               |
      | 'Manchester City'              | 'West Bromwich Albion'         | 'Leicester City'               |
      | 'Manchester City'              | 'Sunderland'                   | 'Leicester City'               |
      | 'Everton'                      | 'Stoke City'                   | 'Watford'                      |
      | 'Everton'                      | 'West Bromwich Albion'         | 'Watford'                      |
      | 'Leicester City'               | 'Burnley'                      | 'AFC Bournemouth'              |
      | 'Arsenal'                      | 'Burnley'                      | 'Everton'                      |
      | 'Arsenal'                      | 'Chelsea'                      | 'Everton'                      |
      | 'Arsenal'                      | 'Southampton'                  | 'Everton'                      |
      | 'Arsenal'                      | 'Watford'                      | 'Everton'                      |
      | 'Watford'                      | 'Leicester City'               | 'Manchester City'              |
      | 'Middlesbrough'                | 'AFC Bournemouth'              | 'Liverpool'                    |
      | 'Burnley'                      | 'Everton'                      | 'West Ham United'              |
      | 'Watford'                      | 'West Ham United'              | 'Sunderland'                   |
      | 'AFC Bournemouth'              | 'Hull City'                    | 'Southampton'                  |
      | 'Arsenal'                      | 'Chelsea'                      | 'Manchester City'              |
      | 'Hull City'                    | 'Leicester City'               | 'Manchester City'              |
      | 'Leicester City'               | 'Burnley'                      | 'Everton'                      |
      | 'West Ham United'              | 'Sunderland'                   | 'Leicester City'               |
      | 'West Ham United'              | 'AFC Bournemouth'              | 'Leicester City'               |
      | 'Manchester City'              | 'AFC Bournemouth'              | 'Liverpool'                    |
      | 'Swansea City'                 | 'Burnley'                      | 'AFC Bournemouth'              |
      | 'Watford'                      | 'Manchester United'            | 'Tottenham Hotspur'            |
      | 'Southampton'                  | 'Burnley'                      | 'Everton'                      |
      | 'Burnley'                      | 'Liverpool'                    | 'Manchester City'              |
      | 'Crystal Palace'               | 'Stoke City'                   | 'Swansea City'                 |
      | 'Crystal Palace'               | 'Middlesbrough'                | 'Swansea City'                 |
      | 'Chelsea'                      | 'Manchester United'            | 'Tottenham Hotspur'            |
      | 'Sunderland'                   | 'AFC Bournemouth'              | 'Stoke City'                   |
      | 'Southampton'                  | 'West Ham United'              | 'Burnley'                      |
      | 'AFC Bournemouth'              | 'West Bromwich Albion'         | 'Hull City'                    |
      | 'Manchester City'              | 'AFC Bournemouth'              | 'Everton'                      |
      | 'Liverpool'                    | 'Arsenal'                      | 'Swansea City'                 |
      | 'Liverpool'                    | 'West Bromwich Albion'         | 'Swansea City'                 |
      | 'Liverpool'                    | 'Middlesbrough'                | 'Swansea City'                 |
      | 'Liverpool'                    | 'Arsenal'                      | 'Swansea City'                 |
      | 'Burnley'                      | 'Everton'                      | 'Arsenal'                      |
      | 'Leicester City'               | 'Crystal Palace'               | 'Southampton'                  |
      | 'Leicester City'               | 'Burnley'                      | 'Southampton'                  |
      | 'AFC Bournemouth'              | 'Swansea City'                 | 'Crystal Palace'               |
      | 'AFC Bournemouth'              | 'Everton'                      | 'Crystal Palace'               |
      | 'Arsenal'                      | 'Sunderland'                   | 'Watford'                      |
      | 'Arsenal'                      | 'Stoke City'                   | 'Watford'                      |
      | 'Leicester City'               | 'Manchester City'              | 'Burnley'                      |
      | 'Southampton'                  | 'Middlesbrough'                | 'Swansea City'                 |
      | 'Southampton'                  | 'West Ham United'              | 'Swansea City'                 |
      | 'Southampton'                  | 'AFC Bournemouth'              | 'Swansea City'                 |
      | 'Crystal Palace'               | 'Stoke City'                   | 'Sunderland'                   |
      | 'Crystal Palace'               | 'Stoke City'                   | 'Sunderland'                   |
      | 'AFC Bournemouth'              | 'Liverpool'                    | 'Everton'                      |
      | 'Liverpool'                    | 'Arsenal'                      | 'Hull City'                    |
      | 'Liverpool'                    | 'Chelsea'                      | 'Hull City'                    |
      | 'Liverpool'                    | 'West Bromwich Albion'         | 'Hull City'                    |
      | 'Liverpool'                    | 'Chelsea'                      | 'Hull City'                    |
      | 'Burnley'                      | 'Liverpool'                    | 'Watford'                      |
      | 'Tottenham Hotspur'            | 'Swansea City'                 | 'Liverpool'                    |
      | 'Tottenham Hotspur'            | 'Hull City'                    | 'Liverpool'                    |
      | 'Leicester City'               | 'Manchester City'              | 'Swansea City'                 |
      | 'AFC Bournemouth'              | 'Liverpool'                    | 'Manchester City'              |
      | 'AFC Bournemouth'              | 'Everton'                      | 'Manchester City'              |
      | 'Middlesbrough'                | 'Swansea City'                 | 'Crystal Palace'               |
      | 'Middlesbrough'                | 'Sunderland'                   | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Chelsea'                      | 'Leicester City'               |
      | 'Liverpool'                    | 'West Bromwich Albion'         | 'Leicester City'               |
      | 'Liverpool'                    | 'Watford'                      | 'Leicester City'               |
      | 'Liverpool'                    | 'Sunderland'                   | 'Leicester City'               |
      | 'Liverpool'                    | 'Everton'                      | 'Leicester City'               |
      | 'Liverpool'                    | 'Chelsea'                      | 'Leicester City'               |
      | 'Liverpool'                    | 'Swansea City'                 | 'Leicester City'               |
      | 'Hull City'                    | 'Southampton'                  | 'Leicester City'               |
      | 'Hull City'                    | 'Swansea City'                 | 'Leicester City'               |
      | 'Arsenal'                      | 'AFC Bournemouth'              | 'Liverpool'                    |
      | 'Arsenal'                      | 'Swansea City'                 | 'Liverpool'                    |
      | 'Arsenal'                      | 'Swansea City'                 | 'Liverpool'                    |
      | 'Arsenal'                      | 'Hull City'                    | 'Liverpool'                    |
      | 'Middlesbrough'                | 'AFC Bournemouth'              | 'Stoke City'                   |
      | 'Burnley'                      | 'Liverpool'                    | 'Swansea City'                 |
      | 'Burnley'                      | 'AFC Bournemouth'              | 'Swansea City'                 |
      | 'Watford'                      | 'Hull City'                    | 'Southampton'                  |
      | 'Watford'                      | 'Everton'                      | 'Southampton'                  |
      | 'Watford'                      | 'West Ham United'              | 'Southampton'                  |
      | 'West Bromwich Albion'         | 'West Ham United'              | 'Crystal Palace'               |
      | 'West Bromwich Albion'         | 'Swansea City'                 | 'Crystal Palace'               |
      | 'West Bromwich Albion'         | 'West Ham United'              | 'Crystal Palace'               |
      | 'West Bromwich Albion'         | 'Sunderland'                   | 'Crystal Palace'               |
      | 'West Bromwich Albion'         | 'Stoke City'                   | 'Crystal Palace'               |
      | 'Sunderland'                   | 'Leicester City'               | 'Manchester City'              |
      | 'Watford'                      | 'West Ham United'              | 'Crystal Palace'               |
      | 'Watford'                      | 'Manchester United'            | 'Crystal Palace'               |
      | 'Watford'                      | 'West Ham United'              | 'Crystal Palace'               |
      | 'Watford'                      | 'Everton'                      | 'Crystal Palace'               |
      | 'Swansea City'                 | 'Leicester City'               | 'Hull City'                    |
      | 'Burnley'                      | 'Leicester City'               | 'Liverpool'                    |
      | 'Swansea City'                 | 'Burnley'                      | 'AFC Bournemouth'              |
      | 'Swansea City'                 | 'Crystal Palace'               | 'AFC Bournemouth'              |
      | 'Swansea City'                 | 'Crystal Palace'               | 'AFC Bournemouth'              |
      | 'West Bromwich Albion'         | 'Watford'                      | 'Everton'                      |
      | 'West Ham United'              | 'Sunderland'                   | 'AFC Bournemouth'              |
      | 'West Ham United'              | 'Hull City'                    | 'AFC Bournemouth'              |
      | 'West Ham United'              | 'Crystal Palace'               | 'AFC Bournemouth'              |
      | 'West Ham United'              | 'Crystal Palace'               | 'AFC Bournemouth'              |
      | 'Hull City'                    | 'Southampton'                  | 'Everton'                      |
      | 'Arsenal'                      | 'Chelsea'                      | 'West Bromwich Albion'         |
      | 'Arsenal'                      | 'Crystal Palace'               | 'West Bromwich Albion'         |
      | 'West Ham United'              | 'Sunderland'                   | 'Leicester City'               |
      | 'West Ham United'              | 'AFC Bournemouth'              | 'Leicester City'               |
      | 'West Ham United'              | 'Burnley'                      | 'Leicester City'               |
      | 'West Ham United'              | 'Swansea City'                 | 'Leicester City'               |
      | 'Burnley'                      | 'Liverpool'                    | 'Tottenham Hotspur'            |
      | 'Chelsea'                      | 'West Ham United'              | 'Crystal Palace'               |
      | 'Chelsea'                      | 'Leicester City'               | 'Crystal Palace'               |
      | 'Chelsea'                      | 'Burnley'                      | 'Crystal Palace'               |
      | 'Chelsea'                      | 'Manchester United'            | 'Crystal Palace'               |
      | 'Chelsea'                      | 'West Ham United'              | 'Crystal Palace'               |
      | 'Chelsea'                      | 'Everton'                      | 'Crystal Palace'               |
      | 'Chelsea'                      | 'Sunderland'                   | 'Crystal Palace'               |
      | 'Chelsea'                      | 'Stoke City'                   | 'Crystal Palace'               |
      | 'Stoke City'                   | 'Sunderland'                   | 'Leicester City'               |
      | 'Stoke City'                   | 'Burnley'                      | 'Leicester City'               |
      | 'Stoke City'                   | 'Swansea City'                 | 'Leicester City'               |
      | 'West Ham United'              | 'AFC Bournemouth'              | 'Hull City'                    |
      | 'West Ham United'              | 'Sunderland'                   | 'Hull City'                    |
      | 'Everton'                      | 'Leicester City'               | 'Liverpool'                    |
      | 'Sunderland'                   | 'Crystal Palace'               | 'Watford'                      |
      | 'Stoke City'                   | 'Watford'                      | 'Burnley'                      |
      | 'Stoke City'                   | 'Watford'                      | 'Burnley'                      |
      | 'Stoke City'                   | 'Swansea City'                 | 'Burnley'                      |
      | 'Sunderland'                   | 'AFC Bournemouth'              | 'Leicester City'               |
      | 'West Bromwich Albion'         | 'Southampton'                  | 'Watford'                      |
      | 'West Bromwich Albion'         | 'Crystal Palace'               | 'Watford'                      |
      | 'Manchester City'              | 'Crystal Palace'               | 'Chelsea'                      |
      | 'Middlesbrough'                | 'Sunderland'                   | 'Hull City'                    |
      | 'Watford'                      | 'Manchester United'            | 'Tottenham Hotspur'            |
      | 'Hull City'                    | 'Leicester City'               | 'Manchester City'              |
      | 'Stoke City'                   | 'Swansea City'                 | 'Liverpool'                    |
      | 'Stoke City'                   | 'Hull City'                    | 'Liverpool'                    |
      | 'Swansea City'                 | 'Liverpool'                    | 'Tottenham Hotspur'            |
      | 'West Bromwich Albion'         | 'Crystal Palace'               | 'Southampton'                  |
      | 'West Bromwich Albion'         | 'Burnley'                      | 'Southampton'                  |
      | 'West Bromwich Albion'         | 'Swansea City'                 | 'Southampton'                  |
      | 'West Bromwich Albion'         | 'West Ham United'              | 'Southampton'                  |
      | 'Swansea City'                 | 'Leicester City'               | 'West Ham United'              |
      | 'Leicester City'               | 'Burnley'                      | 'Everton'                      |
      | 'Leicester City'               | 'Liverpool'                    | 'Everton'                      |
      | 'Arsenal'                      | 'Burnley'                      | 'Crystal Palace'               |
      | 'Arsenal'                      | 'Swansea City'                 | 'Crystal Palace'               |
      | 'Arsenal'                      | 'Chelsea'                      | 'Crystal Palace'               |
      | 'Arsenal'                      | 'Swansea City'                 | 'Crystal Palace'               |
      | 'Arsenal'                      | 'West Ham United'              | 'Crystal Palace'               |
      | 'Arsenal'                      | 'Sunderland'                   | 'Crystal Palace'               |
      | 'Arsenal'                      | 'Stoke City'                   | 'Crystal Palace'               |
      | 'Arsenal'                      | 'Southampton'                  | 'Crystal Palace'               |
      | 'Burnley'                      | 'Watford'                      | 'Everton'                      |
      | 'Burnley'                      | 'Liverpool'                    | 'Everton'                      |
      | 'Burnley'                      | 'Liverpool'                    | 'Everton'                      |
      | 'Southampton'                  | 'Everton'                      | 'Manchester City'              |
      | 'Hull City'                    | 'Leicester City'               | 'Stoke City'                   |
      | 'Hull City'                    | 'Liverpool'                    | 'Stoke City'                   |
      | 'AFC Bournemouth'              | 'Liverpool'                    | 'Tottenham Hotspur'            |
      | 'Swansea City'                 | 'Burnley'                      | 'Watford'                      |
      | 'Swansea City'                 | 'Sunderland'                   | 'Watford'                      |
      | 'Swansea City'                 | 'Southampton'                  | 'Watford'                      |
      | 'Swansea City'                 | 'Crystal Palace'               | 'Watford'                      |
      | 'Swansea City'                 | 'Crystal Palace'               | 'Watford'                      |
      | 'Chelsea'                      | 'Watford'                      | 'Manchester United'            |
      | 'West Bromwich Albion'         | 'Swansea City'                 | 'Liverpool'                    |
      | 'West Bromwich Albion'         | 'Hull City'                    | 'Liverpool'                    |
      | 'West Bromwich Albion'         | 'Leicester City'               | 'Liverpool'                    |
      | 'Middlesbrough'                | 'Sunderland'                   | 'AFC Bournemouth'              |
      | 'Middlesbrough'                | 'Hull City'                    | 'AFC Bournemouth'              |
      | 'Watford'                      | 'Middlesbrough'                | 'Hull City'                    |
      | 'Watford'                      | 'West Ham United'              | 'Hull City'                    |
      | 'Watford'                      | 'Arsenal'                      | 'Hull City'                    |
      | 'Watford'                      | 'Leicester City'               | 'Hull City'                    |
      | 'Watford'                      | 'Everton'                      | 'Hull City'                    |
      | 'Stoke City'                   | 'Hull City'                    | 'Swansea City'                 |
      | 'Stoke City'                   | 'Watford'                      | 'Swansea City'                 |
      | 'Stoke City'                   | 'Watford'                      | 'Swansea City'                 |
      | 'Liverpool'                    | 'Leicester City'               | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Swansea City'                 | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Chelsea'                      | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Arsenal'                      | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Swansea City'                 | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Everton'                      | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Sunderland'                   | 'Crystal Palace'               |
      | 'Liverpool'                    | 'Stoke City'                   | 'Crystal Palace'               |
      | 'Leicester City'               | 'Manchester City'              | 'Arsenal'                      |
      | 'Leicester City'               | 'Liverpool'                    | 'Arsenal'                      |
      | 'Leicester City'               | 'Crystal Palace'               | 'Arsenal'                      |
      | 'Sunderland'                   | 'Crystal Palace'               | 'Middlesbrough'                |
      | 'Sunderland'                   | 'Hull City'                    | 'Middlesbrough'                |
      | 'Sunderland'                   | 'AFC Bournemouth'              | 'Middlesbrough'                |
      | 'Crystal Palace'               | 'Stoke City'                   | 'Burnley'                      |
      | 'West Bromwich Albion'         | 'Southampton'                  | 'Leicester City'               |
      | 'West Bromwich Albion'         | 'Burnley'                      | 'Leicester City'               |
      | 'West Bromwich Albion'         | 'Swansea City'                 | 'Leicester City'               |
      | 'West Bromwich Albion'         | 'Arsenal'                      | 'Leicester City'               |
      | 'Everton'                      | 'Crystal Palace'               | 'Chelsea'                      |
      | 'Sunderland'                   | 'Hull City'                    | 'AFC Bournemouth'              |
      | 'Arsenal'                      | 'Chelsea'                      | 'Tottenham Hotspur'            |
      | 'Watford'                      | 'Hull City'                    | 'Liverpool'                    |
      | 'Watford'                      | 'Leicester City'               | 'Liverpool'                    |
      | 'Tottenham Hotspur'            | 'Manchester City'              | 'West Ham United'              |
      | 'Tottenham Hotspur'            | 'Chelsea'                      | 'West Ham United'              |
      | 'Tottenham Hotspur'            | 'Hull City'                    | 'West Ham United'              |
      | 'Hull City'                    | 'Swansea City'                 | 'Sunderland'                   |
      | 'Hull City'                    | 'Southampton'                  | 'Sunderland'                   |
      | 'Hull City'                    | 'Leicester City'               | 'Sunderland'                   |
      | 'Hull City'                    | 'Middlesbrough'                | 'Sunderland'                   |
      | 'Hull City'                    | 'AFC Bournemouth'              | 'Sunderland'                   |
      | 'Watford'                      | 'Manchester United'            | 'Leicester City'               |
      | 'Watford'                      | 'Everton'                      | 'Leicester City'               |
      | 'Watford'                      | 'Manchester United'            | 'Leicester City'               |
      | 'Watford'                      | 'Everton'                      | 'Leicester City'               |
      | 'Watford'                      | 'Arsenal'                      | 'Leicester City'               |
      | 'Crystal Palace'               | 'Chelsea'                      | 'Manchester City'              |
      | 'Everton'                      | 'Stoke City'                   | 'Swansea City'                 |
      | 'Everton'                      | 'West Bromwich Albion'         | 'Swansea City'                 |
      | 'Everton'                      | 'Middlesbrough'                | 'Swansea City'                 |
      | 'Everton'                      | 'West Ham United'              | 'Swansea City'                 |
      | 'Everton'                      | 'Arsenal'                      | 'Swansea City'                 |
      | 'Everton'                      | 'Manchester City'              | 'Swansea City'                 |
      | 'Everton'                      | 'AFC Bournemouth'              | 'Swansea City'                 |
      | 'Everton'                      | 'West Ham United'              | 'Swansea City'                 |
      | 'Manchester United'            | 'West Bromwich Albion'         | 'Arsenal'                      |
      | 'Manchester United'            | 'Crystal Palace'               | 'Arsenal'                      |
      | 'Manchester United'            | 'Tottenham Hotspur'            | 'Arsenal'                      |
      | 'Southampton'                  | 'Everton'                      | 'Arsenal'                      |
      | 'Southampton'                  | 'Crystal Palace'               | 'Arsenal'                      |
      | 'Watford'                      | 'Swansea City'                 | 'Everton'                      |
      | 'West Bromwich Albion'         | 'Crystal Palace'               | 'Chelsea'                      |
      | 'Burnley'                      | 'Crystal Palace'               | 'AFC Bournemouth'              |
      | 'Burnley'                      | 'Everton'                      | 'AFC Bournemouth'              |
      | 'Middlesbrough'                | 'Swansea City'                 | 'Southampton'                  |
      | 'Stoke City'                   | 'Watford'                      | 'Arsenal'                      |
      | 'Stoke City'                   | 'Watford'                      | 'Arsenal'                      |
      | 'Stoke City'                   | 'Crystal Palace'               | 'Arsenal'                      |
      | 'Sunderland'                   | 'AFC Bournemouth'              | 'Swansea City'                 |
      | 'Sunderland'                   | 'Hull City'                    | 'Swansea City'                 |
      | 'Sunderland'                   | 'AFC Bournemouth'              | 'Swansea City'                 |
      | 'Sunderland'                   | 'Watford'                      | 'Swansea City'                 |
      | 'Hull City'                    | 'Leicester City'               | 'Crystal Palace'               |
      | 'Hull City'                    | 'Swansea City'                 | 'Crystal Palace'               |
      | 'Hull City'                    | 'Swansea City'                 | 'Crystal Palace'               |
      | 'Hull City'                    | 'Southampton'                  | 'Crystal Palace'               |
      | 'Manchester United'            | 'West Ham United'              | 'Tottenham Hotspur'            |
      | 'West Ham United'              | 'AFC Bournemouth'              | 'Liverpool'                    |
      | 'West Ham United'              | 'Swansea City'                 | 'Liverpool'                    |
      | 'West Ham United'              | 'Hull City'                    | 'Liverpool'                    |
      | 'West Ham United'              | 'Crystal Palace'               | 'Liverpool'                    |
      | 'West Ham United'              | 'Crystal Palace'               | 'Liverpool'                    |
      | 'Watford'                      | 'Manchester United'            | 'Chelsea'                      |
      | 'Sunderland'                   | 'Watford'                      | 'Arsenal'                      |
      | 'Sunderland'                   | 'Crystal Palace'               | 'Arsenal'                      |
      | 'West Bromwich Albion'         | 'Leicester City'               | 'Manchester City'              |
      | 'Middlesbrough'                | 'AFC Bournemouth'              | 'Liverpool'                    |
      | 'Middlesbrough'                | 'Swansea City'                 | 'Liverpool'                    |
      | 'Middlesbrough'                | 'Hull City'                    | 'Liverpool'                    |
      | 'Sunderland'                   | 'Crystal Palace'               | 'Chelsea'                      |
      | 'Leicester City'               | 'West Ham United'              | 'Tottenham Hotspur'            |
      | 'Leicester City'               | 'West Ham United'              | 'Tottenham Hotspur'            |
      | 'Hull City'                    | 'Liverpool'                    | 'Tottenham Hotspur'            |
      | 'Hull City'                    | 'West Ham United'              | 'Tottenham Hotspur'            |
      | 'Burnley'                      | 'Everton'                      | 'West Ham United'              |
      | 'Burnley'                      | 'AFC Bournemouth'              | 'West Ham United'              |
      | 'Burnley'                      | 'Leicester City'               | 'West Ham United'              |
      | 'Burnley'                      | 'Liverpool'                    | 'West Ham United'              |
      | 'Everton'                      | 'West Bromwich Albion'         | 'Arsenal'                      |
      | 'Everton'                      | 'West Bromwich Albion'         | 'Arsenal'                      |
      | 'Everton'                      | 'Crystal Palace'               | 'Arsenal'                      |
      | 'Crystal Palace'               | 'Arsenal'                      | 'Manchester United'            |
      | 'Southampton'                  | 'Leicester City'               | 'Stoke City'                   |
      | 'Southampton'                  | 'Burnley'                      | 'Stoke City'                   |
      | 'Southampton'                  | 'Swansea City'                 | 'Stoke City'                   |
      | 'West Bromwich Albion'         | 'West Ham United'              | 'Swansea City'                 |
      | 'West Bromwich Albion'         | 'Hull City'                    | 'Swansea City'                 |
      | 'West Bromwich Albion'         | 'AFC Bournemouth'              | 'Swansea City'                 |
      | 'West Bromwich Albion'         | 'West Ham United'              | 'Swansea City'                 |
      | 'West Bromwich Albion'         | 'Watford'                      | 'Swansea City'                 |
      | 'Watford'                      | 'Leicester City'               | 'Manchester City'              |
      | 'Watford'                      | 'Everton'                      | 'Manchester City'              |
