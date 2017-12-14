Feature: TED Talk Example

  Scenario: Find all talks given by specific speaker

    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Speaker {name: "Hans Rosling"})-[:Gave]->(m)
            RETURN m.title
            """
    Then the result should be:
            |              m.title                                                        |
            | 'The best stats you've ever seen'                                           |
            | 'New insights on poverty'                                                   |
            | 'Insights on HIV, in stunning data visuals'                                 |
            | 'Let my dataset change your mindset'                                        |
            | 'Asia's rise -- how and when'                                               |
            | 'Global population growth, box by box'                                      |
            | 'The good news of the decade? We're winning the war against child mortality'|
            | 'The magic washing machine'                                                 |
            | 'Religions and babies'                                                      |


  Scenario: Find the top 20 speakers with most talks given
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Speaker)-[:Gave]->(m)
            RETURN n.name, COUNT(m) as TalksGiven
            ORDER BY TalksGiven DESC LIMIT 20;
            """
    Then the result should be:
            |    n.name             | TalksGiven  |
            | 'Hans Rosling'        |      9      |
            | 'Juan Enriquez'       |      7      |
            | 'Rives'               |      6      |
            | 'Marco Tempest'       |      6      |
            | 'Bill Gates'          |      5      |
            | 'Dan Ariely'          |      5      |
            | 'Jacqueline Novogratz'|      5      |
            | 'Nicholas Negroponte' |      5      |
            | 'Clay Shirky'         |      5      |
            | 'Julian Treasure'     |      5      |
            | 'Ken Robinson'        |      4      |
            | 'Lawrence Lessig'     |      4      |
            | 'Barry Schwartz'      |      4      |
            | 'Stewart Brand'       |      4      |
            | 'David Pogue'         |      4      |
            | 'Kevin Kelly'         |      4      |
            | 'Steven Johnson'      |      4      |
            | 'Jonathan Haidt'      |      4      |
            | 'Chris Anderson'      |      4      |
            | 'Michael Green'       |      3      |


  Scenario: Find talks realted by tag to specific talk and count them
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Talk {name: "Michael Green: Why we should build wooden skyscrapers"})-[:HasTag]->(t:Tag)<-[:HasTag]-(m:Talk)
            WITH * ORDER BY m.name
            RETURN t.name, COLLECT(m.name), COUNT(m) AS TalksCount
            ORDER BY TalksCount DESC
           """
    Then the result should be:
            |      t.name                 |    COLLECT(m.name)    |          TalksCount         |
            |      'design'               |['Julian Treasure: Shh! Sound health in 8 steps','Julian Treasure: Why architects need to use their ears','Marco Tempest: A magical tale (with augmented reality)','Marco Tempest: The electric rise and fall of Nikola Tesla','Marco Tempest: The magic of truth and lies (and iPods)','Nicholas Negroponte: 5 predictions, from 1984','Nicholas Negroponte: One Laptop per Child','Nicholas Negroponte: One Laptop per Child, two years on','Nicholas Negroponte: Taking OLPC to Colombia','Rives: A story of mixed emoticons','Steven Johnson: How the 'ghost map' helped end a killer disease','Steven Johnson: The Web as a city','Steven Johnson: The playful wonderland behind great inventions','Stewart Brand: The Long Now']                                 |             14              |
            |      'architecture'         |['Julian Treasure: Why architects need to use their ears','Stewart Brand: The Long Now']                                                       |              2              |


  Scenario: Find 20 most fequently used tags
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (t:Tag)<-[:HasTag]-(n:Talk)
            RETURN t.name as Tag, COUNT(n) AS TalksCount
            ORDER BY TalksCount DESC, Tag LIMIT 20;
            """
    Then the result should be:
            |Tag                 |   TalksCount  |
            |'technology'        |      45       |
            |'culture'           |      31       |
            |'global issues'     |      30       |
            |'business'          |      23       |
            |'science'           |      20       |
            |'economics'         |      18       |
            |'entertainment'     |      16       |
            |'TED Brain Trust'   |      15       |
            |'design'            |      15       |
            |'future'            |      14       |
            |'psychology'        |      11       |
            |'education'         |      9        |
            |'global development'|      9        |
            |'innovation'        |      8        |
            |'invention'         |      8        |
            |'society'           |      8        |
            |'statistics'        |      8        |
            |'Africa'            |      7        |
            |'TEDx'              |      7        |
            |'collaboration'     |      7        |


  Scenario: Find 20 talks most rated as Funny
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (r:Rating{name:"Funny"})<-[e:HasRating]-(m:Talk)
            RETURN m.name, e.user_count ORDER BY e.user_count DESC LIMIT 20;
            """
    Then the result should be:
            |           m.name                                            |e.user_count|
            |'Ken Robinson: Do schools kill creativity?'                  |   19645    |
            |'Ken Robinson: Bring on the learning revolution!'            |   3000     |
            |'Rives: The  4 a.m. mystery'                                 |   2460     |
            |'Dan Ariely: Are we in control of our own decisions?'        |   2031     |
            |'Ken Robinson: How to escape education's death valley'       |   1541     |
            |'Barry Schwartz: The paradox of choice'                      |   1526     |
            |'Hans Rosling: The best stats you've ever seen'              |   1390     |
            |'Hans Rosling: New insights on poverty'                      |   1021     |
            |'David Pogue: Simplicity sells'                              |   964      |
            |'Rives: If I controlled the Internet'                        |   949      |
            |'Hans Rosling: The magic washing machine'                    |   869      |
            |'Julian Treasure: How to speak so that people want to listen'|   810      |
            |'David Pogue: 10 top time-saving tech tips'                  |   726      |
            |'Hans Rosling: Asia's rise -- how and when'                  |   651      |
            |'Rives: A story of mixed emoticons'                          |   630      |
            |'David Pogue: The music wars'                                |   604      |
            |'Ken Robinson: Changing education paradigms'                 |   574      |
            |'David Pogue: Cool tricks your phone can do'                 |   571      |
            |'Rives: The Museum of Four in the Morning'                   |   548      |
            |'Marco Tempest: The magic of truth and lies (and iPods)'     |   527      |


  Scenario: Find inspiring talks and their speakers from the field of technology
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Talk)-[:HasTag]->(m:Tag {name: "technology"})
            MATCH (n)-[r:HasRating]->(p:Rating {name: "Inspiring"})
            MATCH (n)<-[:Gave]-(s:Speaker)
            WHERE r.user_count > 1000
            RETURN n.title, s.name, r.user_count ORDER BY r.user_count DESC;
            """
    Then the result should be:
            |    n.title                             |    s.name            |    r.user_count    |
            |'Where good ideas come from'            |'Steven Johnson'      |      1404          |
            |'How web video powers global innovation'|'Chris Anderson'      |      1193          |
            |'Laws that choke creativity'            |'Lawrence Lessig'     |      1074          |


  Scenario: If you've just watched a talk from a certain speaker(e.g. Hans Rosling) you might be interested
            in finding more talks from the same speaker on a similar topic
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Speaker {name: "Hans Rosling"})-[:Gave]->(m:Talk)
            MATCH (t:Talk {title: "New insights on poverty"})-[:HasTag]->(tag:Tag)<-[:HasTag]-(m)
            WITH * ORDER BY tag.name
            RETURN m.title AS Title, COLLECT(tag.name), COUNT(tag) as TagCount
            ORDER BY TagCount DESC, Title;
            """
    Then the result should be:
            |    Title                                  |   COLLECT(tag.name)         |     TagCount      |
            |'The best stats you've ever seen'          |['Africa','Asia','Google','economics','global development','global issues','health','statistics','visualizations']                                   |9|
            |'Let my dataset change your mindset'       |['Africa','Asia','global development','global issues','statistics','visualizations']                                                                 |6|
            |'Asia's rise -- how and when'              |['Asia','economics','health','statistics','visualizations']|5|
            |'The good news of the decade? We're winning the war against child mortality'|['Africa','global development','health','inequality','statistics']                                            |5|
            |'Insights on HIV, in stunning data visuals'|['Africa','global issues','statistics','visualizations']|4|
            |'Global population growth, box by box'     |['global issues','poverty'] |2|
            |'Religions and babies'                     |['global issues']            |1|
            |'The magic washing machine'                |['economics']                |1|


  Scenario: Find how many talks were given per event
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Event)<-[:InEvent]-(t:Talk)
            RETURN n.name as Event, COUNT(t) AS TalksCount
            ORDER BY TalksCount DESC, Event
            LIMIT 20;
            """
    Then the result should be:
            |     Event              |TalksCount|
            |   'TED2006'            |    6     |
            |   'TED2009'            |    6     |
            |   'TED2013'            |    5     |
            |   'TED2007'            |    4     |
            |   'TED2014'            |    4     |
            |   'TED@State'          |    4     |
            |   'TED2012'            |    3     |
            |   'TEDGlobal 2005'     |    3     |
            |   'TEDGlobal 2010'     |    3     |
            |   'TEDGlobal 2011'     |    3     |
            |   'TEDGlobal 2012'     |    3     |
            |   'TEDxSummit'         |    3     |
            |   'EG 2007'            |    2     |
            |   'EG 2008'            |    2     |
            |   'TED Studio'         |    2     |
            |   'TED Talks Education'|    2     |
            |   'TED2003'            |    2     |
            |   'TED2004'            |    2     |
            |   'TED2008'            |    2     |
            |   'TED2010'            |    2     |


  Scenario: Find the most popular tags in specific event
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Event {name:"TED2006"})<-[:InEvent]-(t:Talk)-[:HasTag]->(tag:Tag)
            RETURN tag.name as Tag, COUNT(t) AS TalksCount
            ORDER BY TalksCount DESC, Tag
            LIMIT 20;
            """
    Then the result should be:
            |      Tag          |  TalksCount  |
            | 'global issues'   |      3       |
            | 'technology'      |      3       |
            | 'children'        |      2       |
            | 'culture'         |      2       |
            | 'education'       |      2       |
            | 'entertainment'   |      2       |
            | 'performance'     |      2       |
            | 'Africa'          |      1       |
            | 'Asia'            |      1       |
            | 'Google'          |      1       |
            | 'TED Brain Trust' |      1       |
            | 'business'        |      1       |
            | 'cities'          |      1       |
            | 'computers'       |      1       |
            | 'creativity'      |      1       |
            | 'dance'           |      1       |
            | 'demo'            |      1       |
            | 'design'          |      1       |
            | 'economics'       |      1       |
            | 'entrepreneur'    |      1       |


  Scenario: Discover which speakers participated in more than 2 events
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Speaker)-[:Gave]->(t:Talk)-[:InEvent]->(e:Event)
            WITH n, COUNT(e) AS EventsCount WHERE EventsCount > 2
            RETURN n.name as Speaker, EventsCount
            ORDER BY EventsCount DESC, Speaker;
            """
    Then the result should be:
            |      Speaker          | EventsCount |
            | 'Hans Rosling'        |      9      |
            | 'Juan Enriquez'       |      7      |
            | 'Marco Tempest'       |      6      |
            | 'Rives'               |      6      |
            | 'Bill Gates'          |      5      |
            | 'Clay Shirky'         |      5      |
            | 'Dan Ariely'          |      5      |
            | 'Jacqueline Novogratz'|      5      |
            | 'Julian Treasure'     |      5      |
            | 'Nicholas Negroponte' |      5      |
            | 'Barry Schwartz'      |      4      |
            | 'Chris Anderson'      |      4      |
            | 'David Pogue'         |      4      |
            | 'Jonathan Haidt'      |      4      |
            | 'Ken Robinson'        |      4      |
            | 'Kevin Kelly'         |      4      |
            | 'Lawrence Lessig'     |      4      |
            | 'Steven Johnson'      |      4      |
            | 'Stewart Brand'       |      4      |
            | 'Michael Green'       |      3      |


  Scenario: For each speaker search for other speakers that participated in same events
    Given graph "ted_graph"
    When executing query:
            """
            MATCH (n:Speaker)-[:Gave]->()-[:InEvent]->(e:Event)<-[:InEvent]-()<-[:Gave]-(m:Speaker)
            WHERE n.name != m.name
            WITH DISTINCT n, m ORDER BY m.name
            RETURN n.name AS Speaker, COLLECT(m.name) AS Others
            ORDER BY Speaker;
            """
    Then the result should be:
      |           Speaker           |       Others        |
      |       'Barry Schwartz'      |['Bill Gates','Clay Shirky','Dan Ariely','Hans Rosling','Jacqueline Novogratz','Juan Enriquez','Lawrence Lessig','Marco Tempest','Nicholas Negroponte']|
      |       'Bill Gates'          |['Barry Schwartz','Dan Ariely','Hans Rosling','Jacqueline Novogratz','Juan Enriquez','Ken Robinson']|
      |       'Chris Anderson'      |['Julian Treasure','Steven Johnson','Stewart Brand']|
      |       'Clay Shirky'         |['Barry Schwartz','Hans Rosling','Jacqueline Novogratz','Jonathan Haidt','Julian Treasure','Marco Tempest','Stewart Brand']|
      |       'Dan Ariely'          |['Barry Schwartz','Bill Gates','David Pogue','Hans Rosling','Jacqueline Novogratz','Juan Enriquez']|
      |       'David Pogue'         |['Dan Ariely','Hans Rosling','Juan Enriquez','Ken Robinson','Lawrence Lessig','Michael Green','Nicholas Negroponte','Rives','Stewart Brand']|
      |       'Hans Rosling'        |['Barry Schwartz','Bill Gates','Clay Shirky','Dan Ariely','David Pogue','Jacqueline Novogratz','Juan Enriquez','Ken Robinson','Lawrence Lessig','Nicholas Negroponte','Rives','Stewart Brand']|
      |       'Jacqueline Novogratz'|['Barry Schwartz','Bill Gates','Clay Shirky','Dan Ariely','Hans Rosling','Juan Enriquez','Stewart Brand']|
      |       'Jonathan Haidt'      |['Clay Shirky','Marco Tempest','Rives']|
      |       'Juan Enriquez'       |['Barry Schwartz','Bill Gates','Dan Ariely','David Pogue','Hans Rosling','Jacqueline Novogratz','Kevin Kelly','Lawrence Lessig','Michael Green','Rives','Steven Johnson','Stewart Brand']|
      |       'Julian Treasure'     |['Chris Anderson','Clay Shirky','Marco Tempest','Steven Johnson']|
      |       'Ken Robinson'        |['Bill Gates','David Pogue','Hans Rosling','Nicholas Negroponte','Rives','Stewart Brand']|
      |       'Kevin Kelly'         |['Juan Enriquez','Nicholas Negroponte']|
      |       'Lawrence Lessig'     |['Barry Schwartz','David Pogue','Hans Rosling','Juan Enriquez','Marco Tempest','Michael Green','Nicholas Negroponte','Rives','Stewart Brand']|
      |       'Marco Tempest'       |['Barry Schwartz','Clay Shirky','Jonathan Haidt','Julian Treasure','Lawrence Lessig','Nicholas Negroponte']|
      |       'Michael Green'       |['David Pogue','Juan Enriquez','Lawrence Lessig','Stewart Brand']|
      |       'Nicholas Negroponte' |['Barry Schwartz','David Pogue','Hans Rosling','Ken Robinson','Kevin Kelly','Lawrence Lessig','Marco Tempest','Rives','Stewart Brand']|
      |       'Rives'               |['David Pogue','Hans Rosling','Jonathan Haidt','Juan Enriquez','Ken Robinson','Lawrence Lessig','Nicholas Negroponte','Steven Johnson','Stewart Brand']|
      |       'Steven Johnson'      |['Chris Anderson','Juan Enriquez','Julian Treasure','Rives']|
      |       'Stewart Brand'       |['Chris Anderson','Clay Shirky','David Pogue','Hans Rosling','Jacqueline Novogratz','Juan Enriquez','Ken Robinson','Lawrence Lessig','Michael Green','Nicholas Negroponte','Rives']|

