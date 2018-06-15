## Examples

This chapter shows you how to use Memgraph on real-world data and how to get
interesting and useful information out of it.

### TED Talks Example

[TED](https://www.ted.com/) is a nonprofit organization devoted to spreading
ideas, usually in the form of short, powerful talks.
Today, TED talks are influential videos from expert speakers on almost all
topics &mdash; from science to business to global issues.
Here we present a small dataset which consists of 97 talks. We'll show you how
to model this data as a graph and demonstrate a few example queries.

#### Data Model
Each TED talk has a main speaker, so we
identify two types of nodes &mdash; `Talk` and `Speaker`. Also, we will add
an edge of type `Gave` pointing to a `Talk` from its main `Speaker`.
Each speaker has a name so we can add property `name` to `Speaker` node.
Likewise, we'll add properties `name`, `title` and `description` to node
`Talk`. Furthermore, each talk is given in a specific TED event, so we can
create node `Event` with property `name` and relationship `InEvent` between
talk and event.

Talks are tagged with keywords to facilitate searching, hence we
add node `Tag` with property `name` and relationship `HasTag` between talk and
tag. Moreover, users give ratings to each talk by selecting up to three
predefined string values. Therefore we add node `Rating` with these values as
property `name` and relationship`HasRating` with property `user_count` between
talk and rating nodes.

#### Example Queries

We have prepared a database snapshot for this example, so you can easily import
it when starting Memgraph using the `--durability-directory` option.

```
/usr/lib/memgraph/memgraph --durability-directory /usr/share/memgraph/examples/TEDTalk \
  --durability-enabled=false --snapshot-on-exit=false
```

When using Memgraph installed from DEB or RPM package, you may need to stop
the currently running Memgraph server before you can import the example. Use
the following command:

```
systemctl stop memgraph
```

When using Docker, you can import the example with the following command:

```
docker run -p 7687:7687 \
  -v mg_lib:/var/lib/memgraph -v mg_log:/var/log/memgraph -v mg_etc:/etc/memgraph \
  memgraph --durability-directory /usr/share/memgraph/examples/TEDTalk \
  --durability-enabled=false --snapshot-on-exit=false
```

Now you're ready to try out some of the following queries.

NOTE: If you modify the dataset, the changes will stay only during this run of
Memgraph.

1) Find all talks given by specific speaker:
```
MATCH (n:Speaker {name: "Hans Rosling"})-[:Gave]->(m:Talk)
RETURN m.title;
```


2) Find the top 20 speakers with most talks given:

```
MATCH (n:Speaker)-[:Gave]->(m)
RETURN n.name, COUNT(m) as TalksGiven
ORDER BY TalksGiven DESC LIMIT 20;
```

3) Find talks related by tag to specific talk and count them:
```
MATCH (n:Talk {name: "Michael Green: Why we should build wooden skyscrapers"})-[:HasTag]->(t:Tag)<-[:HasTag]-(m:Talk)
WITH * ORDER BY m.name
RETURN t.name, COLLECT(m.name), COUNT(m) AS TalksCount
ORDER BY TalksCount DESC;
```

4) Find 20 most frequently used tags:
```
MATCH (t:Tag)<-[:HasTag]-(n:Talk)
RETURN t.name as Tag, COUNT(n) AS TalksCount
ORDER BY TalksCount DESC, Tag LIMIT 20;
```

5) Find 20 talks most rated as "Funny". If you want to query by other ratings,
possible values are: Obnoxious, Jaw-dropping, OK, Persuasive, Beautiful,
Confusing, Longwinded, Unconvincing, Fascinating, Ingenious, Courageous, Funny,
Informative and Inspiring.
```
MATCH (r:Rating{name:"Funny"})<-[e:HasRating]-(m:Talk)
RETURN m.name, e.user_count ORDER BY e.user_count DESC LIMIT 20;
```

6) Find inspiring talks and their speakers from the field of technology:
```
MATCH (n:Talk)-[:HasTag]->(m:Tag {name: "technology"})
MATCH (n)-[r:HasRating]->(p:Rating {name: "Inspiring"})
MATCH (n)<-[:Gave]-(s:Speaker)
WHERE r.user_count > 1000
RETURN n.title, s.name, r.user_count ORDER BY r.user_count DESC;
```

7) Now let's see one real-world example &mdash; how to make a real-time
recommendation. If you've just watched a talk from a certain
speaker (e.g. Hans Rosling) you might be interested in finding more talks from
the same speaker on a similar topic:

```
MATCH (n:Speaker {name: "Hans Rosling"})-[:Gave]->(m:Talk)
MATCH (t:Talk {title: "New insights on poverty"})-[:HasTag]->(tag:Tag)<-[:HasTag]-(m)
WITH * ORDER BY tag.name
RETURN m.title as Title, COLLECT(tag.name), COUNT(tag) as TagCount
ORDER BY TagCount DESC, Title;
```

The following few queries are focused on extracting information about
TED events.

8) Find how many talks were given per event:
```
MATCH (n:Event)<-[:InEvent]-(t:Talk)
RETURN n.name as Event, COUNT(t) AS TalksCount
ORDER BY TalksCount DESC, Event
LIMIT 20;
```

9) Find the most popular tags in the specific event:
```
MATCH (n:Event {name:"TED2006"})<-[:InEvent]-(t:Talk)-[:HasTag]->(tag:Tag)
RETURN tag.name as Tag, COUNT(t) AS TalksCount
ORDER BY TalksCount DESC, Tag
LIMIT 20;
```

10) Discover which speakers participated in more than 2 events:
```
MATCH (n:Speaker)-[:Gave]->(t:Talk)-[:InEvent]->(e:Event)
WITH n, COUNT(e) AS EventsCount WHERE EventsCount > 2
RETURN n.name as Speaker, EventsCount
ORDER BY EventsCount DESC, Speaker;
```

11) For each speaker search for other speakers that participated in same
events:
```
MATCH (n:Speaker)-[:Gave]->()-[:InEvent]->(e:Event)<-[:InEvent]-()<-[:Gave]-(m:Speaker)
WHERE n.name != m.name
WITH DISTINCT n, m ORDER BY m.name
RETURN n.name AS Speaker, COLLECT(m.name) AS Others
ORDER BY Speaker;
```

### Football Example

[Football](https://en.wikipedia.org/wiki/Association_football)
(soccer for the heathens) is a team sport played between two teams of eleven
players with a spherical ball. The game is played on a rectangular pitch with
a goal at each and. The object of the game is to score by moving the ball
beyond the goal line into the opposing goal. The game is played by more than
250 million players in over 200 countries, making it the world's most
popular sport.

In this example, we will present a graph model of a reasonably sized dataset
of football matches across world's most popular leagues.

#### Data Model

In essence, we are trying to model a set of football matches. All information
about a single match is going to be contained in three nodes and two edges.
Two of the nodes will represent the teams that have played the match, while the
third node will represent the game itself. Both edges are directed from the
team nodes to the game node and are labeled as `:Played`.

Let us consider a real life example of this model&mdash;Arsene Wenger's 1000th.
game in charge of Arsenal. This was a regular fixture of a 2013/2014
English Premier League, yet it was written in the stars that this historic
moment would be a big London derby against Chelsea on Stanford Bridge. The
sketch below shows how this game is being modeled in our database.

```
+---------------+                                            +-----------------------------+
|n: Team        |                                            |w: Game                      |
|               |-[:Played {side: "home", outcome: "won"}]-->|                             |
|name: "Chelsea"|                                            |HT_home_score: 4             |
+---------------+                                            |HT_away_score: 0             |
                                                             |HT_result: "H"               |
                                                             |FT_home_score: 6             |
                                                             |FT_away_score: 0             |
                                                             |FT_result: "H"               |
+---------------+                                            |date: "2014-03-22"           |
|m: Team        |                                            |league: "ENG-Premier League" |
|               |-[:Played {side: "away", outcome: "lost"}]->|season: 2013                 |
|name: "Arsenal"|                                            |referee: "Andre Marriner"    |
+---------------+                                            +-----------------------------+
```

#### Example Queries

We have prepared a database snapshot for this example, so you can easily import
it when starting Memgraph using the `--durability-directory` option.

```
/usr/lib/memgraph/memgraph --durability-directory /usr/share/memgraph/examples/football \
  --durability-enabled=false --snapshot-on-exit=false
```

When using Docker, you can import the example with the following command:

```
docker run -p 7687:7687 \
  -v mg_lib:/var/lib/memgraph -v mg_log:/var/log/memgraph -v mg_etc:/etc/memgraph \
  memgraph --durability-directory /usr/share/memgraph/examples/football \
  --durability-enabled=false --snapshot-on-exit=false
```

Now you're ready to try out some of the following queries.

NOTE: If you modify the dataset, the changes will stay only during this run of
Memgraph.

1) You might wonder, what leagues are supported?

```
MATCH (n:Game)
RETURN DISTINCT n.league AS League
ORDER BY League;
```

2) We have stored a certain number of seasons for each league. What is the
oldest/newest season we have included?

```
MATCH (n:Game)
RETURN DISTINCT n.league AS League, MIN(n.season) AS Oldest, MAX(n.season) AS Newest
ORDER BY League;
```

3) You have already seen one game between Chelsea and Arsenal, let's list all of
them in chronological order.

```
MATCH (n:Team {name: "Chelsea"})-[e:Played]->(w:Game)<-[f:Played]-(m:Team {name: "Arsenal"})
RETURN w.date AS Date, e.side AS Chelsea, f.side AS Arsenal,
       w.FT_home_score AS home_score, w.FT_away_score AS away_score
ORDER BY Date;
```

4) How about filtering games in which Chelsea won?

```
MATCH (n:Team {name: "Chelsea"})-[e:Played {outcome: "won"}]->
      (w:Game)<-[f:Played]-(m:Team {name: "Arsenal"})
RETURN w.date AS Date, e.side AS Chelsea, f.side AS Arsenal,
       w.FT_home_score AS home_score, w.FT_away_score AS away_score
ORDER BY Date;
```

5) Home field advantage is a thing in football. Let's list the number of home
defeats for each Premier League team in the 2016/2017 season.

```
MATCH (n:Team)-[:Played {side: "home", outcome: "lost"}]->
      (w:Game {league: "ENG-Premier League", season: 2016})
RETURN n.name AS Team, count(w) AS home_defeats
ORDER BY home_defeats, Team;
```

6) At the end of the season the team with the most points wins the league. For
each victory, a team is awarded 3 points and for each draw it is awarded
1 point. Let's find out how many points did reigning champions (Chelsea) have
at the end of 2016/2017 season.

```
MATCH (n:Team {name: "Chelsea"})-[:Played {outcome: "drew"}]->(w:Game {season: 2016})
WITH n, COUNT(w) AS draw_points
MATCH (n)-[:Played {outcome: "won"}]->(w:Game {season: 2016})
RETURN draw_points + 3 * COUNT(w) AS total_points;
```

7) In fact, why not retrieve the whole table?

```
MATCH (n)-[:Played {outcome: "drew"}]->(w:Game {league: "ENG-Premier League", season: 2016})
WITH n, COUNT(w) AS draw_points
MATCH (n)-[:Played {outcome: "won"}]->(w:Game {league: "ENG-Premier League", season: 2016})
RETURN n.name AS Team, draw_points + 3 * COUNT(w) AS total_points
ORDER BY total_points DESC;
```

8) People have always debated which of the major leagues is the most exciting.
One basic metric is the average number of goals per game. Let's see the results
at the end of the 2016/2017 season. WARNING: This might shock you.

```
MATCH (w:Game {season: 2016})
RETURN w.league, AVG(w.FT_home_score) + AVG(w.FT_away_score) AS avg_goals_per_game
ORDER BY avg_goals_per_game DESC;
```

9) Another metric might be the number of comebacks&mdash;games where one side
was winning at half time but were overthrown by the other side by the end
of the match. Let's count such occurrences during all supported seasons across
all supported leagues.

```
MATCH (g:Game) WHERE
(g.HT_result = "H" AND g.FT_result = "A") OR
(g.HT_result = "A" AND g.FT_result = "H")
RETURN g.league AS League, count(g) AS Comebacks
ORDER BY Comebacks DESC;
```

10) Exciting leagues also tend to be very unpredictable. On that note, let's list
all triplets of teams where, during the course of one season, team A won against
team B, team B won against team C and team C won against team A.

```
MATCH (a)-[:Played {outcome: "won"}]->(p:Game {league: "ENG-Premier League", season: 2016})<--
      (b)-[:Played {outcome: "won"}]->(q:Game {league: "ENG-Premier League", season: 2016})<--
      (c)-[:Played {outcome: "won"}]->(r:Game {league: "ENG-Premier League", season: 2016})<--(a)
WHERE p.date < q.date AND q.date < r.date
RETURN a.name AS Team1, b.name AS Team2, c.name AS Team3;
```

### European road network example

In this section we will show how to use some of Memgraph's built-in graph
algorithms. More specifically, we will show how to use breadth-first search
graph traversal algorithm, and Dijkstra's algorithm for finding weighted
shortest paths between nodes in the graph.

#### Data model

One of the most common applications of graph traversal algorithms is driving
route computation, so we will use European road network graph as an example.
The graph consists of 999 major European cities from 39 countries in total.
Each city is connected to the country it belongs to via an edge of type `:In_`.
There are edges of type `:Road` connecting cities less than 500 kilometers
apart. Distance between cities is specified in the `length` property of the
edge.


#### Example queries

We have prepared a database snapshot for this example, so you can easily import
it when starting Memgraph using the `--durability-directory` option.

```bash
/usr/lib/memgraph/memgraph --durability-directory /usr/share/memgraph/examples/Europe \
  --durability-enabled=false --snapshot-on-exit=false
```

When using Docker, you can import the example with the following command:

```bash
docker run -p 7687:7687 \
  -v mg_lib:/var/lib/memgraph -v mg_log:/var/log/memgraph -v mg_etc:/etc/memgraph \
  memgraph --durability-directory /usr/share/memgraph/examples/Europe \
  --durability-enabled=false --snapshot-on-exit=false
```

Now you're ready to try out some of the following queries.

NOTE: If you modify the dataset, the changes will stay only during this run of
Memgraph.

Let's start off with a few simple queries.

1) Let's list all of the countries in our road network.

```opencypher
MATCH (c:Country) RETURN c.name ORDER BY c.name;
```

2) Which Croatian cities are in our road network?

```opencypher
MATCH (c:City)-[:In_]->(:Country {name: "Croatia"})
RETURN c.name ORDER BY c.name;
```

3) Which cities in our road network are less than 200 km away from Zagreb?

```opencypher
MATCH (:City {name: "Zagreb"})-[r:Road]->(c:City)
WHERE r.length < 200
RETURN c.name ORDER BY c.name;
```

Now let's try some queries using Memgraph's graph traversal capabilities.

4) Say you want to drive from Zagreb to Paris. You might wonder, what is the
least number of cities you have to visit if you don't want to drive more than
500 kilometers between stops. Since the edges in our road network don't connect
cities that are more than 500 km apart, this is a great use case for the
breadth-first search (BFS) algorithm.

```opencypher
MATCH p = (:City {name: "Zagreb"})
          -[:Road * bfs]->
          (:City {name: "Paris"}) 
RETURN nodes(p);
```

5) What if we want to bike to Paris instead of driving? It is unreasonable (and
dangerous!) to bike 500 km per day. Let's limit ourselves to biking no more
than 200 km in one go.

```opencypher
MATCH p = (:City {name: "Zagreb"})
          -[:Road * bfs (e, v | e.length <= 200)]->
          (:City {name: "Paris"})
RETURN nodes(p);
```

"What is this special syntax?", you might wonder.

`(e, v | e.length <= 200)` is called a *filter lambda*. It's a function that
takes an edge symbol `e` and a vertex symbol `v` and decides whether this edge
and vertex pair should be considered valid in breadth-first expansion by
returning true or false (or nil). In the above example, lambda is returning
true if edge length is not greater than 200, because we don't want to bike more
than 200 km in one go.

6) Let's say we also don't want to visit Vienna on our way to Paris, because we
have a lot of friends there and visiting all of them would take up a lot of our
time. We just have to update our filter lambda.

```opencypher
MATCH p = (:City {name: "Zagreb"})
          -[:Road * bfs (e, v | e.length <= 200 AND v.name != "Vienna")]->
          (:City {name: "Paris"})
RETURN nodes(p);
```

As you can see, without the additional restriction we could visit 11 cities. If
we want to avoid Vienna, we must visit at least 12 cities.

7) Instead of counting the cities visited, we might want to find the shortest
paths in terms of distance travelled. This is a textbook application of
Dijkstra's algorithm. The following query will return the list of cities on the
shortest path from Zagreb to Paris along with the total length of the path.

```opencypher
MATCH p = (:City {name: "Zagreb"})
          -[:Road * wShortest (e, v | e.length) total_weight]->
          (:City {name: "Paris"})
RETURN nodes(p) as cities, total_weight;
```

As you can see, the syntax is quite similar to breadth-first search syntax.
Instead of a filter lambda, we need to provide a *weight lambda* and the *total
weight symbol*. Given an edge and vertex pair, weight lambda must return the
cost of expanding to the given vertex using the given edge. The path returned
will have the smallest possible sum of costs and it will be stored in the total
weight symbol. A limitation of Dijkstra's algorithm is that the cost must be
non-negative.

8) We can also combine weight and filter lambdas in the shortest-path query.
Let's say we're interested in the shortest path that doesn't require travelling
more that 200 km in one go for our bike route.

```opencypher
MATCH p = (:City {name: "Zagreb"})
      -[:Road * wShortest (e, v | e.length) total_weight (e, v | e.length <= 200)]->
      (:City {name: "Paris"})
RETURN nodes(p) as cities, total_weight;
```

9) Let's try and find 10 cities that are furthest away from Zagreb.

```opencypher
MATCH (:City {name: "Zagreb"})
      -[:Road * wShortest (e, v | e.length) total_weight]->
      (c:City)
RETURN c, total_weight
ORDER BY total_weight DESC LIMIT 10;
```

It is not surprising to see that they are all in Siberia.


To learn more about these algorithms, we suggest you check out their Wikipedia
pages:
* [Breadth-first search](https://en.wikipedia.org/wiki/Breadth-first_search)
* [Dijkstra's algorithm](https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm)


Now you're ready to explore the world of graph databases with Memgraph
by yourself and try it on many more examples and datasets.

### Graph Gists Examples

A nice looking set of small graph examples can be found
[here](https://neo4j.com/graphgists/).  You can take any use-case and try to
execute the queries against Memgraph. To clear the database between trying out
examples, execute the query:

```
MATCH (n) DETACH DELETE n;
```
