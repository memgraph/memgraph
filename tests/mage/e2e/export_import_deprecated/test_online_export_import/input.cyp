setup: |-
    CREATE (n:Person {name:'Anna', birth_datetime: localDateTime("1999-10-05T14:15:00")}), (m:Person {name:'John'}), (k:Person {name:'Kim'}) CREATE (n)-[:IS_FRIENDS_WITH {from_date: date("1995-04-28")}]->(m), (n)-[:IS_FRIENDS_WITH]->(k), (m)-[:IS_MARRIED_TO]->(k) CREATE (:User {name: "Jimmy", cars: ['car1', 'car2', 'car3'], clock: localTime("09:15:00")}) CREATE (:List {listKey: [{inner: 'Map1'}, {inner: 'Map2'}]});
queries:
    - |-
        CALL export_util.json("/var/lib/memgraph/output.json");
    - |-
        MATCH (n) DETACH DELETE n;
    - |-
        CALL import_util.json("/var/lib/memgraph/output.json");
    - |-
        MATCH (n) RETURN count(n);
    - |-
        MATCH (n) RETURN count(n);
    - |-
        MATCH (n) RETURN count(n);
    - |-
        MATCH (n) RETURN count(n);
    - |-
        MATCH (n) RETURN count(n);
    - |-
        MATCH (n) RETURN count(n);
