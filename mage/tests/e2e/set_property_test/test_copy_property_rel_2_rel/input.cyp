CREATE (:Node {id: 1, prop: 'prop1'})-[:TYPE {prop: 'prop3'}]->(:Node {id: 2, prop: 'prop2'});
CREATE (:Node {id: 3, prop: 'prop3'})-[:TYPE {prop: 'prop5'}]->(:Node {id: 4, prop: 'prop4'});
MATCH (a {id: 1})-[r1]->(b {id: 2}) MATCH (c {id: 3})-[r2]->(d {id: 4}) CALL set_property.copyPropertyRel2Rel(r1, ['prop'], r2, ['prop']) YIELD result RETURN result;
