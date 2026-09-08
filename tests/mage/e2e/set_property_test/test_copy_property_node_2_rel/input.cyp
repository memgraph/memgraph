CREATE (:Node {id: 1, prop: 'prop1'})-[:TYPE {prop: 'prop3'}]->(:Node {id: 2, prop: 'prop2'});
MATCH (n {id: 1})-[r]->(m {id: 2}) CALL set_property.copyPropertyNode2Rel(n, ['prop'], r, ['prop']) YIELD result RETURN result;
