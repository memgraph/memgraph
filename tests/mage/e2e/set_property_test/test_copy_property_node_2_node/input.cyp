CREATE (:Node {id: 1, prop: 'prop1'});
CREATE (:Node {id: 2, prop: 'prop2'});
MATCH (n {id: 1}), (m {id: 2}) CALL set_property.copyPropertyNode2Node(n, ['prop'], m, ['prop']) YIELD result RETURN result;
