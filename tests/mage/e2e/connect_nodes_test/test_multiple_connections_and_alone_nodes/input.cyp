CREATE (:A {id:1})-[:CONNECTED_TO {id:1}]->(:B {id:2});
CREATE (:A {id:3})-[:CONNECTED_TO {id:2}]->(:B {id:4});
CREATE (:A {id:5})-[:CONNECTED_TO {id:3}]->(:B {id:6});
CREATE (:C), (:D);
