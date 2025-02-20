UNWIND range(1, 10) AS prop1
UNWIND range(1, 10) AS prop2
CREATE (:Node {prop1: prop1, prop2: prop2});
