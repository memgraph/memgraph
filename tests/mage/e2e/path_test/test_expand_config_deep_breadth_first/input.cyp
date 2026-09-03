CREATE (:Deep {i: 0}) WITH 1 AS _ UNWIND range(1, 5001) AS i CREATE (:Deep {i: i});
