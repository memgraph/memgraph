queries:
    - |-
        CREATE (a:Movie {id: 0, name: "MovieName", genre: "Drama"})
        CREATE (b:Book {id: 1, name: "BookName1", genre: "Drama", propertyToCopy: "copy me"})
        CREATE (c:Book {id: 2, name: "BookName2", genre: "Romance"});
    - |-
        MATCH (n) RETURN n;
