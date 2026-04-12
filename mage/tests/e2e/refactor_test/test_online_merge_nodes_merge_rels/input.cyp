queries:
    - |-
        CREATE (p1:PERSON {NAME: "John Doe"})-[:FRIENDS_WITH {LEVEL: "besties"}]->(:PERSON {NAME: "Bob Smith"}) 
        CREATE (p2:PERSON {EMAIL: "John@Doe.com"}) 
    - |-
        MATCH (n) RETURN n;    
