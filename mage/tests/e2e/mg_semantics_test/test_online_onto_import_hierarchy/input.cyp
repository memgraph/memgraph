setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CALL mg_semantics.onto_import_inline('@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n@prefix owl: <http://www.w3.org/2002/07/owl#> .\n@prefix ex: <http://example.org/onto/> .\nex:Animal a owl:Class .\nex:Dog a owl:Class ; rdfs:subClassOf ex:Animal .\nex:Cat a owl:Class ; rdfs:subClassOf ex:Animal .', 'Turtle');
