setup: |-
    MATCH (n) DETACH DELETE n;
    CALL mg_semantics.graphconfig_drop() YIELD * RETURN *;

queries:
    - |-
        CALL mg_semantics.graphconfig_init();
    - |-
        CALL mg_semantics.shacl_import_inline('@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix schema: <http://schema.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\nschema:PersonShape a sh:NodeShape ;\n  sh:targetClass schema:Person ;\n  sh:property [ sh:path schema:name ; sh:datatype xsd:string ; sh:minCount 1 ] .', 'Turtle');
