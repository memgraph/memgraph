// Initialize graph configuration
CALL mg_semantics.graphconfig_init({
  resourceLabel: "Resource",
  classLabel: "Class",
  subClassOfRel: "SCO",
  uriProperty: "uri",
  nameProperty: "name",
  labelProperty: "label",
  handleVocabUris: "SHORTEN",
  keepLangTag: true,
  handleMultival: "ARRAY",
  handleRDFTypes: "LABELS"
})

// Import RDF data with multilingual labels
CALL mg_semantics.rdf_import_inline('
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix ex: <http://example.org/products#> .

ex:Product a owl:Class ;
    rdfs:label "Product"@en ;
    rdfs:label "Produit"@fr .

ex:item1 a ex:Product ;
    rdfs:label "Cotton T-Shirt"@en ;
    rdfs:label "T-shirt en coton"@fr .
', "turtle")
YIELD triplesLoaded

