// Initialize graph configuration with custom parameters
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

