Feature: Text edge search related features

    Scenario: Create text edge index
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX exampleEdgeIndex ON :DOCUMENT_RELATION
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                                      | label               | property | count |
            | 'edge-type_text (name: exampleEdgeIndex)'       | 'DOCUMENT_RELATION' | []       | 0     |

    Scenario: Drop text edge index
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX exampleEdgeIndex ON :DOCUMENT_RELATION
            """
        And having executed
            """
            DROP TEXT INDEX exampleEdgeIndex
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type | label | property | count |

    Scenario: Search edge by property
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document {name: 'Rules2024'})-[:RELATES_TO {title: 'Rules2024', version: 1}]->(d2:Document {name: 'Other'})
            CREATE (d3:Document {name: 'Rules2024'})-[:RELATES_TO {title: 'Rules2024', version: 2}]->(d4:Document {name: 'Other'})
            CREATE (d5:Document {name: 'Other'})-[:RELATES_TO {title: 'Other', version: 2}]->(d6:Document {name: 'Other'})
            """
        When executing query:
            """
            CALL text_search.search_edges('complianceEdges', 'data.title:Rules2024') YIELD edge
            RETURN edge AS edge
            ORDER BY edge.version ASC
            """
        Then the result should be:
            | edge                                           |
            | [:RELATES_TO {title: 'Rules2024', version: 1}] |
            | [:RELATES_TO {title: 'Rules2024', version: 2}] |

    Scenario: Search all edge properties
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {title: 'Rules2024', fulltext: 'text words', version: 1}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {title: 'Rules2024', fulltext: 'other words', version: 2}]->(d4:Document)
            CREATE (d5:Document)-[:RELATES_TO {title: 'Other', fulltext: 'Rules2024 here', version: 3}]->(d6:Document)
            """
        When executing query:
            """
            CALL text_search.search_all_edges('complianceEdges', 'Rules2024') YIELD edge
            RETURN edge AS edge
            ORDER BY edge.version ASC
            """
        Then the result should be:
            | edge                                                                              |
            | [:RELATES_TO {title: 'Rules2024', fulltext: 'text words', version: 1}]            |
            | [:RELATES_TO {title: 'Rules2024', fulltext: 'other words', version: 2}]           |
            | [:RELATES_TO {title: 'Other', fulltext: 'Rules2024 here', version: 3}]            |

    Scenario: Regex search on edges
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {fulltext: 'words and things'}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {fulltext: 'more words'}]->(d4:Document)
            """
        When executing query:
            """
            CALL text_search.regex_search_edges('complianceEdges', 'wor.*s') YIELD edge
            RETURN edge AS edge
            ORDER BY edge.fulltext ASC
            """
        Then the result should be:
            | edge                                           |
            | [:RELATES_TO {fulltext: 'more words'}]         |
            | [:RELATES_TO {fulltext: 'words and things'}]   |

    Scenario: Search edge aggregate
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {title: 'Rules2024', version: 1}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {title: 'Rules2024', version: 2}]->(d4:Document)
            """
        When executing query:
            """
            CALL text_search.aggregate_edges('complianceEdges', 'data.title:Rules2024', '{"count":{"value_count":{"field":"data.version"}}}') YIELD aggregation
            RETURN aggregation
            """
        Then the result should be:
            | aggregation              |
            | '{"count":{"value":2.0}}'|

    Scenario: Search query with boolean logic on edges
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {title: 'Rules2023', fulltext: 'nothing'}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {title: 'Rules2024', fulltext: 'words', version: 2}]->(d4:Document)
            """
        When executing query:
            """
            CALL text_search.search_edges('complianceEdges', '(data.title:Rules2023 OR data.title:Rules2024) AND data.fulltext:words') YIELD edge
            RETURN edge.title AS title, edge.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title       | version |
            | 'Rules2024' | 2       |

    Scenario: Add edge and search
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {title: 'Rules2024', version: 1}]->(d2:Document)
            """
        When executing query:
            """
            CALL text_search.search_edges('complianceEdges', 'data.title:Rules2024') YIELD edge
            RETURN edge.title AS title, edge.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title       | version |
            | 'Rules2024' | 1       |

    Scenario: Delete indexed edge
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[r:RELATES_TO {title: 'Rules2024', version: 2}]->(d2:Document)
            """
        And having executed
            """
            MATCH ()-[r:RELATES_TO {title: 'Rules2024', version: 2}]-() DELETE r
            """
        When executing query:
            """
            CALL text_search.search_edges('complianceEdges', 'data.title:Rules2024') YIELD edge
            RETURN edge.title AS title, edge.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title | version |

        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                                  | label        | property | count |
            | 'edge-type_text (name: complianceEdges)'    | 'RELATES_TO' | []       | 0     |

    Scenario: Delete indexed edge via detach delete node
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {title: 'Rules2024', version: 1}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {title: 'Rules2024', version: 2}]->(d4:Document)
            """
        And having executed
            """
            MATCH (n) DETACH DELETE n
            """
        When executing query:
            """
            CALL text_search.search_edges('complianceEdges', 'data.title:Rules2024') YIELD edge
            RETURN edge.title AS title, edge.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title | version |

        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                                  | label        | property | count |
            | 'edge-type_text (name: complianceEdges)'    | 'RELATES_TO' | []       | 0     |

    Scenario: Update property of indexed edge
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX complianceEdges ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[r:RELATES_TO {title: 'Rules2024', version: 1}]->(d2:Document)
            """
        And having executed
            """
            MATCH ()-[r:RELATES_TO {version:1}]-() SET r.title = 'Rules2030'
            """
        When executing query:
            """
            CALL text_search.search_edges('complianceEdges', 'data.title:Rules2030') YIELD edge
            RETURN edge.title AS title, edge.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title       | version |
            | 'Rules2030' | 1       |

    Scenario: Case-insensitive regex search with lowercase query on edges
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX testEdgeIndex ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {title: 'TITLE ONE'}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {title: 'title two'}]->(d4:Document)
            CREATE (d5:Document)-[:RELATES_TO {title: 'Title Three'}]->(d6:Document)
            """
        When executing query:
            """
            CALL text_search.regex_search_edges('testEdgeIndex', 't.*') YIELD edge
            RETURN edge.title AS title
            ORDER BY title ASC
            """
        Then the result should be:
            | title         |
            | 'TITLE ONE'   |
            | 'Title Three' |
            | 'title two'   |

    Scenario: Case-insensitive regex search with uppercase query on edges
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX testEdgeIndex ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {content: 'Testing REGEX patterns'}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {content: 'regex TESTING patterns'}]->(d4:Document)
            CREATE (d5:Document)-[:RELATES_TO {content: 'No match here'}]->(d6:Document)
            """
        When executing query:
            """
            CALL text_search.regex_search_edges('testEdgeIndex', 'T.*G') YIELD edge
            RETURN edge.content AS content
            ORDER BY content ASC
            """
        Then the result should be:
            | content                    |
            | 'Testing REGEX patterns'   |
            | 'regex TESTING patterns'   |

    Scenario: Search on nonexistent text edge index raises error
        Given an empty graph
        When executing query:
            """
            CALL text_search.search_edges('noSuchEdgeIndex', 'data.fulltext:words') YIELD edge
            RETURN edge.title AS title, edge.version AS version
            ORDER BY version ASC, title ASC
            """
        Then an error should be raised

    Scenario: Create text edge index with specific properties
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX titleContentEdgeIndex ON :RELATES_TO(title, content)
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                                         | label        | property            | count |
            | 'edge-type_text (name: titleContentEdgeIndex)'     | 'RELATES_TO' | ['title','content'] | 0     |

    Scenario: Search in property-specific edge index with both properties
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX titleContentEdgeIndex ON :RELATES_TO(title, content)
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {title: 'Manual2024', content: 'database operations guide'}]->(d2:Document)
            """
        When executing query:
            """
            CALL text_search.search_edges('titleContentEdgeIndex', 'data.title:Manual2024') YIELD edge
            RETURN edge.title AS title, edge.content AS content
            ORDER BY title ASC
            """
        Then the result should be:
            | title        | content                    |
            | 'Manual2024' | 'database operations guide'|

    Scenario: Search in property-specific edge index with only one required property
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX titleContentEdgeIndex ON :RELATES_TO(title, content)
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {title: 'OnlyTitle', version: 1}]->(d2:Document)
            """
        When executing query:
            """
            CALL text_search.search_edges('titleContentEdgeIndex', 'data.title:OnlyTitle') YIELD edge
            RETURN edge.title AS title, edge.version AS version
            ORDER BY version ASC
            """
        Then the result should be:
            | title       | version |
            | 'OnlyTitle' | 1       |

    Scenario: Verify edges without indexed properties are not searchable
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX titleContentEdgeIndex ON :RELATES_TO(summary)
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {description: 'not indexed property', summary: 'also not indexed'}]->(d2:Document)
            """
        When executing query:
            """
            CALL text_search.search_edges('titleContentEdgeIndex', 'data.description:indexed') YIELD edge
            RETURN edge
            """
        Then the result should be empty

    Scenario: Delete property from indexed edge
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX testEdgeIndex ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[r:RELATES_TO {title: 'Test Title', content: 'Test content'}]->(d2:Document)
            """
        When executing query:
            """
            CALL text_search.search_edges('testEdgeIndex', 'data.title:Test') YIELD edge
            RETURN count(edge) AS count
            """
        Then the result should be:
            | count |
            | 1     |

        And having executed
            """
            MATCH ()-[r:RELATES_TO {title: 'Test Title'}]-() SET r.title = null
            """
        When executing query:
            """
            CALL text_search.search_edges('testEdgeIndex', 'data.title:Test') YIELD edge
            RETURN count(edge) AS count
            """
        Then the result should be:
            | count |
            | 0     |

        When executing query:
            """
            CALL text_search.search_edges('testEdgeIndex', 'data.content:Test') YIELD edge
            RETURN count(edge) AS count
            """
        Then the result should be:
            | count |
            | 1     |

    Scenario: Create edge index on existing edges
        Given an empty graph
        And having executed
            """
            CREATE (a1:Article)-[:REFERENCES {title: 'Database Systems', content: 'Introduction to graph databases and their applications'}]->(a2:Article)
            CREATE (a3:Article)-[:REFERENCES {title: 'Query Languages', content: 'Cypher query language for graph database operations'}]->(a4:Article)
            """

        And having executed
            """
            CREATE TEXT EDGE INDEX reference_index ON :REFERENCES
            """

        When executing query:
            """
            CALL text_search.search_edges('reference_index', 'data.content:graph') YIELD edge
            RETURN edge.title AS title ORDER BY title
            """
        Then the result should be:
            | title              |
            | 'Database Systems' |
            | 'Query Languages'  |

    Scenario: Test limit parameter on edge search
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX limitTestEdgeIndex ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {type: 'reference', content: 'test data'}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {type: 'reference', content: 'test data'}]->(d4:Document)
            CREATE (d5:Document)-[:RELATES_TO {type: 'reference', content: 'test data'}]->(d6:Document)
            CREATE (d7:Document)-[:RELATES_TO {type: 'reference', content: 'test data'}]->(d8:Document)
            """
        When executing query:
            """
            CALL text_search.search_edges('limitTestEdgeIndex', 'data.content:test', 2) YIELD edge
            RETURN count(edge) AS count
            """
        Then the result should be:
            | count |
            | 2     |

    Scenario: Test search returns relevance score for edges
        Given an empty graph
        And having executed
            """
            CREATE TEXT EDGE INDEX scoreTestEdgeIndex ON :RELATES_TO
            """
        And having executed
            """
            CREATE (d1:Document)-[:RELATES_TO {type: 'primary', description: 'critical information'}]->(d2:Document)
            CREATE (d3:Document)-[:RELATES_TO {type: 'secondary', description: 'minor details'}]->(d4:Document)
            """
        When executing query:
            """
            CALL text_search.search_edges('scoreTestEdgeIndex', 'data.description:critical') YIELD edge, score
            RETURN edge.type AS type, round(score) AS score
            """
        Then the result should be:
            | type      | score |
            | 'primary' | 1.0   |
