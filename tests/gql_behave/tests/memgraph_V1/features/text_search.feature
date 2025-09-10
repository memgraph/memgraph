Feature: Text search related features

    Scenario: Create text index
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX exampleIndex ON :Document
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                        | label      | property | count |
            | 'label_text (name: exampleIndex)' | 'Document' | []       | 0     |

    Scenario: Drop text index
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX exampleIndex ON :Document
            """
        And having executed
            """
            DROP TEXT INDEX exampleIndex
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type | label | property | count |

    Scenario: Search by property
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX complianceDocuments ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Rules2024', version: 1})
            CREATE (:Document {title: 'Rules2024', version: 2})
            CREATE (:Document {title: 'Other', version: 2})
            """
        When executing query:
            """
            CALL text_search.search('complianceDocuments', 'data.title:Rules2024') YIELD node
            RETURN node.title AS title, node.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title       | version |
            | 'Rules2024' | 1       |
            | 'Rules2024' | 2       |

    Scenario: Search all properties
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX complianceDocuments ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Rules2024', fulltext: 'text words', version: 1})
            CREATE (:Document {title: 'Rules2024', fulltext: 'other words', version: 2})
            CREATE (:Document {title: 'Other', fulltext: 'Rules2024 here', version: 3})
            """
        When executing query:
            """
            CALL text_search.search_all('complianceDocuments', 'Rules2024') YIELD node
            RETURN node
            ORDER BY node.version ASC, node.title ASC
            """
        Then the result should be:
            | node                                                                  |
            | (:Document {title: 'Rules2024', fulltext: 'text words', version: 1})  |
            | (:Document {title: 'Rules2024', fulltext: 'other words', version: 2}) |
            | (:Document {title: 'Other', fulltext: 'Rules2024 here', version: 3})  |

    Scenario: Regex search
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX complianceDocuments ON :Document
            """
        And having executed
            """
            CREATE (:Document {fulltext: 'words and things'})
            CREATE (:Document {fulltext: 'more words'})
            """
        When executing query:
            """
            CALL text_search.regex_search('complianceDocuments', 'wor.*s') YIELD node
            RETURN node
            ORDER BY node.fulltext ASC
            """
        Then the result should be:
            | node                                       |
            | (:Document {fulltext: 'more words'})       |
            | (:Document {fulltext: 'words and things'}) |

    Scenario: Search aggregate
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX complianceDocuments ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Rules2024', version: 1})
            CREATE (:Document {title: 'Rules2024', version: 2})
            """
        When executing query:
            """
            CALL text_search.aggregate('complianceDocuments', 'data.title:Rules2024', '{"count":{"value_count":{"field":"data.version"}}}') YIELD aggregation
            RETURN aggregation
            """
        Then the result should be:
            | aggregation              |
            | '{"count":{"value":2.0}}'|

    Scenario: Search query with boolean logic
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX complianceDocuments ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Rules2023', fulltext: 'nothing'})
            CREATE (:Document {title: 'Rules2024', fulltext: 'words', version: 2})
            """
        When executing query:
            """
            CALL text_search.search('complianceDocuments', '(data.title:Rules2023 OR data.title:Rules2024) AND data.fulltext:words') YIELD node
            RETURN node.title AS title, node.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title       | version |
            | 'Rules2024' | 2       |

    Scenario: Add node and search
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX complianceDocuments ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Rules2024', version: 1})
            """
        When executing query:
            """
            CALL text_search.search('complianceDocuments', 'data.title:Rules2024') YIELD node
            RETURN node.title AS title, node.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title       | version |
            | 'Rules2024' | 1       |

    Scenario: Delete indexed node
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX complianceDocuments ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Rules2024', version: 2})
            """
        And having executed
            """
            MATCH (n:Document {title: 'Rules2024', version: 2}) DETACH DELETE n
            """
        When executing query:
            """
            CALL text_search.search('complianceDocuments', 'data.title:Rules2024') YIELD node
            RETURN node.title AS title, node.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title | version |

    Scenario: Update property of indexed node
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX complianceDocuments ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Rules2024', version: 1})
            """
        And having executed
            """
            MATCH (n:Document {version:1}) SET n.title = 'Rules2030'
            """
        When executing query:
            """
            CALL text_search.search('complianceDocuments', 'data.title:Rules2030') YIELD node
            RETURN node.title AS title, node.version AS version
            ORDER BY version ASC, title ASC
            """
        Then the result should be:
            | title       | version |
            | 'Rules2030' | 1       |

    Scenario: Case-insensitive regex search with lowercase query
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX testIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'TITLE ONE'})
            CREATE (:Document {title: 'title two'})
            CREATE (:Document {title: 'Title Three'})
            """
        When executing query:
            """
            CALL text_search.regex_search('testIndex', 't.*') YIELD node
            RETURN node.title AS title
            ORDER BY title ASC
            """
        Then the result should be:
            | title         |
            | 'TITLE ONE'   |
            | 'Title Three' |
            | 'title two'   |

    Scenario: Case-insensitive regex search with uppercase query
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX testIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {content: 'Testing REGEX patterns'})
            CREATE (:Document {content: 'regex TESTING patterns'})
            CREATE (:Document {content: 'No match here'})
            """
        When executing query:
            """
            CALL text_search.regex_search('testIndex', 'T.*G') YIELD node
            RETURN node.content AS content
            ORDER BY content ASC
            """
        Then the result should be:
            | content                    |
            | 'Testing REGEX patterns'   |
            | 'regex TESTING patterns'   |

    Scenario: Search on nonexistent text index raises error
        Given an empty graph
        When executing query:
            """
            CALL text_search.search('noSuchIndex', 'data.fulltext:words') YIELD node
            RETURN node.title AS title, node.version AS version
            ORDER BY version ASC, title ASC
            """
        Then an error should be raised

    Scenario: Create text index with specific properties
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX titleContentIndex ON :Document(title, content)
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                        | label      | property            | count |
            | 'label_text (name: titleContentIndex)'  | 'Document' | ['title','content'] | 0     |

    Scenario: Search in property-specific index with both properties
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX titleContentIndex ON :Document(title, content)
            """
        And having executed
            """
            CREATE (:Document {title: 'Manual2024', content: 'database operations guide'})
            CREATE (:Document {title: 'Guide2024', content: 'user manual instructions'})
            CREATE (:Document {title: 'Other', description: 'not indexed property'})
            """
        When executing query:
            """
            CALL text_search.search('titleContentIndex', 'data.title:Manual2024') YIELD node
            RETURN node.title AS title, node.content AS content
            ORDER BY title ASC
            """
        Then the result should be:
            | title        | content                    |
            | 'Manual2024' | 'database operations guide'|

    Scenario: Search in property-specific index with only one required property
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX titleContentIndex ON :Document(title, content)
            """
        And having executed
            """
            CREATE (:Document {title: 'OnlyTitle', version: 1})
            CREATE (:Document {content: 'only content here', version: 2})
            CREATE (:Document {description: 'no indexed properties', version: 3})
            """
        When executing query:
            """
            CALL text_search.search('titleContentIndex', 'data.title:OnlyTitle') YIELD node
            RETURN node.title AS title, node.version AS version
            ORDER BY version ASC
            """
        Then the result should be:
            | title       | version |
            | 'OnlyTitle' | 1       |

    Scenario: Verify nodes without indexed properties are not searchable
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX titleContentIndex ON :Document(title, content)
            """
        And having executed
            """
            CREATE (:Document {title: 'Findable', content: 'also findable'})
            CREATE (:Document {description: 'not indexed property', summary: 'also not indexed'})
            CREATE (:Document {other: 'completely different property'})
            """
        When executing query:
            """
            CALL text_search.search('titleContentIndex', 'data.description:indexed') YIELD node
            RETURN node
            """
        Then the result should be empty

    Scenario: Delete property from indexed node
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX testIndex ON :TestLabel
            """
        And having executed
            """
            CREATE (:TestLabel {title: 'Test Title', content: 'Test content'})
            """
        When executing query:
            """
            CALL text_search.search('testIndex', 'data.title:Test') YIELD node
            RETURN count(node) AS count
            """
        Then the result should be:
            | count |
            | 1     |

        And having executed
            """
            MATCH (n:TestLabel {title: 'Test Title'}) SET n.title = null
            """
        When executing query:
            """
            CALL text_search.search('testIndex', 'data.title:Test') YIELD node
            RETURN count(node) AS count
            """
        Then the result should be:
            | count |
            | 0     |

        When executing query:
            """
            CALL text_search.search('testIndex', 'data.content:Test') YIELD node
            RETURN count(node) AS count
            """
        Then the result should be:
            | count |
            | 1     |

    Scenario: Create index on existing nodes
        Given an empty graph
        And having executed
            """
            CREATE (:Article {title: 'Database Systems', content: 'Introduction to graph databases and their applications'})
            CREATE (:Article {title: 'Query Languages', content: 'Cypher query language for graph database operations'})
            """
        And having executed
            """
            CREATE TEXT INDEX article_index ON :Article
            """
        When executing query:
            """
            CALL text_search.search('article_index', 'data.content:graph') YIELD node
            RETURN node.title AS title ORDER BY title
            """
        Then the result should be:
            | title              |
            | 'Database Systems' |
            | 'Query Languages'  |

    Scenario: Show index info test
        Given an empty graph
        And having executed
            """
            CREATE (:Article {title: 'Database Systems', content: 'Introduction to graph databases and their applications'})
            CREATE (:Article {title: 'Query Languages', content: 'Cypher query language for graph database operations'})
            """
        And having executed
            """
            CREATE TEXT INDEX article_index ON :Article
            """
        When executing query:
            """
            SHOW INDEX INFO
            """
        Then the result should be:
            | index type                          | label      | property  | count |
            | 'label_text (name: article_index)'  | 'Article'  | []        | 2     |


    # Text search tests for edges

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
