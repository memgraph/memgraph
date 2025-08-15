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
            | index type                  | label      | property | count |
            | 'text (name: exampleIndex)' | 'Document' | []       | 0     |

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
            CREATE (:Document {title: 'Rules2024', version: 1, metadata: {gid: 10}})
            CREATE (:Document {title: 'Rules2024', version: 2, metadata: {gid: 11}})
            """
        When executing query:
            """
            CALL text_search.aggregate('complianceDocuments', 'data.title:Rules2024', '{"count":{"value_count":{"field":"metadata.gid"}}}') YIELD aggregation
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
            | 'text (name: titleContentIndex)'  | 'Document' | ['title','content'] | 0     |

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
            | index type                    | label      | property  | count |
            | 'text (name: article_index)'  | 'Article'  | []        | 2     |
