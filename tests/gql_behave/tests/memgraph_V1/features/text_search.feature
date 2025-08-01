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
            | 'text (name: exampleIndex)' | 'Document' | []       | null  |

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
