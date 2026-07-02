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

    Scenario: Test limit parameter on node search
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX limitTestIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Document1', content: 'test content'})
            CREATE (:Document {title: 'Document2', content: 'test content'})
            CREATE (:Document {title: 'Document3', content: 'test content'})
            CREATE (:Document {title: 'Document4', content: 'test content'})
            CREATE (:Document {title: 'Document5', content: 'test content'})
            """
        When executing query:
            """
            CALL text_search.search('limitTestIndex', 'data.content:test', {limit: 3}) YIELD node
            RETURN count(node) AS count
            """
        Then the result should be:
            | count |
            | 3     |

    Scenario: Test search returns relevance score for nodes
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX scoreTestIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'Test Document', content: 'important content here'})
            CREATE (:Document {title: 'Another Doc', content: 'less relevant text'})
            """
        When executing query:
            """
            CALL text_search.search('scoreTestIndex', 'data.content:important') YIELD node, score
            RETURN node.title AS title, round(score) AS score
            """
        Then the result should be:
            | title           | score  |
            | 'Test Document' | 1.0    |

    Scenario: Fuzzy search with distance 1 tolerates a single edit
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            CREATE (:Document {title: 'memgrap'})
            CREATE (:Document {title: 'coffee'})
            """
        When executing query:
            """
            CALL text_search.search('fuzzyIndex', 'data.title:memgraph', {fuzzy_distance: 1}) YIELD node
            RETURN node.title AS title
            ORDER BY title ASC
            """
        Then the result should be:
            | title       |
            | 'memgrap'   |
            | 'memgraph'  |

    Scenario: Fuzzy search with distance 2 tolerates two edits
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            CREATE (:Document {title: 'memgrahps'})
            CREATE (:Document {title: 'coffee'})
            """
        When executing query:
            """
            CALL text_search.search('fuzzyIndex', 'data.title:memgraph', {fuzzy_distance: 2}) YIELD node
            RETURN node.title AS title
            ORDER BY title ASC
            """
        Then the result should be:
            | title         |
            | 'memgrahps'   |
            | 'memgraph'    |

    Scenario: Fuzzy search with transpositions disabled requires two edits for a swap
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgrahp'})
            """
        When executing query:
            """
            CALL text_search.search('fuzzyIndex', 'data.title:memgraph', {fuzzy_distance: 1, fuzzy_transpositions: false}) YIELD node
            RETURN node.title AS title
            """
        Then the result should be empty

    Scenario: Fuzzy search with prefix matches indexed terms that start with the query
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            CREATE (:Document {title: 'coffee'})
            """
        When executing query:
            """
            CALL text_search.search('fuzzyIndex', 'data.title:mem', {fuzzy_distance: 1, fuzzy_prefix: true}) YIELD node
            RETURN node.title AS title
            """
        Then the result should be:
            | title       |
            | 'memgraph'  |

    Scenario: Fuzzy search_all combines fuzzy with all-properties mode
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyAllIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph', body: 'graph database'})
            CREATE (:Document {title: 'unrelated', body: 'memgrap is close'})
            """
        When executing query:
            """
            CALL text_search.search_all('fuzzyAllIndex', 'memgraph', {fuzzy_distance: 1}) YIELD node
            RETURN node.title AS title
            ORDER BY title ASC
            """
        Then the result should be:
            | title         |
            | 'memgraph'    |
            | 'unrelated'   |

    Scenario: Empty config map preserves default behaviour
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX defaultIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            CREATE (:Document {title: 'memgrap'})
            """
        When executing query:
            """
            CALL text_search.search('defaultIndex', 'data.title:memgraph', {}) YIELD node
            RETURN node.title AS title
            """
        Then the result should be:
            | title       |
            | 'memgraph'  |

    Scenario: Fuzzy distance above 2 is rejected
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX rejectIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            """
        When executing query:
            """
            CALL text_search.search('rejectIndex', 'data.title:memgraph', {fuzzy_distance: 3}) YIELD node RETURN node
            """
        Then an error should be raised

    Scenario: Unknown config key is rejected
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX unknownKeyIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            """
        When executing query:
            """
            CALL text_search.search('unknownKeyIndex', 'data.title:memgraph', {bogus: 1}) YIELD node RETURN node
            """
        Then an error should be raised

    Scenario: Unqualified query without fuzzy still errors at parse time
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX unqualifiedIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            """
        When executing query:
            """
            CALL text_search.search('unqualifiedIndex', 'memgraph', {}) YIELD node RETURN node
            """
        Then an error should be raised

    Scenario: Unqualified query with fuzzy errors at parse time
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX unqualifiedFuzzyIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            """
        When executing query:
            """
            CALL text_search.search('unqualifiedFuzzyIndex', 'memgrahp', {fuzzy_distance: 1}) YIELD node RETURN node
            """
        Then an error should be raised

    Scenario: Nonexistent field prefix errors at parse time
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX wrongFieldIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {title: 'memgraph'})
            """
        When executing query:
            """
            CALL text_search.search('wrongFieldIndex', 'nonexistent.foo:memgraph', {}) YIELD node RETURN node
            """
        Then an error should be raised

    Scenario: Fuzzy distance on regex_search is rejected
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX regexRejectIndex ON :Document
            """
        And having executed
            """
            CREATE (:Document {fulltext: 'memgraph'})
            """
        When executing query:
            """
            CALL text_search.regex_search('regexRejectIndex', 'mem.*', {fuzzy_distance: 1}) YIELD node RETURN node
            """
        Then an error should be raised

    Scenario: Fuzzy phrase search enforces order and adjacency with a last-word prefix
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bad wolf', n: 1})
            CREATE (:Doc {title: 'big bad world', n: 2})
            CREATE (:Doc {title: 'the big bad wolf returns', n: 3})
            CREATE (:Doc {title: 'bad big wolf', n: 4})
            CREATE (:Doc {title: 'big wolf', n: 5})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'data.title:big bad wo') YIELD node
            RETURN node.n AS n
            ORDER BY n ASC
            """
        Then the result should be:
            | n |
            | 1 |
            | 2 |
            | 3 |

    Scenario: Fuzzy phrase search enforces word order
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bad wolf', n: 1})
            CREATE (:Doc {title: 'bad big wolf', n: 4})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'data.title:bad big wo') YIELD node
            RETURN node.n AS n
            ORDER BY n ASC
            """
        Then the result should be:
            | n |
            | 4 |

    Scenario: Fuzzy phrase search tolerates a typo with fuzzy_distance 1
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bad wolf', n: 1})
            CREATE (:Doc {title: 'coffee shop', n: 2})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'data.title:big bd wo', {fuzzy_distance: 1}) YIELD node
            RETURN node.n AS n
            ORDER BY n ASC
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Fuzzy phrase search rejects fuzzy_prefix false
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bad wolf', n: 1})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'data.title:big bad wo', {fuzzy_prefix: false}) YIELD node RETURN node
            """
        Then an error should be raised

    Scenario: Fuzzy phrase search shares the fuzzy budget across the whole input
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bd wolf', n: 1})
            CREATE (:Doc {title: 'bg bd wolf', n: 2})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'data.title:big bad wo', {fuzzy_distance: 1}) YIELD node
            RETURN node.n AS n
            ORDER BY n ASC
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Fuzzy phrase search counts a transposition as one edit
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bad wolf', n: 1})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'data.title:big abd wo', {fuzzy_distance: 1}) YIELD node
            RETURN node.n AS n
            """
        Then the result should be:
            | n |
            | 1 |

    Scenario: Fuzzy phrase search with transpositions disabled needs two edits for a swap
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bad wolf', n: 1})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'data.title:big abd wo', {fuzzy_distance: 1, fuzzy_transpositions: false}) YIELD node
            RETURN node.n AS n
            """
        Then the result should be empty

    Scenario: Fuzzy phrase search rejects fuzzy_distance above 2
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bad wolf', n: 1})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'data.title:big bad wo', {fuzzy_distance: 3}) YIELD node RETURN node
            """
        Then an error should be raised

    Scenario: Fuzzy phrase search requires a single-property query
        Given an empty graph
        And having executed
            """
            CREATE TEXT INDEX fuzzyPhraseIndex ON :Doc
            """
        And having executed
            """
            CREATE (:Doc {title: 'big bad wolf', n: 1})
            """
        When executing query:
            """
            CALL text_search.fuzzy_phrase_search('fuzzyPhraseIndex', 'big bad wo') YIELD node RETURN node
            """
        Then an error should be raised
