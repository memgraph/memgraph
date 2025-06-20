const { gql } = require("graphql-tag");

module.exports.typeDefs = gql`
type User @node {
    name: String!
    posts: [Post!]! @relationship(type: "HAS_POST", direction: OUT)
}

type Post @node {
    content: String!
}

type Mutation {
  setup: Boolean
    @cypher(
    statement: """
    CREATE (u:User {name: "John Smith"})
    WITH u
    UNWIND ['alfa', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'gulf', 'hotel', 'india', 'juliett', 'kilo', 'lima', 'mike'] AS word
    CREATE (p:Post {content: word})
    CREATE (u)-[:HAS_POST]->(p)
    RETURN true AS success
    """
    columnName: "success"
    )

  teardown: Int
    @cypher(
      statement: """
        MATCH (x)
        DETACH DELETE x
        RETURN COUNT(x) AS num_deleted_nodes
      """
      columnName: "num_deleted_nodes"
    )
}`
