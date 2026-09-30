const { gql } = require("graphql-tag");

module.exports.typeDefs = gql`
type Movie @node {
    title: String
    tags: [String!]
}

type Mutation {
  setup: Boolean
    @cypher(
    statement: """
      CREATE (:Movie {title: "The Matrix", tags: ["action", "sci-fi"]})
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
