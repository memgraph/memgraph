const { gql } = require("graphql-tag");

module.exports.typeDefs = gql`
type Post @node {
    id: ID! @id
    content: String!
    creator: [User!]! @relationship(type: "HAS_POST", direction: IN)
}

type User @node {
    id: ID! @id
    name: String
    posts: [Post!]! @relationship(type: "HAS_POST", direction: OUT)
}

type Mutation {
  setup: Boolean
    @cypher(
    statement: """
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
