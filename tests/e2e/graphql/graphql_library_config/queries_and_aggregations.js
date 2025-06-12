const { gql } = require("apollo-server");

module.exports.typeDefs = gql`
type Post @node {
    id: ID! @id
    content: String!
    creators: [User!]! @relationship(type: "HAS_POST", direction: IN, properties: "PostedAt")
    createdAt: DateTime!
}

type User @node {
    id: ID! @id
    name: String!
    age: Int!
    posts: [Post!]! @relationship(type: "HAS_POST", direction: OUT, properties: "PostedAt")
    friends: [User!]! @relationship(type: "FRIENDS_WITH", direction: OUT)
}

type PostedAt @relationshipProperties {
    date: DateTime
}

type Mutation {
  populate: Boolean
    @cypher(
    statement: """
      CREATE
       (:User { name: "Alice", id: "51f65ea1-b612-47e6-8cc1-c13735168130", age: 23 }),
       (:User { name: "Bob", id: "02bae290-1943-49e6-8be8-f15c1a0c5923", age: 42 })
      RETURN true AS success
    """
    columnName: "success"
    )

  deleteAllNodes: Int
    @cypher(
      statement: """
        MATCH (x)
        DETACH DELETE x
        RETURN COUNT(x) AS num_deleted_nodes
      """
      columnName: "num_deleted_nodes"
    )
}`
