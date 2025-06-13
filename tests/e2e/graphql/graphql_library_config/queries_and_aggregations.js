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
  setup: Boolean
    @cypher(
    statement: """
      CREATE
       (a:User { name: "Alice", id: "51f65ea1-b612-47e6-8cc1-c13735168130", age: 23 }),
       (b:User { name: "Bob", id: "02bae290-1943-49e6-8be8-f15c1a0c5923", age: 42 }),
       (a)-[:HAS_POST {date: "2011-12-03T10:15:30+01:00[Europe/Paris]"}]->(:Post {id: "5cd0d311-16d8-4e88-9cde-e815c7623a6c", content: "First post", createdAt: "2011-12-03T10:15:30+01:00[Europe/Paris]"}),
       (a)-[:HAS_POST {date: "2011-12-04T10:15:30+01:00[Europe/Paris]"}]->(:Post {id: "fb2a6146-6511-45ae-8956-bc8977f586cc", content: "Second post", createAt: "2011-12-04T10:15:30+01:00[Europe/Paris]"}),
       (a)-[:HAS_POST {date: "2011-12-05T10:15:30+01:00[Europe/Paris]"}]->(:Post {id: "0dc886aa-8c65-48f1-8e46-fd628d695831", content: "Third one", createdAt: "2011-12-05T10:15:30+01:00[Europe/Paris]"}),
       (b)-[:HAS_POST {date: "2011-12-06T10:15:30+01:00[Europe/Paris]"}]->(:Post {id: "94f5a303-8776-4272-ba82-01c1755a43ad", content: "Fourth one", createdAt: "2011-12-06T10:15:30+01:00[Europe/Paris]"})
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
