const { gql } = require("apollo-server");

module.exports.typeDefs = gql`
type Video @node {
  id: ID @id
  views: Int
  ownedBy: [User!]! @relationship(type: "OWN_VIDEO", properties: "OwnVideo", direction: IN)
}

type User @node {
  id: ID @id
  ownVideo: [Video!]! @relationship(type: "OWN_VIDEO", properties: "OwnVideo", direction: OUT)
}

type OwnVideo @relationshipProperties {
  revenue: Float
}

type Mutation {
  setup: Boolean
    @cypher(
    statement: """
      CREATE (v1:Video {id: "db3b98b6-0497-4f57-ae07-793eea62d1b3", views: 42}),
             (v2:Video {id: "38142e1a-c843-45d7-8b57-3bd6984fd478", views: 11}),
        (u1:User {id: "fe2a1e27-b42f-4f11-93a8-690704afdb35"}),
        (u2:User {id: "1d2fd1cd-d4ad-4c45-beda-fce64badcb8e"}),
        (u1)-[:OWN_VIDEO {revenue: 1.0}]->(v1),
        (u2)-[:OWN_VIDEO {revenue: 2.0}]->(v2),
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
