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
      CREATE (:Video {id: "db3b98b6-0497-4f57-ae07-793eea62d1b3", views: 42})
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
