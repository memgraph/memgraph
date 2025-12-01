const { gql } = require("graphql-tag");

module.exports.typeDefs = gql`
type N @node {
  id: ID!
  name: String
  description: String
  isActive: Boolean

  age: Int
  score: Float
  largeNumber: BigInt

  birthDate: Date
  localMeetingTime: LocalTime
  createdAt: DateTime
  lastModified: LocalDateTime
  sessionDuration: Duration

  location: Point
  coordinates: CartesianPoint
}

type Mutation {
  teardown: Int
    @cypher(
      statement: """
        MATCH (x)
        DETACH DELETE x
        RETURN COUNT(x) AS num_deleted_nodes
      """
      columnName: "num_deleted_nodes"
    )
}
`
