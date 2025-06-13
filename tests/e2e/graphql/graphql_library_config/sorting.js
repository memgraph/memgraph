const { gql } = require("apollo-server");

module.exports.typeDefs = gql`
type Movie @node {
    title: String!
    runtime: Int!
    actors: [Actor!]! @relationship(type: "ACTED_IN", direction: IN)
}

type Actor @node {
    surname: String!
    movies: [Movie!]! @relationship(type: "ACTED_IN", direction: OUT)
}

type Mutation {
  setup: Boolean
    @cypher(
    statement: """
    CREATE
      (inception:Movie {title: "Inception", runtime: 148}),
      (matrix:Movie {title: "The Matrix", runtime: 136}),
      (interstellar:Movie {title: "Interstellar", runtime: 169}),
      (parasite:Movie {title: "Parasite", runtime: 132}),
      (spirited:Movie {title: "Spirited Away", runtime: 125}),
      (:Actor {surname: "DiCaprio"})-[:ACTED_IN]->(inception),
      (:Actor {surname: "Gordon-Levitt"})-[:ACTED_IN]->(inception),
      (:Actor {surname: "Page"})-[:ACTED_IN]->(inception),
      (:Actor {surname: "Reeves"})-[:ACTED_IN]->(matrix),
      (:Actor {surname: "Moss"})-[:ACTED_IN]->(matrix),
      (:Actor {surname: "Fishburne"})-[:ACTED_IN]->(matrix),
      (:Actor {surname: "McConaughey"})-[:ACTED_IN]->(interstellar),
      (:Actor {surname: "Song"})-[:ACTED_IN]->(parasite),
      (:Actor {surname: "Hiiragi"})-[:ACTED_IN]->(spirited)
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
