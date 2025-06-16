const { gql } = require("apollo-server");

module.exports.typeDefs = gql`
type User @node {
    name: String!
}

type Mutation {
  setup: Boolean
    @cypher(
    statement: """
      UNWIND [
      'Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank', 'Ivy',
      'Jack', 'Kara', 'Leo', 'Mona', 'Nina', 'Oscar', 'Pam', 'Quinn', 'Ray',
      'Sara', 'Tom', 'Uma', 'Vince', 'Wendy'] AS name
      CREATE (:User {name: name})
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
