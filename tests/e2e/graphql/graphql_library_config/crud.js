const { gql } = require("apollo-server");

module.exports.typeDefs = gql`
  type Post @node {
    id: ID! @id
    content: String!
    creator: User!
  }

  type User @node {
    id: ID! @id
    name: String
    posts: [Post!]! @relationship(type: "HAS_POST", direction: OUT)
  }
`;
