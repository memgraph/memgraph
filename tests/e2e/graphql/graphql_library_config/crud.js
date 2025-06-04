const { Neo4jGraphQL } = require("@neo4j/graphql");
const { ApolloServer, gql } = require("apollo-server");
const neo4j = require("neo4j-driver");

const typeDefs = gql`
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

const driver = neo4j.driver(
    "bolt://localhost:7687",
    neo4j.auth.basic("", "")
);

const neoSchema = new Neo4jGraphQL({
    typeDefs, driver,
    config: {
        driverConfig: {
            database: "memgraph",
        },
    }
});

neoSchema.getSchema().then((schema) => {
    const server = new ApolloServer({
        schema,
    });

    server.listen().then(({ url }) => {
        console.log(`🚀 Server ready at ${url}`);
    });
})
