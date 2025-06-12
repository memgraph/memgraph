const { Neo4jGraphQL } = require("@neo4j/graphql");
const { ApolloServer, gql } = require("apollo-server");
const fs = require('fs')
const neo4j = require("neo4j-driver");

const { typeDefs } = require(process.argv[2])

const driver = neo4j.driver("bolt://localhost:7687", neo4j.auth.basic("", ""));

const neoSchema = new Neo4jGraphQL({
    typeDefs,
    driver,
    debug: true,
    config: {
    features: {
      filters: true,
    },
  },
  });

async function start() {
  const schema = await neoSchema.getSchema();

  const server = new ApolloServer({
    schema,
    context: () => ({
      driver,
      sessionConfig: { database: "memgraph" },
      cypherQueryOptions: { addVersionPrefix: false }
    }),
  });

  server.listen().then(({ url }) => {
    console.log(`🚀 Server ready at ${url}`);
  });
}

start()
