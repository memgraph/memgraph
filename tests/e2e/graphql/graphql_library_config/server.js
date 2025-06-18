const { Neo4jGraphQL} = require("@neo4j/graphql");
const { ApolloServer } = require("@apollo/server");
const { startStandaloneServer } = require("@apollo/server/standalone");
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

const startServer = async () => {
    try {
        // Generate the schema
        const schema = await neoSchema.getSchema();

        // Create Apollo Server
        const server = new ApolloServer({
            schema,
            formatError: (error) => {
                console.error('GraphQL Error:', error);
                return {
                    message: error.message,
                    path: error.path,
                    extensions: {
                        code: error.extensions?.code || 'INTERNAL_SERVER_ERROR'
                    }
                };
            }
        });

        const { url } = await startStandaloneServer(server, {
            context: async ({ req }) => ({
                req,
                sessionConfig: { database: "memgraph" },
                cypherQueryOptions: { addVersionPrefix: false }
            }),
            listen: { port: 4000 },
        });
        console.log(`🚀 Server ready at ${url}`);
    } catch (error) {
        console.error('Error starting server:', error);
        process.exit(1);
    }
};

// Handle process termination
process.on('SIGINT', async () => {
    console.log('Shutting down server...');
    await driver.close();
    process.exit(0);
});

startServer();
