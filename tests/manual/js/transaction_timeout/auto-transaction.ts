import { Driver, RxSession, Session } from "neo4j-driver";
import { finalize } from "rxjs";

var neo4j = require("neo4j-driver");

const driver: Driver = neo4j.driver("bolt://localhost:7687");

async function setup() {
  const session: Session = driver.session();

  try {
    await session.run('MATCH (n) DETACH DELETE n');
    await session.run('UNWIND RANGE(1, 100) AS x CREATE ()');
  } finally {
    session.close();
  }
}

setup()
  .then(
    () => {
      const session: RxSession = driver.rxSession({ defaultAccessMode: 'READ' });
      session
        .run("MATCH (), (), (), () RETURN 42 AS thing;", // NOTE: A long query
          undefined,
          { timeout: 50 } // NOTE: with a short timeout
        )
        .records()
        .pipe(finalize(() => {
          session.close();
          driver.close();
        }))
        .subscribe({
          next: record => { },
          complete: () => { console.info('complete'); process.exit(1); }, // UNEXPECTED
          error: msg => console.error('Error:', msg.message), // NOTE: expected to error with server side timeout
        });
    }
  )
