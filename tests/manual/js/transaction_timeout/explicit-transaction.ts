import { Driver, Session } from "neo4j-driver";

import { EMPTY } from "rxjs"
import { catchError, finalize, map, mergeMap, concatWith } from "rxjs/operators"

const neo4j = require("neo4j-driver");

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

setup().then(() => {
  const rxSession = driver.rxSession({ defaultAccessMode: 'READ' });
  rxSession
    .beginTransaction({ timeout: 50 }) // NOTE: a short timeout
    .pipe(
      mergeMap(tx =>
        tx
          .run('MATCH (),(),(),() RETURN 42 AS thing;') // NOTE: a long query
          .records()
          .pipe(
            catchError(err => { tx.rollback(); throw err; }),
            concatWith(EMPTY.pipe(finalize(() => tx.commit())))
          )
      ),
      finalize(() => { rxSession.close(); driver.close() })
    )
    .subscribe({
      next: record => { },
      complete: () => { console.info('complete'); process.exit(1); }, // UNEXPECTED
      error: msg => console.error('Error:', msg.message), // NOTE: expected to error with server side timeout
    })
})
