# -*- coding: utf-8 -*-

def query(q, context, params={}):
    """
    Function used to execute query on database. Query results are
    set in context.result_list. If exception occurs, it is set on
    context.exception.

    @param q:
        String, database query.
    @param context:
        behave.runner.Context, context of all tests.
    @return:
        List of query results.
    """
    results_list = []

    session = context.driver.session()
    try:
        # executing query
        results = session.run(q, params)
        results_list = list(results)
        """
        This code snippet should replace code which is now
        executing queries when session.transactions will be supported.

        with session.begin_transaction() as tx:
            results = tx.run(q, params)
            summary = results.summary()
            results_list = list(results)
            tx.success = True
        """
    except Exception as e:
        # exception
        context.exception = e
        context.log.info('%s', str(e))
    finally:
        session.close()

    return results_list
