CREATE ();
CREATE ();
CREATE ();
CALL periodic.iterate("MATCH (n) RETURN n", "SET n.prop = 1", {batch_size: 1}) YIELD success RETURN success;
