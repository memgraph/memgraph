CREATE ({boolean: true, integer: 1, float: 2.0, string: "abcd"});
CREATE ({boolean: true, integer: 1, float: 2.0, string: "abcd"});
CALL periodic.iterate("MATCH (n) RETURN n.boolean as boolean, n.integer as integer, n.float as float, n.string as string",  "CREATE ({copy: true, boolean: boolean, integer: integer, float: float, string: string})", {batch_size: 1}) YIELD success RETURN success;
