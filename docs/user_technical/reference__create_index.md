## CREATE INDEX

### Summary

Create an index on the specified label, property pair.

### Syntax

```opencypher
CREATE INDEX ON :<label_name>(<property_name>)
```

### Remarks

  * `label_name` is the name of the record label.
  * `property_name` is the name of the property within a record.
  * At the moment, created indexes cannot be deleted.
