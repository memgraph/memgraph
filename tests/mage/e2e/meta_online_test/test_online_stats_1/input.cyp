setup: >-
  CALL meta.reset();

  CREATE TRIGGER meta_stats BEFORE COMMIT EXECUTE CALL
  meta.update(createdObjects, deletedObjects, removedVertexProperties,
  removedEdgeProperties, setVertexLabels, removedVertexLabels);
queries:
  - |-
    CREATE (:Movie {one:1, two:2, three:3});
    CREATE (:Series {one:1, two:2, three:3});
    CREATE (:Movie {one:5, two:2, three:3});
cleanup: |-
  CALL meta.reset();
  DROP TRIGGER meta_stats;
