CREATE INDEX ON :`label2`;
CREATE INDEX ON :`label2`(`prop2`);
CREATE INDEX ON :`label`(`prop2`);
CREATE INDEX ON :`label`(`prop`);
CREATE INDEX ON :`nested`(`a.b.c`);
CREATE EDGE INDEX ON :`edgetype`;
CREATE EDGE INDEX ON :`edgetype`(`prop`);
ANALYZE GRAPH;
