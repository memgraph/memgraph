CREATE TRIGGER trigger_default ON CREATE BEFORE COMMIT EXECUTE UNWIND createdVertices AS node SET node.triggered = true;
