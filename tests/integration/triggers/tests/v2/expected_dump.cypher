CREATE TRIGGER trigger_default SECURITY DEFINER ON CREATE BEFORE COMMIT EXECUTE UNWIND createdVertices AS node SET node.triggered = true;
