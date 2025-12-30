CREATE TRIGGER trigger_default SECURITY DEFINER ON CREATE BEFORE COMMIT EXECUTE UNWIND createdVertices AS node SET node.triggered = true;
CREATE TRIGGER trigger_definer SECURITY DEFINER ON CREATE BEFORE COMMIT EXECUTE UNWIND createdVertices AS node SET node.triggered = true;
CREATE TRIGGER trigger_invoker SECURITY INVOKER ON CREATE BEFORE COMMIT EXECUTE UNWIND createdVertices AS node SET node.triggered = true;
