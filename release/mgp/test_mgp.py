import importlib.util
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
spec = importlib.util.spec_from_file_location("mgp", os.path.join(SCRIPT_DIR, "..", "..", "include", "mgp.py"))
mgp = importlib.util.module_from_spec(spec)
sys.modules["mgp"] = mgp
spec.loader.exec_module(mgp)


def test_query_module_can_be_imported_with_mocked_mgp():
    mgp.Logger().info("loaded")

    @mgp.read_proc
    def read(ctx: mgp.ProcCtx, x: int, y: str = "a") -> mgp.Record(out=int):
        return mgp.Record(out=x + len(y))

    @mgp.write_proc
    def write(ctx: mgp.ProcCtx) -> mgp.Record(out=mgp.Nullable[str]):
        return mgp.Record(out=None)

    def init(query: str, config: mgp.Map):
        return query, config

    def fetch(query: str, config: mgp.Map) -> mgp.Record(row=mgp.Map):
        return [mgp.Record(row={"query": query, **config})]

    mgp.add_batch_read_proc(fetch, init, lambda: None)
    mgp.add_batch_write_proc(fetch, init, lambda: None)

    @mgp.function
    def func(ctx: mgp.FuncCtx, a: mgp.Any, b: int = 1):
        return a or b
