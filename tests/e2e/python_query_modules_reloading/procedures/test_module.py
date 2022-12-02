import mgp
import networkx
from mage.test_module.test_functions import test_function as test_function1

# isort: off
# fmt: off
from mage.test_module.test_functions_dir.test_functions import \
    test_function as test_function2

# fmt: on


@mgp.read_proc
def test(
    ctx: mgp.ProcCtx, a: mgp.Number, b: mgp.Number
) -> mgp.Record(result1=mgp.Number, result2=mgp.Number, test_version=str):
    return mgp.Record(result1=test_function1(a, b), result2=test_function2(a, b), test_version=networkx.__version__)
