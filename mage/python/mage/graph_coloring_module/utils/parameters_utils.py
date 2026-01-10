import types
from typing import Any, Dict

from mage.graph_coloring_module.graph import Graph


def param_value(graph: Graph, parameters: Dict[str, Any], param: str, initial_value: Any = None) -> Any:
    if parameters is None:
        if initial_value is None:
            return None
        return initial_value

    param = parameters.get(param)

    if param is None:
        if initial_value is None:
            return None
        return initial_value

    if isinstance(param, types.FunctionType):
        param = param(graph)

    return param
