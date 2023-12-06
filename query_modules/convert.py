from json import loads

import mgp


@mgp.function
def str2object(string: str) -> mgp.Any:
    if string:
        return loads(string)
    return None
