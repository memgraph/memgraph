class BaseClass:
    def __init__(self, num_to_return=1) -> None:
        self._init_is_called = False
        self._num_to_return = num_to_return
        self._num_returned = 0

    def reset(self):
        self._init_is_called = False
        self._num_returned = 0

    def set(self):
        self._init_is_called = True

    def get(self):
        return self._init_is_called

    def increment_returned(self, returned: int):
        self._num_returned += returned

    def get_to_return(self) -> int:
        return self._num_to_return - self._num_returned


class InitializationUnderlyingGraphMutable(BaseClass):
    def __init__(self):
        super().__init__()


class InitializationGraphMutable(BaseClass):
    def __init__(self):
        super().__init__()
