class PopulationCreationException(Exception):
    def __init__(self, message):
        super().__init__(message)


class MissingParametersException(Exception):
    def __init__(self, message):
        super().__init__(message)


class WrongColoringException(Exception):
    def __init__(self, message):
        super().__init__(message)


class IllegalColorException(Exception):
    def __init__(self, message):
        super().__init__(message)


class IllegalNodeException(Exception):
    def __init__(self, message):
        super().__init__(message)


class IncorrectParametersException(Exception):
    def __init__(self, message):
        super().__init__(message)
