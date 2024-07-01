import os


def compose_path(filename: str):
    return os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "data", filename))


def load_test_data(filename: str):
    with open(file=compose_path(filename=filename), mode="r") as test_data_file:
        return test_data_file.read()
