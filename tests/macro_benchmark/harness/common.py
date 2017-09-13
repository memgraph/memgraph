import os

WALL_TIME = "wall_time"
CPU_TIME = "cpu_time"
MAX_MEMORY = "max_memory"

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

def get_absolute_path(path, base=""):
    if base == "build":
        extra = "../../../build"
    elif base == "build_release":
        extra = "../../../build_release"
    elif base == "libs":
        extra = "../../../libs"
    elif base == "config":
        extra = "../../../config"
    else:
        extra = ""
    return os.path.normpath(os.path.join(DIR_PATH, extra, path))
