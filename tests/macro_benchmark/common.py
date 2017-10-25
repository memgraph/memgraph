import os
from argparse import ArgumentParser

try:
    import jail
    APOLLO = True
except:
    import jail_faker as jail
    APOLLO = False


WALL_TIME = "wall_time"
CPU_TIME = "cpu_time"
MAX_MEMORY = "max_memory"

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


def get_absolute_path(path, base=""):
    if base == "build":
        extra = "../../build"
    elif base == "build_release":
        extra = "../../build_release"
    elif base == "libs":
        extra = "../../libs"
    elif base == "config":
        extra = "../../config"
    else:
        extra = ""
    return os.path.normpath(os.path.join(DIR_PATH, extra, path))


# Assign process to cpus passed by flag_name flag from args. If process is
# running on Apollo that flag is obligatory, otherwise is ignored. flag_name
# should not contain leading dashed.
def set_cpus(flag_name, process, args):
    argp = ArgumentParser()
    # named, optional arguments
    argp.add_argument("--" + flag_name, nargs="+", type=int, help="cpus that "
                      "will be used by process. Obligatory on Apollo, ignored "
                      "otherwise.")
    args, _ = argp.parse_known_args(args)
    attr_flag_name = flag_name.replace("-", "_")
    cpus = getattr(args, attr_flag_name)
    assert not APOLLO or cpus, \
            "flag --{} is obligatory on Apollo".format(flag_name)
    if cpus:
        process.set_cpus(cpus, hyper = False)
