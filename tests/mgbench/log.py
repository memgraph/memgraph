COLOR_GRAY = 0
COLOR_RED = 1
COLOR_GREEN = 2
COLOR_YELLOW = 3
COLOR_BLUE = 4
COLOR_VIOLET = 5
COLOR_CYAN = 6


def log(color, *args):
    print("\033[1;3{}m~~".format(color), *args, "~~\033[0m")


def init(*args):
    log(COLOR_BLUE, *args)


def info(*args):
    log(COLOR_CYAN, *args)


def success(*args):
    log(COLOR_GREEN, *args)


def warning(*args):
    log(COLOR_YELLOW, *args)


def error(*args):
    log(COLOR_RED, *args)
