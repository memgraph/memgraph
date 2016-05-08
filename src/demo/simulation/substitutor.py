# -*- coding: utf-8 -*-

import sys
import random
import string


char_options = string.ascii_lowercase + string.digits


def random_integer(start=0, end=sys.maxsize):
    '''
    :returns: random integer between start and end
    '''
    return random.randint(start, end)


def random_float(start=0, end=1):
    '''
    :returns: random float between start and end (uniform distribution)
    '''
    return random.uniform(start, end)


def random_bool():
    '''
    :returns: random bool value
    '''
    return bool(random.getrandbits(1))


def random_string(size=5):
    '''
    :param size: int, string size

    :returns: random string of specific size build from ascii_lowercase chars
              and digits
    '''
    return ''.join([random.choice(char_options)
                    for _ in range(size)])


placeholders = {
    '#': random_integer,
    '@': random_float,
    '*': random_bool,
    '^': random_string
}


def substitute(text='', placeholders=placeholders):
    '''
    Substitutes chars in text with values generated from functions placed
    in placeholders dict.

    :param text: str, substitutable text
    :param placeholders: dict, key is char that will be substituted, value
                         is function that is going to be used to generate
                         a new value
    '''
    return ''.join((c if c not in placeholders else str(placeholders[c]())
                    for c in iter(text)))


if __name__ == '__main__':

    def test_1():
        print([f() for f in [random_integer, random_float,
                             random_bool, random_string]])

    def test_2():
        return substitute('int # float @ bool * string ^')

    def test_3():
        print(test_2())

    test_3()
