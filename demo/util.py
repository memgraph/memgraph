# -*- coding: utf-8 -*-

'''
python utilities
'''

import os
import sys
import json


def get_env(env_name, default=None, type=None):
    '''
    Fetches environment variable.
    '''
    if env_name in os.environ and type:
        return type(os.environ[env_name])
    elif env_name in os.environ:
        return os.environ[env_name]
    elif type:
        return type(default)

    return default


def set_modul_attrs(name, values):
    """
    Updates the module with the values.

    :param name: str, module name, if this method call is located
                 inside module then use __name__
    :param values: dict, contains new key values.
    """
    #   get the config module (this module)
    config_module = sys.modules[name]

    #   set all the values for given keys on this module
    for key, value in values.items():
        setattr(config_module, key, value)


def load_module_attrs(name, env_flag):
    """
    A function for initializing module from config files (JSON formatted).
    Called when loading the module. The module is specified by it's name.

    :param name: str, module name, if this method call is located
                 inside module then use __name__
    :param env_flag: str, name of the env variable in which is path to JSON
                     file from which module attributes are going to be
                     populated
    :param argv_flag: str, name of program argument which will define path
                      to the json file
    """
    config_path = get_env(env_flag)
    if config_path:
        with open(config_path, 'r') as config_file:
            set_modul_attrs(name, json.load(config_file))
