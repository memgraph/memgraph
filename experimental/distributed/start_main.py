#!/usr/bin/env python3
# Automatically copied to the build/ directory during Makefile (configured by cmake)

import os

command = 'gnome-terminal'
config_filename = 'config'
glog_flags = '-alsologtostderr --minloglevel=2'


def GetMainCall(my_mnid, address, port):
  return "./main {} --my_mnid {} --address {} --port {} --config_filename={}".format(
    glog_flags, my_mnid, address, port, config_filename)


def GetClientCall():
  return "./main-client {} --address 127.0.0.1 --port 10000 --config_filename={}".format(
    glog_flags, config_filename)


def NamedGnomeTab(name, command):
  return " --tab -e \"bash -c 'printf \\\"\\033]0;{}\\007\\\"; {}'\" ".format(name, command)


if __name__ == "__main__":
  f = open(config_filename, 'r')
  for line in f:
    data = line.strip().split(' ')
    my_mnid = data[0]
    address = data[1]
    port = data[2]
    command += NamedGnomeTab("mnid={}".format(my_mnid), GetMainCall(my_mnid, address, port))

  command += NamedGnomeTab("client", GetClientCall())
  print(command)
  os.system(command)
