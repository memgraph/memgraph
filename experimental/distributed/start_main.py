#!/usr/bin/env python3
# Automatically copied to the build/ directory during Makefile (configured by cmake)

import os

terminal_command = 'gnome-terminal'
terminal_flags = ' --geometry=200x50 '  # columns x rows

config_filename = 'config'
log_dir = "logs"
glog_flags = '--alsologtostderr --logbufsecs=0 --minloglevel=0 --log_dir="{}" '.format(log_dir)

def GetMainCall(my_mnid, address, port):
  ret = "./main {} --my_mnid {} --address {} --port {} --config_filename={}".format(
    glog_flags, my_mnid, address, port, config_filename)

  print(ret)
  return ret


def GetClientCall():
  ret = "./main-client {} --address 127.0.0.1 --port 10000 --config_filename={}".format(
    glog_flags, config_filename)
  print(ret)
  return ret


def NamedGnomeTab(name, command):
  return " --tab -e \"bash -c 'printf \\\"\\033]0;{}\\007\\\"; {}'\" ".format(name, command)


if __name__ == "__main__":
  command = "{} {}".format(terminal_command, terminal_flags)

  f = open(config_filename, 'r')
  for line in f:
    data = line.strip().split(' ')
    my_mnid = data[0]
    address = data[1]
    port = data[2]
    command += NamedGnomeTab("mnid={}".format(my_mnid), GetMainCall(my_mnid, address, port))

  command += NamedGnomeTab("client", GetClientCall())
  print(command)
  os.system('mkdir -p {}'.format(log_dir))
  os.system(command)
