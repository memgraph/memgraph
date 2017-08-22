# Unfortunately I don't know how to force CMake to copy this script to
# the test folder so for now you will have to do it yourself.

import os

command = 'gnome-terminal'
program = './distributed_test'
config_filename = 'config'
flags = '--minloglevel=2'

f = open(config_filename, 'r')
for line in f:
  data = line.strip().split(' ')
  my_mnid = data[0]
  address = data[1]
  port = data[2]
  call = "{} {} --my_mnid {} --address {} --port {} --config_filename={}".format(
    program, flags, my_mnid, address, port, config_filename)
  command += " --tab -e '{}'".format(call)

print(command)
os.system(command)
