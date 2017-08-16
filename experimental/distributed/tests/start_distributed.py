# Unfortunately I don't know how to force CMake to copy this script to
# the test folder so for now you will have to do it yourself.

import os

command = 'gnome-terminal'
program = './distributed'
config_filename = 'config'
flags = ' --minloglevel 2'

f = open(config_filename, 'r')
for line in f:
  data = line.strip().split(' ')
  my_mnid = data[0]
  address = data[1]
  port = data[2]
  call = program + flags + ' --my_mnid ' + my_mnid + ' --address ' + address +\
         ' --port ' + port + ' --config_filename ' + config_filename
  command += " --tab -e '" + call + "'"

os.system(command)