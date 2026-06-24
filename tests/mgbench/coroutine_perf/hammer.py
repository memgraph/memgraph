import sys
import time

import mgclient

port = int(sys.argv[1])
q = sys.argv[2]
secs = float(sys.argv[3])
c = mgclient.connect(host="127.0.0.1", port=port)
c.autocommit = True
cur = c.cursor()
end = time.time() + secs
while time.time() < end:
    cur.execute(q)
    cur.fetchall()
