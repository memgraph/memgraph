# -*- coding: utf-8 -*-

import json
from util import get_env, set_modul_attrs

host = "0.0.0.0"

port = 5000

log_level = 'INFO'

try:
    set_modul_attrs(__name__, json.loads(get_env('CONFIG')))
except:
    pass
