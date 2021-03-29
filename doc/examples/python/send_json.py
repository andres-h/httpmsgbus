#!/usr/bin/env python3

import sys
import json
from urllib.request import Request, urlopen

BUS = 'http://localhost:8000/test'

level = sys.argv[1]
text = sys.argv[2]

msg = {
    'type': 'SYSTEM_ALERT',
    'queue': 'SYSTEM_ALERT',
    'data': {
        'level': level,
        'text': text
    }
}

r = Request(BUS + '/open')
r.add_header('Content-Type', 'application/json')
ack = json.loads(urlopen(r, json.dumps({}).encode('utf-8')).read())

r = Request(BUS + '/send/' + ack['sid'])
r.add_header('Content-Type', 'application/json')
urlopen(r, json.dumps({'0': msg}).encode('utf-8'))

