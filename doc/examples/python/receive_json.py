#!/usr/bin/env python

import json
from urllib2 import Request, urlopen

BUS='http://localhost:8000/test'

print "Connecting to %s and receiving objects..." % BUS

param = {
    'heartbeat': 10,
    'queue': {
        'SYSTEM_ALERT': {
            'seq': -1
        }
    }
}

r = Request(BUS + '/open')
r.add_header('Content-Type', 'application/json')
ack = json.loads(urlopen(r, json.dumps(param)).read())
print json.dumps(ack, indent=2)

oid = ""
while True:
    with urlopen(BUS + '/recv/' + str(ack['sid']) + oid) as fh:
        for msg in json.loads(fh.read()).values():
            print json.dumps(msg, indent=2)

            try:
                oid = '/' + str(msg['queue']) + '/' + str(msg['seq'])

            except KeyError:
                pass

