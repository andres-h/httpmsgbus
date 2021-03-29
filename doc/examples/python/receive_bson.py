#!/usr/bin/env python3

import bson
import bson.json_util
from urllib.request import urlopen

BUS='http://localhost:8000/test'

print("Connecting to %s and receiving objects..." % BUS)

param = {
    'heartbeat': 10,
    'queue': {
        'SYSTEM_ALERT': {
            'seq': -1
        }
    }
}

ack = bson.BSON(urlopen(BUS + '/open', bson.BSON.encode(param)).read()).decode()
print(bson.json_util.dumps(ack, indent=2))

for msg in bson.decode_file_iter(urlopen(BUS + '/stream/' + str(ack['sid']))):
    print(bson.json_util.dumps(msg, indent=2))

