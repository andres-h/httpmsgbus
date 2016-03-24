#!/usr/bin/env python

import sys
import bson
from urllib import urlopen

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

ack = bson.BSON(urlopen(BUS + '/open', bson.BSON.encode({})).read()).decode()
urlopen(BUS + '/send/' + ack['sid'], bson.BSON.encode(msg))

