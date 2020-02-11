********************
Protocol Description
********************

URL format
==========

The URL has the following form:

{prefix}/{busname}/{method}[/{arg1}/{arg2}...]

prefix
  Arbitrary HTTP(S) prefix, such as http://localhost:8000

busname
  Arbitrary bus name (a single server can serve several busses at the same TCP port).

method
  See below.

arg1, arg2, ...
  Method-dependent arguments.

Function
========

Function defines the purpose of the server. The following functions are defined:

SC3MASTER
  SeisComP master messaging server

WAVESERVER
  Waveform server

Capabilities
============

Capabilities define certain features and extensions to the core protocol. The following capabilities are defined:

JSON
  JSON format supported by /open, /send, /recv and /stream (cap:STREAM)

BSON
  BSON format supported (default)

INFO
  /info supported

STREAM
  /stream supported

WINDOW
  Time window requests supported

FILTER
  Filter supported

REGEX
  Filter supports the $regex operator (potential security risk with user-supplied regular expressions)

OOD
  Out-of-order messages supported

Data format
===========

POST data can be in JSON (cap:JSON) or BSON (cap:BSON) format, the actual format is selected by the Content-Type header. The format of GET data is the same as the format used in /open (JSON or BSON). Sessionless methods (/features, /status, /info) use JSON format.

Mandatory methods
=================

/features
*********

Purpose: returns functions and capabilities supported by the server and optionally the name and version of the server software.

Arguments: none.

Response::

  {
    "software": <string>,
    "functions": <list>,
    "capabilities": <list>
  }

software
  Name and version of server software (optional).

functions
  List of functions.

capabilities
  List of capabilities.

/open
*****

Purpose: opens a session, required by subsequent /send, /recv and /stream (cap:STREAM) methods.

Arguments: none

POST input::

  {
    "cid": <string>,
    "heartbeat": <int>,
    "recv_limit": <int>,
    "queue": {
      <queue_name>: {
        "topics": <list of string>,
        "seq": <int>,
        "endseq": <int>,
        "starttime": <string>,
        "endtime": <string>,
        "filter": <doc>,
        "qlen": <int>,
        "oowait": <int>,
        "keep": <bool>
      },
      ...
    },
  }

cid
  Requested client ID (optional).

heartbeat
  Heartbeat interval in seconds.

recv_limit
  Suggested maximum amount of kilobytes to return in one /recv call. The actual size can be slightly larger, depending on message size.

topics
  List of topics that the client is interested in. Wildcards ? and * are supported. A prefix '!' negates the pattern; message is delivered to the client if it matches any of the positive patterns and none of the negative patterns. None is equivalent to ["*"].

seq
  Starting sequence number. Negative numbers count from the end of the queue: -1 is the "next" message, -2 is the last message in the queue and so on. None is equivalent to -1.

endseq (cap:WINDOW)
  Ending sequence number. Can be used by clients to fill sequence gaps (seq..endseq).

starttime (cap:WINDOW)
  Request messages whose starttime..endtime overlaps with given starttime..endtime (time window request).

endtime (cap:WINDOW)
  Request messages whose starttime..endtime overlaps with given starttime..endtime (time window request).

filter (cap:FILTER)
  `MongoDB style <https://docs.mongodb.org/manual/reference/operator/query/>`_ message filter. Operators $and, $or, $not, $nor, $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin, $exists and $regex (cap:REGEX) are supported.

qlen (cap:OOD)
  Maximum queue length (last_sequence - current_sequence). When set, some messages can be discarded to make sure that the client does not fall too much behind real time.

oowait (cap:OOD)
  Maximum time to wait for out-of-order messages, in seconds.

keep
  Keep queue open after all data received (realtime mode).

Response: HTTP 400 with error message or

::

  {
    "queue": {
      <queue_name>: {
        "seq": <int>, 
        "error": <string>
      },
      ...
    }, 
    "sid": <string>,
    "cid": <string>
  }

seq
  Actual sequence number (>= 0) or None if error.

error
  Error string (queue does not exist, invalid parameters, etc.). None if no error (seq must be set).

sid
  Session ID (required in subsequent /send, /recv and /stream methods).

cid
  Assigned client ID.

/status
*******

Purpose: returns the status of connected clients (sessions).

Arguments: none.

Response::

  {
    "session": {
      <sid>: {
        "cid": <string>,
        "address": <string>,
        "ctime": <string>,
        "sent": <int>,
        "received": <int>,
        "format": <string>,
        "heartbeat": <int>,
        "recv_limit": <int>,
        "queue": {
          <queue_name>: {
            "topics": <list of strings>,
            "seq": <int>,
            "endseq": <int>,
            "starttime": <string>,
            "endtime": <string>,
            "filter": <doc>,
            "qlen": <int>,
            "oowait": <int>,
            "keep": <bool>,
            "eof": <bool>
          },
          ...
        },
      },
      ...
    },
  }

address
  Address of peer in ip:port format.

ctime
  Time when the session was created.

sent
  Number of bytes sent (client->server), not accounting HTTP headers and compression.

received
  Number of bytes received (server->client), not accounting HTTP headers and compression.

format
  JSON or BSON.

eof
  End of stream reached.

The remaining attributes have the same meaning as in /open above.

/send
*****

Purpose: sends a message.

Arguments: /sid

sid
  The session ID received from /open.

POST input::

  {
    "type": <string>,
    "queue": <string>,
    "topic": <string>,
    "seq": <int>,
    "starttime": <int>,
    "endtime": <int>,
    "data": <doc>
  }

type
  Message type, eg., "SC3". Can be any string, except "HEARTBEAT" and "EOF".

queue
  Destination queue of the message, eg. "SC3MSG"

topic
  Optional topic/group of the message, eg., "PICK".

seq (cap:OOD)
  Optional sequence number of the message (if None or missing, the sequence number will be assigned by the server).

starttime (cap:WINDOW)
  Optional effective start time of the message.

endtime (cap:WINDOW)
  Optional effective end time of the message.

data
  Payload.

A heartbeat message can be sent to keep an idle session from expiring. The message is otherwise ignored by the server.

::

  {
    "type": "HEARTBEAT"
  }

JSON and BSON (cap:BSON) formats are supported. Multiple messages can be sent in one /send call; in case of BSON format, multiple messages must be concatenated. In case of JSON format, an array-style document must be sent (even if there is only a single message)::

  {
    "0": <msg>,
    "1": <msg>,
    ...
  }

Response: HTTP 400 with error message or HTTP 204.

/recv
*****

Purpose: receive a message.

Arguments: /sid[/queue/seq]

sid
  The session ID received from /open.

queue/seq
  Queue and sequence number of the last message received to ensure continuity in case of network errors (due to buffering, the server can otherwise not be sure that all messages have reached the client).

If sid is not known to server, HTTP 400 is returned and the client should proceed with /open to create a new session.

If queue/seq does not match queue/seq of last message sent, HTTP 400 is returned and the client should proceed with /open to create a new session. However, if queue/seq does match an earlier object, the server may roll back and continue.

Response: HTTP 400 with error message or

::

  {
    "type": <string>,
    "queue": <string>,
    "topic": <string>,
    "sender": <string>,
    "seq": <int>,
    "starttime": <int>,
    "endtime": <int>,
    "data": <doc>
  }

sender
  client ID of the sending client.

The remaining attributes have the same meaning as in /post. 

Two special values are defined for type:

HEARTBEAT
  Heartbeat message.
EOF (cap:WINDOW)
  End of time window (or endseq) reached.

/recv blocks until at least one message (incl. HEARTBEAT) is available and then returns one or more messages. In case of JSON format, an array-style document is returned (even if the response only contains a single message)::

  {
    "0": <msg>,
    "1": <msg>,
    ...
  }

Optional methods
================

/info (cap:INFO)
****************

Purpose: returns the list of queues and topics and available data.

Arguments: none.

Response::

  {
    "queue": {
      <queue_name>: {
        "startseq": <int>,
        "starttime": <string>,
        "endseq": <int>,
        "endtime": <string>,
        "topics": {
          <topic>: {
            "starttime": <string>,
            "endtime": <string>
          },
          ...
        },
      },
      ...
    },
  }
      
startseq
  Sequence number of the first message in queue.

starttime
  Start time of the first message in queue if defined, otherwise null.

endseq
  Sequence number of the last message in queue + 1.

endtime
  End time of the last message in queue if defined, otherwise null.

topics
  Topics in the queue with optional starttime and endtime. The set of topics may not be exhaustive (a perfect implementation requires scanning the whole queue or using separate databases to keep track of available topics).

/stream (cap:STREAM)
********************

Works like /recv, except that /stream sends an endless stream of messages and never returns. In case of JSON format, an array-style document is returned; since the document has no end, only a progressive JSON parser would be useful.
