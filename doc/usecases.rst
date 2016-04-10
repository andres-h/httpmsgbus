.. _use-cases:

*********
Use Cases
*********

Sending and receiving simple JSON objects
=========================================

Demo scripts send_json.py and receive_json.py are included in the distribution.

* Start httpmsgbus without arguments; in this case persistent storage is not used. Busses and queues are created on demand when first used.

* In another shell, call:
  ::
    $ python send_json.py notice "something happened"

* Now the bus and queue have been created, so you can subscribe to the queue by calling:
  ::
    $ python receive_json.py

* Any messages sent by send_json.py will now be received by receive_json.py.

JSON objects can be easily used in Javascript, for example a Javascript client may connect to HMB and display alerts on a web page.

When the httpmsgbus process in the above example is killed, all messages will be lost. It is possible to enable persistent storage using the -D command-line option, in which case the messages will be saved and available after a restart.

To save messages in files in a directory "filedb", use:
::
  $ httpmsgbus -D filedb://filedb

To save messages in a MongoDB database, use:
::
  $ httpmsgbus -D mongodb://localhost:27017

In the latter case, you can see your messages using the MongoDB shell:
::
  $ mongo test
  MongoDB shell version: 3.0.7
  connecting to: test
  > db.SYSTEM_ALERT.findOne()
  {
          "_id" : ObjectId("5702e2e22ba8707f01375106"),
          "type" : "SYSTEM_ALERT",
          "queue" : "SYSTEM_ALERT",
          "sender" : "j5WknMw9h1wQUovw",
          "seq" : NumberLong(0),
          "data" : {
                  "text" : "something happened",
                  "level" : "notice"
          }
  }

Sending and receiving binary objects
====================================

The BSON format can be used to embed binary data without space overhead. For example, the "eventpush" program (available in the "hmb-clients" repository) uses HMB to send messages containing compressed XML data. Such messages can be received using the "qmlreceiver" program.

Sending and receiving SC3 data model items
==========================================

In SeisComP 3, HMB is disabled by default and can be enabled by adding the following options to ~/seiscomp3/etc/kernel.ini:
::
  hmb.enable = true
  hmb.port = 8000

Thereafter HMB can be configured with "scconfig" and started like any other SC3 module:
::
  $ seiscomp start httpmsgbus

Modules like scimport can send messages to HMB instead of the Spread messaging server, for example:
::
  $ echo "msggroups = AMPLITUDE,PICK,LOCATION,MAGNITUDE,EVENT,QC,INVENTORY,CONFIG" >scimport.cfg
  $ seiscomp exec scimport --no-filter -o hmb://localhost:8000/test --console 1 -v

Likewise, messages can be received from HMB instead of Spread:
::
  $ seiscomp exec scmm -H hmb://localhost:8000/test --console 1 -v

The "pick2hmb" program, included in the distribution, can be studied as a C++ example of sending SC3 objects to HMB.

Sending and receiving waveform data
===================================

The wavefeed SC3 module can be started to feed waveform data to HMB instead of a local SeedLink server.
::
  $ seiscomp start wavefeed

Now an HMB recordstream can be used:
::
  $ seiscomp exec scrttv -I hmb://localhost:8000/wave
