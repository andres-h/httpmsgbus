************
Introduction
************

httpmsgbus (HMB) functions as a messaging service, which runs over HTTP. It facilitates the transfer of objects, such as `SeisComP <http://www.seiscomp3.org/>`_ data model items, but also other content, between a server and a client. Messages sent by one client can be received by multiple clients connected to the same bus. `JSON <http://json.org/>`_ and `BSON <http://bsonspec.org>`_ formats are used for communication.

A bus may have multiple *queues*. Order of messages within a queue is preserved. A queue may have multiple *topics*; topic name is simply an attribute of a message. A receiving client subscribes to one or more queues and tells which topics it is interested in.

Each message within a queue has a sequence number, so it is possible to resume connection without data loss, provided that the needed messages are still in the queue. A client can also select messages based on start- and end-time, and filter messages using a subset of `MongoDB <https://www.mongodb.org/>`_ query language.

HMB supports out-of-order messages by letting a sending client specify the sequence number when sending messages. Messages are received in order; a receiving client may ignore out-of-order messages or wait for missing messages until a timeout.

httpmsgbus can be used as a standalone program or as an add-on to SeisComP.

.. note::

   httpmsgbus is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2, or (at your option) any later version. For more information, see http://www.gnu.org/
