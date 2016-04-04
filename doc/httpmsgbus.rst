.. _httpmsgbus:

**********
httpmsgbus
**********

Description
===========

"httpmsgbus" is a server implementing the HMB protocol. It can use a database (-D) or RAM only. httpmsgbus usually runs behind a web server acting as a reverse proxy.

Database URL
============

Supported database types are mongodb and filedb. MongoDB is recommended in most scenarios. Filedb is suitable for binary messages with fixed size, such as waveform records. Database URL can take one of the following forms:

mongodb://server:port
  Use MongoDB server running at server:port. A repository will be created for each bus and a capped collection for each queue. The collection size is determined by the -q command-line parameter.

filedb:///directory?blocksPerFile=int&blocksize=int&bufsize=int&maxOpenFiles=int
  Use filedb in directory. A subdirectory will be created for each bus and a further subdirectory for each queue. Messages will be stored in (possibly sparse) files consisting of fixed size blocks. File name will be the sequence number of the first block in file. The total size of files in a directory is determined by the -q command-line parameter.

The following optional parameters are supported by filedb:

blocksPerFile (default 1024)
  Number of blocks per file.

blocksize (default 1024)
  Size of one block in bytes, maximum message size.

bufsize (default 65536)
  Read buffer size.

maxOpenFiles (default 800)
  Maximum number of file handles to keep open.

X-Forwarded-For
===============

When HMB runs behind a reverse proxy, connections appear to originate from localhost or wherever the proxy is running. Using the X-Forwarded-For HTTP header, it is possible to determine the actual IP address of a client, which is then used for session counting (-c), etc.

Sequence difference into future
===============================

When using multiple servers, it may happen that a client requests a future message that has not reached the current server yet. Sequence numbers in future can be enabled with the -d command-line option. If the maximum difference is exceeded, the sequence number is considered invalid and the current seqence number is used.

Command-line
============

httpmsgbus [options]

-D string
  Database URL

-F
  Use X-Forwarded-For

-P int
  TCP port (default 8000)

-V
  Show program's version and exit

-b int
  Buffer (RAM) size in messages per queue (default 100)

-c int
  Connections (sessions) per IP (default 10)

-d int
  Maximum sequence difference into future (default 0)

-h
  Show help message

-p int
  Maximum size of POST data in KB (default 10240)

-q int
  Queue (MongoDB capped collection) size in MB (default 256)

-s
  Log via syslog

-t int
  Session timeout in seconds (default 120)
