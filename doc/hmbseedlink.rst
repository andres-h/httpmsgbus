Description
===========

"hmbseedlink" is a proxy implementing the SeedLink protocol on top of HMB. A SeedLink station maps to an HMB queue and a SeedLink stream maps to an HMB topic. The payload of an HMB message would be a Mini-SEED record.

Command-line
============

hmbseedlink [options]

-H string
  Source HMB URL

-O string
  Organization

-P int
  TCP port (default 18000)

-V
  Show program's version and exit

-c int
  Connections per IP (default 10)

-h
  Show help message

-q int
  Limit backlog of records (queue length)

-s
  Log via syslog

-t int
  HMB timeout in seconds (default 120)

-w int
  Wait for out-of-order data in seconds
