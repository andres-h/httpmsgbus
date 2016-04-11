wavefeed
========

Description
-----------

*wavefeed* is a program that runs a SeedLink plugin (normally chain_plugin) and sends the waveform data to HMB. Only log and Mini-SEED packets are supported.

Command-line
------------

wavefeed [options]

-C string
  Plugin command line

-H string
  Destination HMB URL

-V
  Show program's version and exit

-X string
  Regex matching channels with unreliable timing

-b int
  Maximum number of messages to buffer (default 1024)

-h
  Show help message

-s
  Log via syslog

-t int
  HMB timeout in seconds (default 120)
