.. _installation:

************
Installation
************

Compiling and installing HMB
============================

* Install Go from https://golang.org/dl/ and make sure that the "go" tool is in the path.

* Install PCRE development package for your Linux distribution (usually named pcre-devel or libpcre3-dev).

* Either copy HMB source code to SC3 source tree and use CMake or simply call the "install.sh" script included.


Configuring reverse proxy
=========================

HMB does not implement SSL, HTTP compression and authentication, so when providing the HMB service you usually want to have HMB behind a reverse proxy. Below is an example Apache configuration using mod_proxy and mod_deflate and implementing basic authentication. In some cases you may want to expose only a subset of methods or use different credentials for sending and receiving.
::
  ProxyPass        /hmb/busname/ http://hmbserver:8000/busname/
  ProxyPassReverse /hmb/busname/ http://hmbserver:8000/busname/

  <Proxy http://hmbserver:8000/>
  AuthType Basic
  AuthName "HMB"
  AuthUserFile /srv/www/hmbusers
  Require valid-user
  SetOutputFilter DEFLATE
  SetInputFilter INFLATE
  </Proxy>
