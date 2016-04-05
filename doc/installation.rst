.. _installation:

************
Installation
************

Compiling and installing HMB
============================

* Install Go from https://golang.org/dl/ and make sure that the "go" tool is in the path.

* Make sure that you have a working Internet connection and git is installed. Git will be used by the "go" tool to download some additional Go packages.

* Install PCRE development package for your Linux distribution (usually named pcre-devel or libpcre3-dev). If you are on a non-Linux platform or want to create a portable binary for Linux, it is possible to use the standard Go regexp package instead of PCRE by removing the "vendor" directory in HMB source. The standard Go regexp package may have a memory leak in its current version, so keep an eye on the memory consumption if you use it.

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
  SetInputFilter DEFLATE
  </Proxy>
