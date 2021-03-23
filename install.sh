#!/bin/sh -e

# Standalone install script as an alternative to CMake

cd "`dirname "$0"`"
mkdir -p "`pwd`/build"
export GOPATH="`pwd`/build:`pwd`/apps/go"

cd "`pwd`"
go get -u github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre
go get -u github.com/golang/groupcache/lru
go get -u github.com/golang/protobuf/proto
go get -u gopkg.in/mgo.v2
go get -u gopkg.in/tylerb/graceful.v1
go install httpmsgbus hmbseedlink wavefeed
mkdir -p ~/seiscomp/sbin
cp -p apps/go/bin/* ~/seiscomp/sbin
mkdir -p ~/seiscomp/etc/init
cp -p apps/go/src/*/config/* ~/seiscomp/etc/init
mkdir -p ~/seiscomp/etc/descriptions
cp -p apps/go/src/*/descriptions/* ~/seiscomp/etc/descriptions
mkdir -p ~/seiscomp/lib/python/hmb
cp -p libs/python/hmb/* ~/seiscomp/lib/python/hmb
echo httpmsgbus successfully installed as ~/seiscomp/sbin/httpmsgbus

