#!/bin/sh -e

# Standalone install script as an alternative to CMake

cd "`dirname "$0"`"
export GOPATH="`pwd`/apps/go"

cd "$GOPATH/src"
go get github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre
go get github.com/golang/groupcache/lru
go get github.com/golang/protobuf/proto
go get gopkg.in/mgo.v2
go get gopkg.in/tylerb/graceful.v1
go install httpmsgbus hmbseedlink wavefeed
mkdir -p ~/seiscomp3/sbin
cp -p ../bin/* ~/seiscomp3/sbin
mkdir -p ~/seiscomp3/etc/init
cp -p */config/* ~/seiscomp3/etc/init
mkdir -p ~/seiscomp3/etc/descriptions
cp -p */descriptions/* ~/seiscomp3/etc/descriptions
mkdir -p ~/seiscomp3/lib/python/hmb
cp -p ../../../libs/python/hmb/* ~/seiscomp3/lib/python/hmb
echo httpmsgbus successfully installed as ~/seiscomp3/sbin/httpmsgbus

