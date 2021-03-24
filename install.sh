#!/bin/sh -e

# Standalone install script as an alternative to CMake

cd "`dirname "$0"`"
export GOPATH="`pwd`/build:`pwd`/apps/go"

get() {
	echo "downloading $1"
	go get -u "$1"
}

mkdir -p build
get github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre
get github.com/golang/groupcache/lru
get github.com/golang/protobuf/proto
get gopkg.in/mgo.v2
get gopkg.in/tylerb/graceful.v1
echo "compiling the packages"
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

