#!/bin/sh -e

cd "`dirname "$0"`"
export GOPATH="`pwd`/build"

compile() {
	echo "compiling $1"
	( cd "apps/go/src/$1" && go install )
}

mkdir -p build
compile httpmsgbus
compile hmbseedlink
compile wavefeed
mkdir -p ~/seiscomp/sbin
cp -p build/bin/* ~/seiscomp/sbin
mkdir -p ~/seiscomp/etc/init
cp -p apps/go/src/*/config/* ~/seiscomp/etc/init
mkdir -p ~/seiscomp/etc/descriptions
cp -p apps/go/src/*/descriptions/* ~/seiscomp/etc/descriptions
mkdir -p ~/seiscomp/lib/python/hmb
cp -p libs/python/hmb/* ~/seiscomp/lib/python/hmb
echo httpmsgbus successfully installed as ~/seiscomp/sbin/httpmsgbus

