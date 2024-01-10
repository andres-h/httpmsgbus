#!/bin/sh

latest="`curl -Ls 'https://go.dev/dl/?mode=json' | python -c 'import sys; import json; sys.stdout.write(json.load(sys.stdin)[0]["version"])'`"

curl -Ls "https://go.dev/dl/$latest.linux-amd64.tar.gz" | tar -C /usr/local -xzf -

