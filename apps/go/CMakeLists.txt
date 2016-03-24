SET(CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/)
PROJECT(HTTPMSGBUS Go)

ADD_CUSTOM_TARGET(
    HMBDEPS
    COMMAND GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} get github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre
    COMMAND GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} get github.com/golang/groupcache/lru
    COMMAND GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} get github.com/golang/protobuf/proto
    COMMAND GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} get gopkg.in/mgo.v2
    COMMAND GOPATH=${GOPATH} ${CMAKE_Go_COMPILER} get gopkg.in/tylerb/graceful.v1
)

SUBDIRS(src)

