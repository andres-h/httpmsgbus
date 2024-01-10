module bitbucket.org/andresh/httpmsgbus/apps/go/src/httpmsgbus

go 1.21.5

replace bitbucket.org/andresh/httpmsgbus/apps/go/src/regexp => ../regexp

require (
	bitbucket.org/andresh/httpmsgbus/apps/go/src/regexp v0.0.0-00010101000000-000000000000
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/golang/protobuf v1.5.3
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
	gopkg.in/tylerb/graceful.v1 v1.2.15
)

require (
	github.com/glenn-brown/golang-pkg-pcre v0.0.0-20120522223659-48bb82a8b8ce // indirect
	golang.org/x/net v0.20.0 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
