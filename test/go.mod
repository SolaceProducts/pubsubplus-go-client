module solace.dev/go/messaging/test

go 1.17

require (
	github.com/Microsoft/go-winio v0.5.2 // indirect
	github.com/Shopify/toxiproxy/v2 v2.3.0
	github.com/containerd/containerd v1.6.1 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/docker v20.10.12+incompatible // indirect
	github.com/google/uuid v1.3.0
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/moby/sys/mount v0.3.1 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/onsi/ginkgo/v2 v2.1.3
	github.com/onsi/gomega v1.18.1
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/testcontainers/testcontainers-go v0.12.0
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f // indirect
	golang.org/x/sys v0.0.0-20220227234510-4e6760a101f9 // indirect
	google.golang.org/genproto v0.0.0-20220304144024-325a89244dc8 // indirect
	solace.dev/go/messaging v0.0.0
	solace.dev/go/messaging/test/sempclient/action v0.0.0
	solace.dev/go/messaging/test/sempclient/config v0.0.0
	solace.dev/go/messaging/test/sempclient/monitor v0.0.0
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Microsoft/hcsshim v0.9.2 // indirect
	github.com/antihax/optional v1.0.0 // indirect
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/containerd/cgroups v1.0.3 // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/moby/sys/mountinfo v0.6.0 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/runc v1.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/oauth2 v0.0.0-20220223155221-ee480838109b // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/grpc v1.44.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace solace.dev/go/messaging v0.0.0 => ../

replace solace.dev/go/messaging/test/sempclient/action v0.0.0 => ./sempclient/action

replace solace.dev/go/messaging/test/sempclient/config v0.0.0 => ./sempclient/config

replace solace.dev/go/messaging/test/sempclient/monitor v0.0.0 => ./sempclient/monitor
