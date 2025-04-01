# Tests

The integration tests are stored in this directory. These integration tests are driven by a test context. The test context is made up of a few parts: connection details used to populate a messaging service configuration and management tools to interact with external resources such as the broker, toxi proxi, auth servers, etc. Most of the testing is done using a local docker-compose based broker, however this is abstracted out via a test context. The test context allows multiple configurations to be used for various test scenarios, as well as provides access to optional features such as Toxi Proxi. Currently, two test contexts are used:
- docker-compose based test context using testcontainers to spin up a broker and an instance of toxi proxi
- environment based test context that reads in various environment variables to configure access to a broker

*Note:* the SEMPv2 clients' generated code is not committed. To run the tests, run `go generate` from the `./test/sempclient` package. This will take a few minutes.

## Tooling

To reliably retrieve packaging, use a go proxy.  Be sure to set in your environment:
- export GOPROXY=https://proxy.golang.org,direct
- go env -w GOPROXY=https://proxy.golang.org,direct

[Ginkgo](https://onsi.github.io/ginkgo/) is used as the testing framework with [Gomega](https://onsi.github.io/gomega/) as the assert library. The tests also define a `./test/testcontext` package that is stood up before the suite and torn down after the suite is run. This test context is accessible through the CurrentContext variable defined in the root of the test package.
- go install github.com/onsi/ginkgo/v2/ginkgo@v2.1.3

## Running the tests

First, docker and docker-compose must be installed and accessible to the current user. Second, `go generate` must be run from the `./test/sempclient` directory. The tests can be run by navigating to the test directory and running `ginkgo` or 'go test'. 'ginkgo' is preferable as the pre-configured timeout (for the whole test suite) is 1hr whereas the default timeout using 'go test' is 10mins. This will start by spinning up a docker container containing the broker and then running the tests on that container. To run against an external broker, `go test -tags remote` can be used to instruct the tests to target an environment variable based broker. See the [Environment Variables](#environment-variables) section below for more information.

To run an individual test, first install the ginkgo command line tool with `go install github.com/onsi/ginkgo/v2/ginkgo@latest`, then run the tests with `ginkgo --focus="mytestregex"` from the test directory. This regex will match describe/context/it strings. For more information, see the [ginkgo documentation](https://onsi.github.io/ginkgo/#the-spec-runner).

## Coverage
Golang provides built in coverage reporting for test suites. A coverage report can be generated with `go test -coverprofile coverage.out -coverpkg solace.dev/go/messaging/internal/...,solace.dev/go/messaging/pkg/...`. This profile can be viewed as HTML with `go tool cover -html coverage.out`.

## Debug Logging
Debug logs can be enabled on the integration tests by adding the build tag `-tags enable_debug_logging` to the `go test` or `ginkgo` command. By default, the log level is set to Info.

## Environment Variables
Various environment variables can be used to configure the test context. If using an environment based test context (`-tags environment` on the call to `go test`), then `PUBSUB_HOST` (and `PUBSUB_MANAGEMENT_HOST` if it differs from the messaging host) must be specified as the IP or hostname of the broker to which you are connecting. The following variables are shared between all test contexts, and are used as parameters when using a locally hosted context such as testcontainers:
- `PUBSUB_PORT_PLAINTEXT`
- `PUBSUB_PORT_SSL`
- `PUBSUB_PORT_COMPRESSED`
- `PUBSUB_PORT_SEMP`
- `PUBSUB_VPN`
- `PUBSUB_USERNAME`
- `PUBSUB_MGMT_USER`
- `PUBSUB_MGMT_PASSWORD`

### Toxi Proxi
The broker is often accessed through [toxiproxi](https://github.com/Shopify/toxiproxy) to test various edge cases that rely on network saturation, latency, disconnects, etc. Toxiproxi, which is stood up in docker-compose based test contexts, can be controlled through the golang [client](https://github.com/Shopify/toxiproxy/tree/master/client).

### Kerberos
Kerberos tests can be run when using testcontainers by exporting the variable KRB_TEST_IMAGE with a valid Kerberos server image.

### OAuth
OAuth tests can be run when using testcontainers by exporting the variable OAUTH_TEST_IMAGE with a valid OAuth server image.

## Generated SEMPv2 client
In addition to the above generated code, we generate a SEMPv2 client based on the SEMPv2 spec using the [OpenAPI generator](https://github.com/OpenAPITools/openapi-generator) that is used in tests in order to configure remote resources. To generate the SEMPv2 client, Docker must be installed. Navigate to `./test/sempclient` and run `go generate`. This will generate the config, action and monitor APIs. See `./test/sempclient/semp-client.go` for the various generate directives.

### Updating the SEMPv2 client spec
Config, action and monitor specs can be added to `./test/sempclient/spec` and the generator rerun to update the SEMPv2 specs.
