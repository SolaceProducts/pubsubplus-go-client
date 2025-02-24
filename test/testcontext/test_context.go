// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package testcontext is the package containing different contexts
package testcontext

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

var defaultConfigPaths = []string{"./data/config/config.json"}

const customConfigEnv = "PUBSUB_IT_CONFIG"

var instance testContext

func init() {
	instance = getTestContext()
}

// Setup is a blocking call to set up the test context. For example, docker compose based
// contexts will be started with this function.
func Setup() error {
	return instance.Setup()
}

// Teardown is a blocking call to teardown the test context. For example, docker compose
// based contexts will be destroyed by this function.
func Teardown() error {
	return instance.Teardown()
}

// Messaging returns the connection details for connections to the backing broker for messaging
func Messaging() *MessagingConfig {
	return instance.Messaging()
}

// SEMP returns the SEMPv2 client entrypoint if available, or nil if SEMPv2 is unsupported (should never happen)
func SEMP() SEMPv2 {
	return instance.SEMPv2()
}

// Toxi returns a toxiproxy client if available, or nil if ToxiProxy is not available.
// ToxiProxy will not be available in certain test contexts and should always be checked for presence before executing.
// For example, when targeting an environment based broker such as an appliance, no toxi proxi will be available.
func Toxi() ToxiProxy {
	return instance.ToxiProxy()
}

// Kerberos returns a boolean indicating if kerberos is enabled
func Kerberos() bool {
	return instance.Kerberos()
}

// OAuth returns the OAuth config
func OAuth() *OAuthConfig {
	return instance.OAuth()
}

// function to wait for semp serivces
func WaitForSEMPReachable() error {
	return instance.WaitForSEMPReachable()
}

// testContext represents a test context
type testContext interface {
	// Setup is a blocking call to set up the test context. For example, docker compose based
	// contexts will be started with this function.
	Setup() error
	// Teardown is a blocking call to teardown the test context. For example, docker compose
	// based contexts will be destroyed by this function.
	Teardown() error
	// Messaging returns the connection details for connections to the backing broker for messaging
	Messaging() *MessagingConfig
	// Kerberos returns a boolean indicating if kerberos is enabled
	Kerberos() bool
	// SEMPv2 returns thre SEMPv2 client entrypoint if available, or nil if SEMPv2 is unsupported (should never happen)
	SEMPv2() SEMPv2
	// ToxiProxy returns a toxiproxy client if available, or nil if ToxiProxy is not available.
	// ToxiProxy will not be available in certain test contexts and should always be checked for presence before executing.
	// For example, when targeting an environment based broker such as an appliance, no toxi proxi will be available.
	ToxiProxy() ToxiProxy
	// OAuth returns the OAuth config
	OAuth() *OAuthConfig
	// waits for semp service to be reachable
	WaitForSEMPReachable() error
}

type testContextCommon struct {
	config          *TestConfig
	semp            *sempV2Impl
	toxi            *toxiProxyImpl
	kerberosEnabled bool
}

// GetConnectionDetails impl
func (context *testContextCommon) Messaging() *MessagingConfig {
	return context.config.Messaging
}

func (context *testContextCommon) SEMPv2() SEMPv2 {
	if context.semp == nil {
		return nil
	}
	return context.semp
}

func (context *testContextCommon) ToxiProxy() ToxiProxy {
	if context.toxi == nil {
		return nil
	}
	return context.toxi
}
func (context *testContextCommon) OAuth() *OAuthConfig {
	return context.config.OAuth
}

func (context *testContextCommon) Kerberos() bool {
	return context.kerberosEnabled
}

// loads the configs based on the given path
func (context *testContextCommon) setupCommon(configPath string) error {
	context.config = &TestConfig{}
	// load a default config first
	fmt.Printf("Loading common config files %s\n", defaultConfigPaths)
	for _, defaultConfigPath := range defaultConfigPaths {
		if err := context.config.loadConfig(defaultConfigPath); err != nil {
			return err
		}
	}
	// then load a specific config overriding whatever is necessary
	fmt.Printf("Loading config file for test strategy %s\n", configPath)
	if err := context.config.loadConfig(configPath); err != nil {
		return err
	}
	// then load in the specified config if applicable
	customConfig, ok := os.LookupEnv(customConfigEnv)
	if ok {
		fmt.Printf("Loading config file specified by %s, %s\n", customConfigEnv, customConfig)
		if err := context.config.loadConfig(customConfig); err != nil {
			return err
		}
	}
	// then load the environment variables
	if err := context.config.loadEnvironment(); err != nil {
		return err
	}
	if context.config.SEMP.Host == "" {
		fmt.Printf("No SEMP host provided, defaulting to messaging host %s\n", context.config.Messaging.Host)
		context.config.SEMP.Host = context.config.Messaging.Host
	}
	fmt.Printf("Loaded config:\n%s\n", context.config)
	return nil
}

// function to poll for a msgvpn state where expected values are "up", "down", or "standby"
// note state will be convert to lower case
func (context *testContextCommon) waitForVPNState(state string) error {
	//resp, httpResp, err := context.semp.Monitor().MsgVpnApi.GetMsgVpn(context.semp.MonitorCtx(), context.config.Messaging.VPN, nil)
	// resp.Data.State contains a string for operational state of the message vpn values are: "up", "down", or "standby"
	maxPollInterval := 500 * time.Millisecond
	pollInterval := 1 * time.Millisecond
	timeout := 300 * time.Second
	timeoutChannel := time.After(timeout)

	for {
		if pollInterval < maxPollInterval {
			pollInterval = pollInterval * 2
		} else {
			pollInterval = maxPollInterval
		}
		resp, _, err := context.semp.Monitor().MsgVpnApi.GetMsgVpn(context.semp.MonitorCtx(), context.config.Messaging.VPN, nil)
		if err == nil && resp.Data != nil {
			if remoteState := strings.ToLower(resp.Data.State); remoteState == strings.ToLower(state) {
				return nil
			}
		}
		select {
		case <-timeoutChannel:
			return fmt.Errorf("timed out waiting for vpn status", context.config.Messaging.VPN)
		case <-time.After(pollInterval):
			continue
		}
	}
}

func (context *testContextCommon) WaitForSEMPReachable() error {
	return context.waitForSEMP()
}

func (context *testContextCommon) waitForToxiProxy() error {
	err := waitForEndpoint(fmt.Sprintf("http://%s:%d", context.config.ToxiProxy.Host, context.config.ToxiProxy.Port), 404)
	if err != nil {
		return err
	}
	return nil
}

func (context *testContextCommon) waitForSEMP() error {
	err := waitForEndpoint(fmt.Sprintf("http://%s:%d/SEMP/v2/config/help/", context.config.SEMP.Host, context.config.SEMP.Port), 200)
	if err != nil {
		return err
	}
	return nil
}

func (context *testContextCommon) waitForMessaging() error {
	err := waitForEndpoint(fmt.Sprintf("http://%s:%d/health-check/direct-active", context.config.Messaging.Host, context.config.Messaging.MessagingPorts.HealthCheckPort), 200)
	if err != nil {
		return err
	}

	err = waitForEndpoint(fmt.Sprintf("http://%s:%d/health-check/guaranteed-active", context.config.Messaging.Host, context.config.Messaging.MessagingPorts.HealthCheckPort), 200)
	if err != nil {
		return err
	}
	return nil
}

func waitForEndpoint(endpoint string, expectedCode int) error {
	maxPollInterval := 500 * time.Millisecond
	pollInterval := 1 * time.Millisecond
	timeout := 300 * time.Second
	timeoutChannel := time.After(timeout)

	for {
		if pollInterval < maxPollInterval {
			pollInterval = pollInterval * 2
		} else {
			pollInterval = maxPollInterval
		}
		resp, err := http.Get(endpoint)
		if err == nil {
			if err := resp.Body.Close(); resp.StatusCode == expectedCode && err == nil {
				return nil
			}
		}
		select {
		case <-timeoutChannel:
			return fmt.Errorf("timed out waiting for %s", endpoint)
		case <-time.After(pollInterval):
			continue
		}
	}
}

// sempv1 sends the given data as a SEMPv1 request. the request will be wrapped in an rpc block
func (ctx testContextCommon) sempV1(data string) error {
	out := []byte("<rpc semp-version=\"soltr/9_8VMR\">" + data + "</rpc>")
	// Formulate request
	url := fmt.Sprintf("http://%s:%d/SEMP", ctx.config.SEMP.Host, ctx.config.SEMP.Port)
	req, err := http.NewRequest("POST", url, bytes.NewReader(out))
	if err != nil {
		return err
	}
	// Set headers
	req.SetBasicAuth(ctx.config.SEMP.Username, ctx.config.SEMP.Password)
	req.Header.Add("ContentType", "application/xml")
	// Exec
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("expected code %d, got %d", 200, resp.StatusCode)
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return err
	}
	respString := buf.String()
	if !strings.Contains(respString, "ok") {
		return fmt.Errorf("expected response to contain string ok: %s", respString)
	}
	return nil
}
