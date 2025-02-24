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

package testcontext

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	"solace.dev/go/messaging/test/sempclient/action"
	"solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/sempclient/monitor"
)

// SEMPConfig structure
type SEMPConfig struct {
	Port       int    `json:"port" env:"PUBSUB_SEMP_PORT"`
	Secure     int    `json:"secure" env:"PUBSUB_SECURE_SEMP_PORT"`
	Host       string `json:"host" env:"PUBSUB_MANAGEMENT_HOST"`
	Username   string `json:"username" env:"PUBSUB_MANAGEMENT_USER"`
	Password   string `json:"password" env:"PUBSUB_MANAGEMENT_PASSWORD"`
	ForcePlain bool   `json:"force_plain,omitempty" env:"PUBSUB_SEMP_FORCE_PLAIN"`
}

// SEMPv2 allows access to various SEMPv2 clients as well as their authentication parameters
type SEMPv2 interface {
	// ActionCtx returns the context to use when making action API calls
	ActionCtx() context.Context
	// Action returns the action API client entrypoint
	Action() *action.APIClient
	// Auth returns the auth to use when making API calls
	ConfigCtx() context.Context
	// Config returns the config API client entrypoint
	Config() *config.APIClient
	// Auth returns the auth to use when making API calls
	MonitorCtx() context.Context
	// Monitor returns the monitor API client entrypoint
	Monitor() *monitor.APIClient
}

// internal implementation of SEMPv2 to use
type sempV2Impl struct {
	config *SEMPConfig

	actionCtx     context.Context
	actionClient  *action.APIClient
	configCtx     context.Context
	configClient  *config.APIClient
	monitorCtx    context.Context
	monitorClient *monitor.APIClient
}

func newSempV2(config *SEMPConfig) *sempV2Impl {
	return &sempV2Impl{
		config: config,
	}
}

// common setup functionality that instantiates sempv2 clients based on a set of connection details
func (semp *sempV2Impl) setup() error {
	// Create a new HTTP client to use with SEMP that skips validation
	// This lets us use SEMP over TLS allowing for server certificate configuration
	httpClient := http.DefaultClient
	url := fmt.Sprintf("http://%s:%d/SEMP/v2/", semp.config.Host, semp.config.Port)
	if !semp.config.ForcePlain {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		httpClient = &http.Client{Transport: tr}
		url = fmt.Sprintf("https://%s:%d/SEMP/v2/", semp.config.Host, semp.config.Secure)
	}
	semp.actionCtx = context.WithValue(context.Background(), action.ContextBasicAuth, action.BasicAuth{
		UserName: semp.config.Username,
		Password: semp.config.Password,
	})
	semp.actionClient = action.NewAPIClient(&action.Configuration{
		BasePath:   url + "action",
		HTTPClient: httpClient,
	})
	semp.configCtx = context.WithValue(context.Background(), config.ContextBasicAuth, config.BasicAuth{
		UserName: semp.config.Username,
		Password: semp.config.Password,
	})
	semp.configClient = config.NewAPIClient(&config.Configuration{
		BasePath:   url + "config",
		HTTPClient: httpClient,
	})
	semp.monitorCtx = context.WithValue(context.Background(), monitor.ContextBasicAuth, monitor.BasicAuth{
		UserName: semp.config.Username,
		Password: semp.config.Password,
	})
	semp.monitorClient = monitor.NewAPIClient(&monitor.Configuration{
		BasePath:   url + "monitor",
		HTTPClient: httpClient,
	})
	return nil
}

func (semp *sempV2Impl) ActionCtx() context.Context {
	return semp.actionCtx
}

func (semp *sempV2Impl) Action() *action.APIClient {
	return semp.actionClient
}

func (semp *sempV2Impl) ConfigCtx() context.Context {
	return semp.configCtx
}

func (semp *sempV2Impl) Config() *config.APIClient {
	return semp.configClient
}

func (semp *sempV2Impl) MonitorCtx() context.Context {
	return semp.monitorCtx
}

func (semp *sempV2Impl) Monitor() *monitor.APIClient {
	return semp.monitorClient
}
