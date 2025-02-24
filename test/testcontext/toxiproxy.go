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
	"fmt"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
)

const (
	smfProxyName           = "smf"
	compressedSmfProxyName = "compressedSmf"
	secureSmfProxyName     = "secureSmf"
)

// ToxiProxy interface
type ToxiProxy interface {
	Config() *ToxiProxyConfig
	GetClient() *toxiproxy.Client
	SMF() *toxiproxy.Proxy
	CompressedSMF() *toxiproxy.Proxy
	SecureSMF() *toxiproxy.Proxy
	SEMP() *toxiproxy.Proxy
	ResetProxies()
}

// ToxiProxyConfig structure
type ToxiProxyConfig struct {
	Upstream       string `json:"upstream,omitempty" env:"TOXIPROXY_UPSTREAM"`
	Host           string `json:"host,omitempty" env:"TOXIPROXY_HOST"`
	Port           int    `json:"port,omitempty" env:"TOXIPROXY_PORT"`
	PlaintextPort  int    `json:"plaintext_port,omitempty" env:"TOXIPROXY_PLAINTEXT_PORT"`
	CompressedPort int    `json:"compressed_port,omitempty" env:"TOXIPROXY_COMPRESSED_PORT"`
	SecurePort     int    `json:"secure_port,omitempty" env:"TOXIPROXY_SECURE_PORT"`
}

type toxiProxyImpl struct {
	config  *ToxiProxyConfig
	client  *toxiproxy.Client
	proxies map[string]*toxiproxy.Proxy
}

func newToxiProxy(config *ToxiProxyConfig) *toxiProxyImpl {
	return &toxiProxyImpl{
		config: config,
		client: toxiproxy.NewClient(fmt.Sprintf("%s:%d", config.Host, config.Port)),
	}
}

func (toxiProxy *toxiProxyImpl) setup() error {
	toxiProxy.proxies = make(map[string]*toxiproxy.Proxy)
	var err error
	toxiProxy.proxies[smfProxyName], err = toxiProxy.client.CreateProxy(
		smfProxyName,
		fmt.Sprintf(":%d", toxiProxy.config.PlaintextPort),
		fmt.Sprintf("%s:%d", toxiProxy.config.Upstream, 55555),
	)
	if err != nil {
		return err
	}
	toxiProxy.proxies[compressedSmfProxyName], err = toxiProxy.client.CreateProxy(
		compressedSmfProxyName,
		fmt.Sprintf(":%d", toxiProxy.config.CompressedPort),
		fmt.Sprintf("%s:%d", toxiProxy.config.Upstream, 55003),
	)
	if err != nil {
		return err
	}
	toxiProxy.proxies[secureSmfProxyName], err = toxiProxy.client.CreateProxy(
		secureSmfProxyName,
		fmt.Sprintf(":%d", toxiProxy.config.SecurePort),
		fmt.Sprintf("%s:%d", toxiProxy.config.Upstream, 55443),
	)
	if err != nil {
		return err
	}
	return nil
}

func (toxiProxy *toxiProxyImpl) teardown() error {
	for _, proxy := range toxiProxy.proxies {
		err := proxy.Delete()
		if err != nil {
			return err
		}
	}
	return nil
}

func (toxiProxy *toxiProxyImpl) ResetProxies() {
	for _, p := range toxiProxy.proxies {
		p.Delete()
	}
	toxiProxy.setup()
}

func (toxiProxy *toxiProxyImpl) Config() *ToxiProxyConfig {
	return toxiProxy.config
}

func (toxiProxy *toxiProxyImpl) GetClient() *toxiproxy.Client {
	return toxiProxy.client
}

func (toxiProxy *toxiProxyImpl) SMF() *toxiproxy.Proxy {
	return toxiProxy.proxies[smfProxyName]
}

func (toxiProxy *toxiProxyImpl) CompressedSMF() *toxiproxy.Proxy {
	return toxiProxy.proxies[compressedSmfProxyName]
}

func (toxiProxy *toxiProxyImpl) SecureSMF() *toxiproxy.Proxy {
	return toxiProxy.proxies[secureSmfProxyName]
}

func (toxiProxy *toxiProxyImpl) SEMP() *toxiproxy.Proxy {
	panic("not implemented") // TODO: Implement
}
