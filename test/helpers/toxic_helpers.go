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

package helpers

import (
	"fmt"

	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/test/testcontext"

	//lint:ignore ST1001 dot import is fine for tests
	. "github.com/onsi/ginkgo/v2"
)

// ToxicConfiguration function
func ToxicConfiguration() config.ServicePropertyMap {
	if testcontext.Toxi() == testcontext.ToxiProxy(nil) {
		return nil
	}
	connectionDetails := testcontext.Messaging()
	toxiProxyConfig := testcontext.Toxi().Config()
	url := fmt.Sprintf("%s:%d", toxiProxyConfig.Host, toxiProxyConfig.PlaintextPort)
	config := config.ServicePropertyMap{
		config.ServicePropertyVPNName:                    connectionDetails.VPN,
		config.TransportLayerPropertyHost:                url,
		config.AuthenticationPropertySchemeBasicUserName: connectionDetails.Authentication.BasicUsername,
		config.AuthenticationPropertySchemeBasicPassword: connectionDetails.Authentication.BasicPassword,
	}
	return config
}

// CheckToxiProxy function
func CheckToxiProxy() {
	if testcontext.Toxi() == nil {
		Skip("No toxiproxy found")
	}
}
