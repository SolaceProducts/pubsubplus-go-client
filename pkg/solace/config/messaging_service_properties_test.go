// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"solace.dev/go/messaging/pkg/solace/config"
)

var serviceProperties = config.ServicePropertyMap{
	config.AuthenticationPropertyScheme:              "basic",
	config.AuthenticationPropertySchemeBasicPassword: "default",
	config.TransportLayerPropertyHost:                "localhost",
	config.TransportLayerPropertyKeepAliveInterval:   float64(5),
}

var servicePropertiesJSON = `{"solace":{"messaging":{"authentication":{"basic":{"password":"default"},"scheme":"basic"},"transport":{"host":"localhost","keep-alive-interval":5}}}}`

func TestMessagingServicePropertiesCopy(t *testing.T) {
	myProperties := serviceProperties.GetConfiguration()
	myProperties[config.AuthenticationPropertyScheme] = "client"
	if serviceProperties[config.AuthenticationPropertyScheme] == "client" {
		t.Error("map was passed by reference, not copied on GetConfiguration")
	}
}
func TestMessagingServicePropertiesFromJSON(t *testing.T) {
	output := make(config.ServicePropertyMap)
	json.Unmarshal([]byte(servicePropertiesJSON), &output)
	for key, val := range output {
		expectedVal, ok := serviceProperties[key]
		if !ok {
			t.Errorf("did not expect key %s", key)
		}
		if expectedVal != val {
			t.Errorf("expected %s to equal %s", val, expectedVal)
		}
	}
	for key := range serviceProperties {
		_, ok := output[key]
		if !ok {
			t.Errorf("expected key %s to be present", key)
		}
	}
}

func TestMessagingServicePropertiesToJSON(t *testing.T) {
	output, err := json.Marshal(serviceProperties)
	if err != nil {
		t.Errorf("expected error to be nil, got %s", err)
	}
	if len(output) != len(servicePropertiesJSON) {
		t.Errorf("expected output '%s' to equal '%s'", output, servicePropertiesJSON)
	}
	for i, b := range output {
		if servicePropertiesJSON[i] != b {
			t.Errorf("expected output '%s' to equal '%s'", output, servicePropertiesJSON)
		}
	}
}

func TestPrintingIllegalValues(t *testing.T) {
	var obfuscatedProperties = []config.ServiceProperty{
		config.AuthenticationPropertySchemeBasicPassword,
		config.AuthenticationPropertySchemeClientCertPrivateKeyFilePassword,
		config.AuthenticationPropertySchemeOAuth2AccessToken,
		config.AuthenticationPropertySchemeOAuth2OIDCIDToken,
	}
	const value = "NotAllowed"
	for _, prop := range obfuscatedProperties {
		myPropMap := make(config.ServicePropertyMap)
		myPropMap[prop] = value
		output := fmt.Sprint(myPropMap)
		if strings.Contains(output, value) {
			t.Error("Expected output to not contain " + value + ", got " + output)
		}
	}
}
