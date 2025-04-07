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
	"testing"

	"solace.dev/go/messaging/pkg/solace/config"
)

var endpointProperties = config.EndpointPropertyMap{
	config.EndpointPropertyDurable:              true,
	config.EndpointPropertyExclusive:            true,
	config.EndpointPropertyMaxMessageRedelivery: uint64(5),
	config.EndpointPropertyMaxMessageSize:       uint64(10000000), // queue default on broker
	config.EndpointPropertyNotifySender:         true,
	config.EndpointPropertyQuotaMB:              uint64(5000), // 5000MB
	config.EndpointPropertyRespectsTTL:          true,
	config.EndpointPropertyPermission:           config.EndpointPermissionModifyTopic, // permission to modify topic subscriptions
}

var endpointPropertiesJSON = `{"solace":{"messaging":{"endpoint-property":{"durable":true,"exclusive":true,"max-message-redelivery":5,"max-message-size":10000000,"notify-sender":true,"permission":"solace.messaging.endpoint-permission.modify-topic","quota-mb":5000,"respects-ttl":true}}}}`

func TestEndpointPropertiesCopy(t *testing.T) {
	myProperties := endpointProperties.GetConfiguration()
	myProperties[config.EndpointPropertyPermission] = config.EndpointPermissionNone // no permission
	if endpointProperties[config.EndpointPropertyPermission] == config.EndpointPermissionNone {
		t.Error("map was passed by reference, not copied on GetConfiguration")
	}
}
func TestEndpointPropertiesFromJSON(t *testing.T) {
	output := make(config.EndpointPropertyMap)
	json.Unmarshal([]byte(endpointPropertiesJSON), &output)
	for key, val := range output {
		expectedVal, ok := endpointProperties[key]
		if !ok {
			t.Errorf("did not expect key %s", key)
		}
		switch v := val.(type) {
		case float64: // the default unmarshal type for numbers is float64
			val = uint64(v)
		}
		if fmt.Sprintf("%v", expectedVal) != fmt.Sprintf("%v", val) {
			t.Errorf("expected %s to equal %s", val, expectedVal)
		}
	}
	for key := range endpointProperties {
		_, ok := output[key]
		if !ok {
			t.Errorf("expected key %s to be present", key)
		}
	}
}

func TestEndpointPropertiesToJSON(t *testing.T) {
	output, err := json.Marshal(endpointProperties)
	if err != nil {
		t.Errorf("expected error to be nil, got %s", err)
	}
	if len(output) != len(endpointPropertiesJSON) {
		t.Errorf("expected output '%s' to equal '%s'", output, endpointPropertiesJSON)
	}
	for i, b := range output {
		if endpointPropertiesJSON[i] != b {
			t.Errorf("expected output '%s' to equal '%s'", output, endpointPropertiesJSON)
		}
	}
}
