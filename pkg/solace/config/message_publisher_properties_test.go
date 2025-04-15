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
	"testing"

	"solace.dev/go/messaging/pkg/solace/config"
)

var publisherProperties = config.PublisherPropertyMap{
	config.PublisherPropertyBackPressureBufferCapacity: float64(10),
	config.PublisherPropertyBackPressureStrategy:       "default",
}

var publisherPropertiesJSON = `{"solace":{"messaging":{"publisher":{"back-pressure":{"buffer-capacity":10,"strategy":"default"}}}}}`

func TestMessagePublisherPropertiesCopy(t *testing.T) {
	myProperties := publisherProperties.GetConfiguration()
	myProperties[config.PublisherPropertyBackPressureStrategy] = "test"
	if publisherProperties[config.PublisherPropertyBackPressureStrategy] == "test" {
		t.Error("map was passed by reference, not copied on GetConfiguration")
	}
}
func TestMessagePublisherPropertiesFromJSON(t *testing.T) {
	output := make(config.PublisherPropertyMap)
	json.Unmarshal([]byte(publisherPropertiesJSON), &output)
	for key, val := range output {
		expectedVal, ok := publisherProperties[key]
		if !ok {
			t.Errorf("did not expect key %s", key)
		}
		if expectedVal != val {
			t.Errorf("expected %s to equal %s", val, expectedVal)
		}
	}
	for key := range publisherProperties {
		_, ok := output[key]
		if !ok {
			t.Errorf("expected key %s to be present", key)
		}
	}
}

func TestMessagePublisherPropertiesToJSON(t *testing.T) {
	output, err := json.Marshal(publisherProperties)
	if err != nil {
		t.Errorf("expected error to be nil, got %s", err)
	}
	if len(output) != len(publisherPropertiesJSON) {
		t.Errorf("expected output '%s' to equal '%s'", output, publisherPropertiesJSON)
	}
	for i, b := range output {
		if publisherPropertiesJSON[i] != b {
			t.Errorf("expected output '%s' to equal '%s'", output, publisherPropertiesJSON)
		}
	}
}
