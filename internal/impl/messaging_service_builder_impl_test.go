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

package impl

import (
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/core"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
)

func TestMessagingServiceGC(t *testing.T) {
	messagingServiceCleanedUp := make(chan struct{})
	transportCleanedUp := make(chan struct{})
	{
		service, err := NewMessagingServiceBuilder().FromConfigurationProvider(config.ServicePropertyMap{
			config.AuthenticationPropertySchemeBasicUserName: "hello",
			config.AuthenticationPropertySchemeBasicPassword: "world",
			config.ServicePropertyVPNName:                    "default",
			config.TransportLayerPropertyHost:                "localhost",
		}).Build()
		if err != nil {
			t.Error(err)
		}
		runtime.SetFinalizer(service, func(service solace.MessagingService) {
			close(messagingServiceCleanedUp)
		})
		serviceImpl, ok := service.(*messagingServiceImpl)
		if !ok {
			t.Error("did not get service impl back")
		}
		runtime.SetFinalizer(serviceImpl.transport, nil)
		runtime.SetFinalizer(serviceImpl.transport, func(transport core.Transport) {
			close(transportCleanedUp)
		})
	}
	attempts := 0
	maxAttempts := 5
	for {
		attempts++
		if attempts > maxAttempts {
			t.Error("ran out of GC attempts waiting for finalizers to be called")
			break
		}
		runtime.GC()
		select {
		case <-messagingServiceCleanedUp:
			// success
		case <-time.After(10 * time.Millisecond):
			continue
		}
		select {
		case <-transportCleanedUp:
			// success
		case <-time.After(10 * time.Millisecond):
			continue
		}
		break
	}

}

func TestBooleanConverter(t *testing.T) {
	assertEquals := func(actual, expected string) {
		if actual != expected {
			t.Errorf("expected %s to equal %s", actual, expected)
		}
	}
	// integers
	assertEquals(ccsmp.SolClientPropEnableVal, booleanConverter(1))
	assertEquals(ccsmp.SolClientPropEnableVal, booleanConverter(100))
	assertEquals(ccsmp.SolClientPropDisableVal, booleanConverter(0))
	// strings
	assertEquals(ccsmp.SolClientPropEnableVal, booleanConverter("true"))
	assertEquals(ccsmp.SolClientPropDisableVal, booleanConverter("false"))
	assertEquals(ccsmp.SolClientPropDisableVal, booleanConverter("asdf"))
	// booleans
	assertEquals(ccsmp.SolClientPropEnableVal, booleanConverter(true))
	assertEquals(ccsmp.SolClientPropDisableVal, booleanConverter(false))
	// loaded from JSON
	jsonToTest := []struct {
		json     string
		expected string
	}{
		{`{"val":1}`, ccsmp.SolClientPropEnableVal},
		{`{"val":0}`, ccsmp.SolClientPropDisableVal},
		{`{"val":true}`, ccsmp.SolClientPropEnableVal},
		{`{"val":false}`, ccsmp.SolClientPropDisableVal},
		{`{"val":null}`, ccsmp.SolClientPropDisableVal},
		{`{"val":"true"}`, ccsmp.SolClientPropEnableVal},
		{`{"val":"false"}`, ccsmp.SolClientPropDisableVal},
	}
	for _, testCase := range jsonToTest {
		structure := struct {
			Val interface{} `json:"val"`
		}{}
		json.Unmarshal([]byte(testCase.json), &structure)
		assertEquals(testCase.expected, booleanConverter(structure.Val))
	}

}
