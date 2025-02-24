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

package provisioner

import (
	"testing"

	"solace.dev/go/messaging/internal/impl/core"
)

func TestEndpointProvisionerBuilderWithInvalidDurabilityType(t *testing.T) {
	provisioner := NewEndpointProvisionerImpl(&mockInternalEndpointProvisioner{})
	provisioner.WithDurability(false) // We do not support provisioning non-durable endpoints
	outcome := provisioner.Provision("hello", true)
	if !outcome.GetStatus() || outcome.GetError() != nil {
		t.Error("Did not expected error while setting endpoint durability to false. Error should be on provision action")
	}
}

func TestEndpointProvisionerBuilderWithInvalidMaxMessageRedeliveryRange(t *testing.T) {
	provisioner := NewEndpointProvisionerImpl(&mockInternalEndpointProvisioner{})
	provisioner.WithMaxMessageRedelivery(256) // valid message redelivery range is from 0 - 255
	outcome := provisioner.Provision("hello", true)
	if outcome.GetStatus() || outcome.GetError() == nil {
		t.Error("Expected error while provisioning queue with out of range max message redelivery")
	}
}

func TestEndpointProvisionerBuilderWithValidZeroMaxMessageRedelivery(t *testing.T) {
	provisioner := NewEndpointProvisionerImpl(&mockInternalEndpointProvisioner{})
	provisioner.WithMaxMessageRedelivery(0) // zero message redelivery (Min value supported)
	outcome := provisioner.Provision("hello", true)
	if !outcome.GetStatus() || outcome.GetError() != nil {
		t.Error("Did not expect error while provisioning queue with valid message redelivery. Error: ", outcome.GetError())
	}
}

func TestEndpointProvisionerBuilderWithValidMaxMessageRedelivery(t *testing.T) {
	provisioner := NewEndpointProvisionerImpl(&mockInternalEndpointProvisioner{})
	provisioner.WithMaxMessageRedelivery(255) // valid message redelivery count (Max value supported)
	outcome := provisioner.Provision("hello", true)
	if !outcome.GetStatus() || outcome.GetError() != nil {
		t.Error("Did not expect error while provisioning queue with valid message redelivery. Error: ", outcome.GetError())
	}
}

func TestEndpointProvisionerBuilderWithValidZeroMaxMessageSize(t *testing.T) {
	provisioner := NewEndpointProvisionerImpl(&mockInternalEndpointProvisioner{})
	provisioner.WithMaxMessageSize(0) // valid zero message size (Min value supported)
	outcome := provisioner.Provision("hello", true)
	if !outcome.GetStatus() || outcome.GetError() != nil {
		t.Error("Did not expect error while provisioning queue with valid max message size")
	}
}

func TestEndpointProvisionerBuilderWithValidZeroQuotaMB(t *testing.T) {
	provisioner := NewEndpointProvisionerImpl(&mockInternalEndpointProvisioner{})
	provisioner.WithQuotaMB(0) // valid zero queue quota size (Min value supported)
	outcome := provisioner.Provision("hello", true)
	if !outcome.GetStatus() || outcome.GetError() != nil {
		t.Error("Did not expect error while provisioning queue with valid queue quota size")
	}
}

type mockInternalEndpointProvisioner struct {
	events                    func() core.Events
	isRunning                 func() bool
	provision                 func(properties []string, ignoreExistErrors bool) (core.ProvisionCorrelationID, <-chan core.ProvisionEvent, core.ErrorInfo)
	deprovision               func(properties []string, ignoreMissingErrors bool) (core.ProvisionCorrelationID, <-chan core.ProvisionEvent, core.ErrorInfo)
	clearProvisionCorrelation func(id core.ProvisionCorrelationID)
}

func (mock *mockInternalEndpointProvisioner) Events() core.Events {
	if mock.events != nil {
		return mock.events()
	}
	return &mockEvents{}
}

func (mock *mockInternalEndpointProvisioner) IsRunning() bool {
	if mock.isRunning != nil {
		return mock.isRunning()
	}
	return true
}

// Mock Provision endpoint function
func (mock *mockInternalEndpointProvisioner) Provision(properties []string, ignoreExistErrors bool) (core.ProvisionCorrelationID, <-chan core.ProvisionEvent, core.ErrorInfo) {
	if mock.provision != nil {
		return mock.provision(properties, ignoreExistErrors)
	}
	// used to simulate a provision outcome result
	provisionOutcomeChannel := make(chan core.ProvisionEvent, 1)
	provisionOutcomeChannel <- &provisionEvent{
		id:  0,
		err: nil,
	}
	return 0, provisionOutcomeChannel, nil
}

// Mock Deprovision endpoint function
func (mock *mockInternalEndpointProvisioner) Deprovision(properties []string, ignoreMissingErrors bool) (core.ProvisionCorrelationID, <-chan core.ProvisionEvent, core.ErrorInfo) {
	if mock.deprovision != nil {
		return mock.deprovision(properties, ignoreMissingErrors)
	}
	// used to simulate a deprovision outcome result
	deprovisionOutcomeChannel := make(chan core.ProvisionEvent, 1)
	deprovisionOutcomeChannel <- &provisionEvent{
		id:  0,
		err: nil,
	}
	return 0, deprovisionOutcomeChannel, nil
}

func (mock *mockInternalEndpointProvisioner) ClearProvisionCorrelation(id core.ProvisionCorrelationID) {
	if mock.clearProvisionCorrelation != nil {
		mock.clearProvisionCorrelation(id)
	}
}

type mockEvents struct {
}

func (events *mockEvents) AddEventHandler(sessionEvent core.Event, responseCode core.EventHandler) uint {
	return 0
}
func (events *mockEvents) RemoveEventHandler(id uint) {
}

// provisionEvent
type provisionEvent struct {
	id  core.ProvisionCorrelationID
	err error
}

func (event *provisionEvent) GetID() core.ProvisionCorrelationID {
	return event.id
}

func (event *provisionEvent) GetError() error {
	return event.err
}
