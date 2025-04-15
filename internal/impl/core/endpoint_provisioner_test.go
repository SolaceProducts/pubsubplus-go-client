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

package core

import (
	"fmt"
	"testing"
	"unsafe"
)

// TestSolClientEndpointProvisionerTerminate - tests that start() adds the event provision/deprovision handlers
func TestSolClientEndpointProvisionerStart(t *testing.T) {
	events := dummyEvents()
	endpointProvisioner := newCcsmpEndpointProvisioner(nil, events)
	if endpointProvisioner.isRunning == 1 {
		t.Error("endpointProvisioner should not have started")
	}
	if endpointProvisioner.provisionOkEvent > 0 {
		t.Error("provisionOkEvent handler should not have been added before starting endpoint provisioner")
	}
	if endpointProvisioner.provisionErrorEvent > 0 {
		t.Error("provisionErrorEvent handler should not have been added before starting endpoint provisioner")
	}
	endpointProvisioner.start()
	if endpointProvisioner.isRunning == 0 {
		t.Error("endpointProvisioner should have started")
	}
	eventHandlers, ok := events.eventHandlers[SolClientProvisionOk]
	if ok && len(eventHandlers) == 0 {
		t.Error("provisionOkEvent handler should have been added after starting endpoint provisioner")
	}
	eventHandlers, ok = events.eventHandlers[SolClientProvisionError]
	if ok && len(eventHandlers) == 0 {
		t.Error("provisionErrorEvent handler should have been added after starting endpoint provisioner")
	}
}

// TestSolClientEndpointProvisionerTerminate - tests that terminate() removes the event provision/deprovision handlers
func TestSolClientEndpointProvisionerTerminate(t *testing.T) {
	events := dummyEvents()
	endpointProvisioner := newCcsmpEndpointProvisioner(nil, events)
	endpointProvisioner.start() // start the provisioner
	if !endpointProvisioner.IsRunning() {
		t.Error("endpointProvisioner should have started")
	}
	endpointProvisioner.terminate() // terminate the provisioner
	if endpointProvisioner.isRunning == 1 {
		t.Error("endpointProvisioner should have been terminate after calling terminate() on endpoint provisioner")
	}
	eventHandlers, ok := events.eventHandlers[SolClientProvisionOk]
	if ok && len(eventHandlers) > 0 {
		t.Error("provisionOkEvent handler should have been removed after calling terminate() on endpoint provisioner")
	}
	eventHandlers, ok = events.eventHandlers[SolClientProvisionError]
	if ok && len(eventHandlers) > 0 {
		t.Error("provisionErrorEvent handler should have been removed after calling terminate() on endpoint provisioner")
	}
}

// TestSolClientEndpointProvisionerGetNewProvisionCorrelation - tests that a new provision correlation is generated and available for use
// Mock that the getNewProvisionCorrelation returns the channel and Id
func TestSolClientEndpointProvisionerGetNewProvisionCorrelation(t *testing.T) {
	events := dummyEvents()
	endpointProvisioner := newCcsmpEndpointProvisioner(nil, events)
	provisionCorrelationMap := endpointProvisioner.provisionCorrelation

	if len(provisionCorrelationMap) > 0 {
		t.Error("We should not have any correlationID in the provision correlation map before calling getNewProvisionCorrelation()")
	}
	correlationID, channel := endpointProvisioner.getNewProvisionCorrelation()
	if len(provisionCorrelationMap) == 0 {
		t.Error("A correlation entry should have been added to the provision correlation map after calling getNewProvisionCorrelation()")
	}
	resultChan, ok := provisionCorrelationMap[correlationID]
	if !ok || (resultChan != channel) {
		t.Error("The added correlation entry should map what was returned by the getNewProvisionCorrelation()")
	}
	endpointProvisioner.getNewProvisionCorrelation() // second call
	if len(provisionCorrelationMap) != 2 {
		t.Error("Should have two correlation entries in the provision correlation map after calling getNewProvisionCorrelation() twice")
	}
}

// TestSolClientEndpointProvisionerClearProvisionCorrelation - tests that the provision correlation is cleared from the correlation map
// Mock that the ClearProvisionCorrelation clears the channel and Id
func TestSolClientEndpointProvisionerClearProvisionCorrelation(t *testing.T) {
	events := dummyEvents()
	endpointProvisioner := newCcsmpEndpointProvisioner(nil, events)
	provisionCorrelationMap := endpointProvisioner.provisionCorrelation

	if len(provisionCorrelationMap) > 0 {
		t.Error("We should not have any correlationID in the provision correlation map before calling getNewProvisionCorrelation()")
	}
	correlationID1, _ := endpointProvisioner.getNewProvisionCorrelation()
	if len(provisionCorrelationMap) == 0 {
		t.Error("A correlation entry should have been added to the provision correlation map after calling getNewProvisionCorrelation()")
	}
	correlationID2, channel2 := endpointProvisioner.getNewProvisionCorrelation()
	if len(provisionCorrelationMap) != 2 {
		t.Error("Should have two correlation entries in the provision correlation map after calling getNewProvisionCorrelation() twice")
	}
	correlationID3, _ := endpointProvisioner.getNewProvisionCorrelation()
	if len(provisionCorrelationMap) != 3 {
		t.Error("Should have three correlation entries in the provision correlation map after third call to getNewProvisionCorrelation()")
	}

	resultChan, ok := provisionCorrelationMap[correlationID2]
	if !ok || (resultChan != channel2) {
		t.Error("The provision correlation map should contain the added correlation entry (correlationID2)")
	}

	endpointProvisioner.ClearProvisionCorrelation(correlationID2) // clear correlation from map
	_, ok = provisionCorrelationMap[correlationID2]
	if ok {
		t.Error("The provision correlation map should not contain the correlation entry (correlationID2) after calling ClearProvisionCorrelation()")
	}
	if len(provisionCorrelationMap) > 2 {
		t.Error("Should have two correlation entries in the correlation map since one entry was cleared")
	}

	endpointProvisioner.ClearProvisionCorrelation(correlationID3) // clear correlation from map
	_, ok = provisionCorrelationMap[correlationID3]
	if ok {
		t.Error("The provision correlation map should not contain the correlation entry (correlationID3) after calling ClearProvisionCorrelation()")
	}
	if len(provisionCorrelationMap) > 1 {
		t.Error("Should have one correlation entry left in the correlation map")
	}

	endpointProvisioner.ClearProvisionCorrelation(correlationID1) // clear correlation from map
	if len(provisionCorrelationMap) > 0 {
		t.Error("Should have no correlation entry left in the correlation map")
	}
}

// TestSolClientEndpointProvisionerHandleProvisionOkEvent - tests the provision event handlers
// Mock emit an event and checking that the result in the channel is the emited event
func TestSolClientEndpointProvisionerHandleProvisionOkEvent(t *testing.T) {
	events := dummyEvents()
	endpointProvisioner := newCcsmpEndpointProvisioner(nil, events)
	provisionCorrelationMap := endpointProvisioner.provisionCorrelation
	endpointProvisioner.start() // start the provisioner (should register the handlers)
	if !endpointProvisioner.IsRunning() {
		t.Error("endpointProvisioner should have started")
	}

	// get a new correlation ID and outcome channel
	correlationID, channel := endpointProvisioner.getNewProvisionCorrelation()
	endpointProvisioner.ClearProvisionCorrelation(correlationID) // remove the ID from the map since the address may be incorrect

	// create a new mock test event
	var eventUser = 9
	var testProvisionEventParams = &sessionEventInfo{
		fmt.Errorf("hello world"),
		"hello world",
		unsafe.Pointer(&correlationID),
		unsafe.Pointer(&eventUser),
	}

	// add the correct correlationID to the map
	correctCorrelationID := uintptr(testProvisionEventParams.GetCorrelationPointer())
	provisionCorrelationMap[correctCorrelationID] = channel

	events.emitEvent(SolClientProvisionOk, testProvisionEventParams)
	if len(provisionCorrelationMap) > 0 {
		t.Error("The provision correlation map should be empty")
	}
	fmt.Println(correlationID, channel)
	event := <-channel
	if event.GetID() != correctCorrelationID {
		t.Errorf("handleProvisionAndDeprovisionOK() called with wrong params! expected ID [%v], got [%v]", correctCorrelationID, event.GetID())
	}
}

// TestSolClientEndpointProvisionerHandleProvisionErrorEvent - tests the provision event handlers
// Mock emit an event and checking that the result in the channel is the emited event
func TestSolClientEndpointProvisionerHandleProvisionErrorEvent(t *testing.T) {
	events := dummyEvents()
	endpointProvisioner := newCcsmpEndpointProvisioner(nil, events)
	provisionCorrelationMap := endpointProvisioner.provisionCorrelation
	endpointProvisioner.start() // start the provisioner (should register the handlers)
	if !endpointProvisioner.IsRunning() {
		t.Error("endpointProvisioner should have started")
	}

	// get a new correlation ID and outcome channel
	correlationID, channel := endpointProvisioner.getNewProvisionCorrelation()
	endpointProvisioner.ClearProvisionCorrelation(correlationID) // remove the ID from the map since the address may be incorrect

	// create a new mock test event
	var eventUser = 9
	var testProvisionEventParams = &sessionEventInfo{
		fmt.Errorf("hello world"),
		"hello world",
		unsafe.Pointer(&correlationID),
		unsafe.Pointer(&eventUser),
	}

	// add the correct correlationID to the map
	correctCorrelationID := uintptr(testProvisionEventParams.GetCorrelationPointer())
	provisionCorrelationMap[correctCorrelationID] = channel

	if len(provisionCorrelationMap) == 0 {
		t.Error("A new correlation entry should have been added to the provision correlation map")
	}
	// emit provision error here
	events.emitEvent(SolClientProvisionError, testProvisionEventParams)
	if len(provisionCorrelationMap) > 0 {
		t.Error("The provision correlation map should be empty")
	}
	event := <-channel
	if event.GetID() != correctCorrelationID {
		t.Errorf("handleProvisionAndDeprovisionErr() called with wrong params! expected ID [%v], got [%v]", correctCorrelationID, event.GetID())
	}
}

// TestSolClientEndpointProvisionerHandleProvisionAndDeprovisionFunc - tests the provision event handlers
// call the event handler directly and check that the result in the channel is the passed in event
func TestSolClientEndpointProvisionerHandleProvisionAndDeprovisionFunc(t *testing.T) {
	events := dummyEvents()
	endpointProvisioner := newCcsmpEndpointProvisioner(nil, events)
	provisionCorrelationMap := endpointProvisioner.provisionCorrelation
	endpointProvisioner.start() // start the provisioner (should register the handlers)
	if !endpointProvisioner.IsRunning() {
		t.Error("endpointProvisioner should have started")
	}

	// get a new correlation ID and outcome channel
	correlationID, channel := endpointProvisioner.getNewProvisionCorrelation()
	endpointProvisioner.ClearProvisionCorrelation(correlationID) // remove the ID from the map since the address may be incorrect

	// create a new mock test event
	var eventUser = 9
	var testProvisionEventParams = &sessionEventInfo{
		fmt.Errorf("hello world"),
		"hello world",
		unsafe.Pointer(&correlationID),
		unsafe.Pointer(&eventUser),
	}

	// add the correct correlationID to the map
	correctCorrelationID := uintptr(testProvisionEventParams.GetCorrelationPointer())
	provisionCorrelationMap[correctCorrelationID] = channel

	if len(provisionCorrelationMap) == 0 {
		t.Error("A new correlation entry should have been added to the provision correlation map")
	}
	// call event handler directly with even params
	endpointProvisioner.handleProvisionAndDeprovisionEvent(testProvisionEventParams, nil)
	if len(provisionCorrelationMap) > 0 {
		t.Error("The provision correlation map should be empty")
	}
	event := <-channel
	if event.GetID() != correctCorrelationID {
		t.Errorf("handleProvisionAndDeprovisionEvent() called with wrong params! expected ID [%v], got [%v]", correctCorrelationID, event.GetID())
	}
}
