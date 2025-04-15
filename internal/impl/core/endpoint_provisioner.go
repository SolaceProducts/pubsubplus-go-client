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
	"sync"
	"sync/atomic"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace"
)

// ProvisionCorrelationID defined
type ProvisionCorrelationID = uintptr

// ProvisionEvent is the event passed to a channel on completion of an endpoint provision
type ProvisionEvent interface {
	GetID() ProvisionCorrelationID
	GetError() error
}

// EndpointProvisioner interface
type EndpointProvisioner interface {
	// Events returns SolClientEvents
	Events() Events
	// IsRunning checks if the internal provisioner is running
	IsRunning() bool
	// Provision the endpoint on the broker from the correlation pointer
	Provision(properties []string, ignoreExistErrors bool) (ProvisionCorrelationID, <-chan ProvisionEvent, ErrorInfo)
	// Deprovision an endpoint on the broker from the given correlation pointer
	Deprovision(properties []string, ignoreMissingErrors bool) (ProvisionCorrelationID, <-chan ProvisionEvent, ErrorInfo)
	// ClearProvisionCorrelation clears the provison correlation with the given ID
	ClearProvisionCorrelation(id ProvisionCorrelationID)
}

type ccsmpBackedEndpointProvisioner struct {
	events    *ccsmpBackedEvents
	session   *ccsmp.SolClientSession
	isRunning int32
	rxLock    sync.RWMutex

	// provision and deprovision
	provisionCorrelationLock              sync.Mutex
	provisionCorrelation                  map[ProvisionCorrelationID]chan ProvisionEvent
	provisionCorrelationID                ProvisionCorrelationID
	provisionOkEvent, provisionErrorEvent uint
}

func newCcsmpEndpointProvisioner(session *ccsmp.SolClientSession, events *ccsmpBackedEvents) *ccsmpBackedEndpointProvisioner {
	provisioner := &ccsmpBackedEndpointProvisioner{}
	provisioner.events = events
	provisioner.session = session
	provisioner.isRunning = 0
	provisioner.provisionCorrelation = make(map[ProvisionCorrelationID]chan ProvisionEvent)
	provisioner.provisionCorrelationID = 0
	return provisioner
}

func (provisioner *ccsmpBackedEndpointProvisioner) Events() Events {
	return provisioner.events
}

func (provisioner *ccsmpBackedEndpointProvisioner) IsRunning() bool {
	return atomic.LoadInt32(&provisioner.isRunning) == 1
}

func (provisioner *ccsmpBackedEndpointProvisioner) start() {
	if !atomic.CompareAndSwapInt32(&provisioner.isRunning, 0, 1) {
		return
	}
	// this events are for both Provision and Deprovision
	provisioner.provisionOkEvent = provisioner.Events().AddEventHandler(SolClientProvisionOk, provisioner.handleProvisionAndDeprovisionOK)
	provisioner.provisionErrorEvent = provisioner.Events().AddEventHandler(SolClientProvisionError, provisioner.handleProvisionAndDeprovisionErr)
}

func (provisioner *ccsmpBackedEndpointProvisioner) terminate() {
	if !atomic.CompareAndSwapInt32(&provisioner.isRunning, 1, 0) {
		return
	}

	// clean up the provision and deprovision resources like eventHandlers and corelation IDs
	provisioner.events.RemoveEventHandler(provisioner.provisionOkEvent)
	provisioner.events.RemoveEventHandler(provisioner.provisionErrorEvent)
	// Clear out the provision/deprovision correlation map when terminating, interrupt all awaiting items
	provisioner.provisionCorrelationLock.Lock()
	defer provisioner.provisionCorrelationLock.Unlock()
	terminationError := solace.NewError(&solace.ServiceUnreachableError{}, constants.CouldNotConfirmProvisionDeprovisionServiceUnavailable, nil)
	for id, result := range provisioner.provisionCorrelation {
		delete(provisioner.provisionCorrelation, id)
		result <- &provisionEvent{
			id:  id,
			err: terminationError,
		}
	}
}

// Provision the endpoint on the broker with a correlationID
func (provisioner *ccsmpBackedEndpointProvisioner) Provision(properties []string, ignoreExistErrors bool) (ProvisionCorrelationID, <-chan ProvisionEvent, ErrorInfo) {
	provisioner.rxLock.RLock()
	defer provisioner.rxLock.RUnlock()
	correlationID, channel := provisioner.getNewProvisionCorrelation()

	errInfo := provisioner.session.SolClientEndpointProvisionAsync(properties, correlationID, ignoreExistErrors)
	if errInfo != nil {
		provisioner.ClearProvisionCorrelation(correlationID)
		return 0, nil, errInfo
	}
	return correlationID, channel, nil
}

// Deprovision the endpoint from the broker with a correlationID
func (provisioner *ccsmpBackedEndpointProvisioner) Deprovision(properties []string, ignoreMissingErrors bool) (ProvisionCorrelationID, <-chan ProvisionEvent, ErrorInfo) {
	provisioner.rxLock.RLock()
	defer provisioner.rxLock.RUnlock()
	correlationID, channel := provisioner.getNewProvisionCorrelation()

	errInfo := provisioner.session.SolClientEndpointDeprovisionAsync(properties, correlationID, ignoreMissingErrors)
	if errInfo != nil {
		provisioner.ClearProvisionCorrelation(correlationID)
		return 0, nil, errInfo
	}
	return correlationID, channel, nil
}

func (provisioner *ccsmpBackedEndpointProvisioner) ClearProvisionCorrelation(id ProvisionCorrelationID) {
	provisioner.provisionCorrelationLock.Lock()
	defer provisioner.provisionCorrelationLock.Unlock()
	delete(provisioner.provisionCorrelation, id) // remove the provision correlation ID
}

func (provisioner *ccsmpBackedEndpointProvisioner) getNewProvisionCorrelation() (ProvisionCorrelationID, chan ProvisionEvent) {
	newID := atomic.AddUintptr(&provisioner.provisionCorrelationID, 1)
	// we always want to have a space of 1 in the channel so we don't even block
	resultChan := make(chan ProvisionEvent, 1)
	provisioner.provisionCorrelationLock.Lock()
	provisioner.provisionCorrelation[newID] = resultChan
	provisioner.provisionCorrelationLock.Unlock()
	return newID, resultChan
}

func (provisioner *ccsmpBackedEndpointProvisioner) handleProvisionAndDeprovisionOK(event SessionEventInfo) {
	provisioner.handleProvisionAndDeprovisionEvent(event, nil)
}

func (provisioner *ccsmpBackedEndpointProvisioner) handleProvisionAndDeprovisionErr(event SessionEventInfo) {
	provisioner.handleProvisionAndDeprovisionEvent(event, event.GetError())
}

func (provisioner *ccsmpBackedEndpointProvisioner) handleProvisionAndDeprovisionEvent(event SessionEventInfo, err error) {
	provisioner.provisionCorrelationLock.Lock()
	defer provisioner.provisionCorrelationLock.Unlock()
	corrP := uintptr(event.GetCorrelationPointer())
	if resultChan, ok := provisioner.provisionCorrelation[corrP]; ok {
		delete(provisioner.provisionCorrelation, corrP)
		resultChan <- &provisionEvent{
			id:  corrP,
			err: err,
		}
	} else if logging.Default.IsDebugEnabled() {
		logging.Default.Debug(fmt.Sprintf("Handle Provision/Deprovision callback called but no provision correlation channel is registered for CorrelationID %v", corrP))
	}
}

// provisionEvent
type provisionEvent struct {
	id  ProvisionCorrelationID
	err error
}

func (event *provisionEvent) GetID() ProvisionCorrelationID {
	return event.id
}

func (event *provisionEvent) GetError() error {
	return event.err
}
