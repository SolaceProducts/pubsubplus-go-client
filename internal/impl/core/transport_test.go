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

package core // same package to test internal functionality

import (
	"fmt"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

var correlation = 1
var user = 2

var testParams = &sessionEventInfo{
	fmt.Errorf("hello world"), "hello world", unsafe.Pointer(&correlation), unsafe.Pointer(&user),
}

var testEvent = SolClientEventCanSend
var testEvent2 = SolClientEventDown

func TestSolClientTransportEventRegistration(t *testing.T) {
	events := dummyEvents()
	handler := &callback{}
	events.AddEventHandler(testEvent, handler.exec)
	events.emitEvent(testEvent, testParams)
	if handler.params == nil {
		t.Error("event handler not called")
	} else if !handler.params.equals(testParams) {
		t.Errorf("event handler called with wrong params! expected [%v], got [%s]", testParams, handler.params)
	}
}

func TestSolClientTransportEventDeregistration(t *testing.T) {
	events := dummyEvents()
	handler := &callback{}
	eventID := events.AddEventHandler(testEvent, handler.exec)
	events.emitEvent(testEvent, testParams)
	if handler.params == nil {
		t.Error("event handler not called")
	} else if !handler.params.equals(testParams) {
		t.Errorf("event handler called with wrong params! expected [%v], got [%s]", testParams, handler.params)
	}
	events.RemoveEventHandler(eventID)
	handler.params = nil
	events.emitEvent(testEvent, testParams)
	if handler.params != nil {
		t.Error("event handler called, expected it to not be called after being deregistereds")
	}
}

func TestSolClientTransportEventNoOverlap(t *testing.T) {
	events := dummyEvents()
	handler := &callback{}
	events.AddEventHandler(testEvent, handler.exec)
	events.emitEvent(testEvent2, testParams)
	if handler.params != nil {
		t.Error("event handler called when not registered for event")
	}
}

func TestSolClientTransportMultipleEventRegistration(t *testing.T) {
	events := dummyEvents()
	numHandlers := 5
	handlers := [5]*callback{}
	// register multiple handlers
	for i := 0; i < numHandlers; i++ {
		handlers[i] = &callback{}
		events.AddEventHandler(testEvent, handlers[i].exec)
	}
	// send one event
	events.emitEvent(testEvent, testParams)
	// assert that all handlers were called
	for i := 0; i < numHandlers; i++ {
		if handlers[i].params == nil {
			t.Error("event handler not called")
		} else if !handlers[i].params.equals(testParams) {
			t.Errorf("event handler called with wrong params! expected [%v], got [%s]", testParams, handlers[i].params)
		}
	}
}

// Originally, the CCSMP implementation of SolClientTransport/Publisher/Receiver would never get GC'd because
// of a circular reference. This function will validate that we can terminate successfully and get garbage collected.
func TestSolClientTransportGC(t *testing.T) {
	notification := make(chan struct{})
	{
		transport, err := NewTransport("", []string{})
		if err != nil {
			t.Error(err)
		}
		runtime.SetFinalizer(transport, nil) // clear finalizer first
		runtime.SetFinalizer(transport, func(transport *ccsmpTransport) {
			close(notification)
		})
	}
	maxGcAttempts := 10
	gcAttempts := 0
loop:
	for {
		runtime.GC()
		select {
		case <-notification:
			// success
			break loop
		case <-time.After(10 * time.Millisecond):
			gcAttempts++
			if gcAttempts == maxGcAttempts {
				t.Errorf("gave up on garbage collection after %d attempts", gcAttempts)
				break loop
			}
			continue loop
		}
	}
}

// We define the following behaviour for the case of an unstarted, orphaned transport:
// the underlying context and session will be gc'd and all references will be removed.
// func TestSolClientTransportFinalizer(t *testing.T) {
// 	var contextP *ccsmp.SolClientContext
// 	var sessionP *ccsmp.SolClientSession
// 	{
// 		transport, err := NewTransport([]string{})
// 		if err != nil {
// 			t.Error(err)
// 		}
// 		transportImpl, ok := transport.(*ccsmpTransport)
// 		if !ok {
// 			t.Errorf("expected *ccsmpTransport, got %T", transport)
// 		} else {
// 			contextP = transportImpl.context
// 			sessionP = transportImpl.session
// 		}
// 	}
// }

func dummyEvents() *ccsmpBackedEvents {
	return &ccsmpBackedEvents{
		eventHandlers: make(map[Event](map[uint]EventHandler)),
	}
}

type callback struct {
	params *callbackParams
}

type callbackParams struct {
	err          error
	info         string
	correlationP unsafe.Pointer
}

func (params *callbackParams) equals(other SessionEventInfo) bool {
	return params.err == other.GetError() && params.info == other.GetInfoString() && params.correlationP == other.GetCorrelationPointer()
}

func (params *callbackParams) String() string {
	return fmt.Sprintf("responseCode=%d, info='%s', correlationP=%p", params.err, params.info, params.correlationP)
}

func (c *callback) exec(eventInfo SessionEventInfo) {
	c.params = &callbackParams{
		eventInfo.GetError(), eventInfo.GetInfoString(), eventInfo.GetCorrelationPointer(),
	}
}
