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

package message

import (
	"encoding/hex"
	"strings"
	"testing"

	"solace.dev/go/messaging/internal/ccsmp"
)

func TestOutboundMessageFree(t *testing.T) {
	msg, err := NewOutboundMessage()
	if err != nil {
		t.Error("did not expect error, got " + err.Error())
	}
	if msg.messagePointer == ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}
	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
	if msg.messagePointer != ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected MessagePointer to be freed and set to nil, it was not")
	}
}

func TestInboundMessageFree(t *testing.T) {
	msgP, ccsmpErr := ccsmp.SolClientMessageAlloc()
	if ccsmpErr != nil {
		t.Error("did not expect error, got " + ccsmpErr.GetMessageAsString())
	}
	msg := NewInboundMessage(msgP, false)
	if msg.messagePointer == ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}
	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
	if msg.messagePointer != ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected MessagePointer to be freed and set to nil, it was not")
	}
}

// Distributed Tracing

func TestSetCreationTraceContext(t *testing.T) {
	// the creation context value to test
	creationCtxTraceID, _ := hex.DecodeString("79f90916c9a3dad1eb4b328e00469e45")
	creationCtxSpanID, _ := hex.DecodeString("3b364712c4e1f17f")
	sampledValue := true
	traceStateValue := "sometrace=Example"
	emptyTraceStateValue := ""

	var creationCtxTraceID16, emptyCreationCtxTraceID16 [16]byte
	var creationCtxSpanID8, emptyCreationCtxSpanID8 [8]byte
	copy(creationCtxTraceID16[:], creationCtxTraceID)
	copy(creationCtxSpanID8[:], creationCtxSpanID)

	msgP, ccsmpErr := ccsmp.SolClientMessageAlloc()
	if ccsmpErr != nil {
		t.Error("did not expect error, got " + ccsmpErr.GetMessageAsString())
	}
	msg := NewInboundMessage(msgP, false)
	if msg.messagePointer == ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}

	// test setting the creation context value
	ok := msg.SetCreationTraceContext(creationCtxTraceID16, creationCtxSpanID8, sampledValue, &traceStateValue)
	if !ok {
		t.Error("expected SetCreationTraceContext() function for valid values to succeed and return true")
	}

	// test setting the creation context value - empty TraceID
	okEmptyTraceID := msg.SetCreationTraceContext(emptyCreationCtxTraceID16, creationCtxSpanID8, sampledValue, &traceStateValue)
	if !okEmptyTraceID {
		t.Error("expected SetCreationTraceContext() function for empty TraceID to succeed and return true")
	}

	// test setting the creation context value - empty SpanID
	okEmptySpanID := msg.SetCreationTraceContext(creationCtxTraceID16, emptyCreationCtxSpanID8, sampledValue, &traceStateValue)
	if !okEmptySpanID {
		t.Error("expected SetCreationTraceContext() function for empty SpanID to succeed and return true")
	}

	// test setting the creation context value - empty trace state
	okEmptyTraceState := msg.SetCreationTraceContext(creationCtxTraceID16, creationCtxSpanID8, sampledValue, &emptyTraceStateValue)
	if !okEmptyTraceState {
		t.Error("expected SetCreationTraceContext() function for empty traceState to succeed and return true")
	}

	// test setting the creation context value - nil trace state
	okNilTraceState := msg.SetCreationTraceContext(creationCtxTraceID16, creationCtxSpanID8, sampledValue, nil)
	if !okNilTraceState {
		t.Error("expected SetCreationTraceContext() function for Nil traceState to succeed and return true")
	}

	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
}

func TestGetCreationTraceContext(t *testing.T) {
	// the creation context value to test
	creationCtxTraceID, _ := hex.DecodeString("79f90916c9a3dad1eb4b328e00469e45")
	creationCtxSpanID, _ := hex.DecodeString("3b364712c4e1f17f")
	sampledValue := true
	traceStateValue := "sometrace=Example"

	var creationCtxTraceID16 [16]byte
	var creationCtxSpanID8 [8]byte
	copy(creationCtxTraceID16[:], creationCtxTraceID)
	copy(creationCtxSpanID8[:], creationCtxSpanID)

	msgP, ccsmpErr := ccsmp.SolClientMessageAlloc()
	if ccsmpErr != nil {
		t.Error("did not expect error, got " + ccsmpErr.GetMessageAsString())
	}
	msg := NewInboundMessage(msgP, false)
	if msg.messagePointer == ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}

	// test setting the creation context value
	setValuesOk := msg.SetCreationTraceContext(creationCtxTraceID16, creationCtxSpanID8, sampledValue, &traceStateValue)
	if !setValuesOk {
		t.Error("expected SetCreationTraceContext() function for valid values to succeed and return true")
	}

	// get the creation context values
	traceID, spanID, sampled, traceState, ok := msg.GetCreationTraceContext()
	if !ok {
		t.Error("expected GetCreationTraceContext() function to return creation context values and return true")
	}

	// test traceID equality
	if traceID != creationCtxTraceID16 {
		t.Error("expected GetCreationTraceContext() traceID from message should be the same as what was set in message")
	}

	// test spanID equality
	if spanID != creationCtxSpanID8 {
		t.Error("expected GetCreationTraceContext() spanID from message should be the same as what was set in message")
	}

	// test traceID equality
	if sampled != sampledValue {
		t.Error("expected GetCreationTraceContext() sampled value from message should be the same as what was set in message")
	}

	// test traceState equality
	if strings.Compare(traceState, traceStateValue) != 0 {
		t.Error("expected GetCreationTraceContext() traceState from message should be the same as what was set in message")
	}

	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
}

func TestSetTransportTraceContext(t *testing.T) {
	// the transport context value to test
	transportCtxTraceID, _ := hex.DecodeString("55d30916c9a3dad1eb4b328e00469e45")
	transportCtxSpanID, _ := hex.DecodeString("a7164712c4e1f17f")
	sampledValue := true
	traceStateValue := "sometrace=Example"
	emptyTraceStateValue := ""

	var transportCtxTraceID16, emptyTransportCtxTraceID16 [16]byte
	var transportCtxSpanID8, emptyTransportCtxSpanID8 [8]byte
	copy(transportCtxTraceID16[:], transportCtxTraceID)
	copy(transportCtxSpanID8[:], transportCtxSpanID)

	msgP, ccsmpErr := ccsmp.SolClientMessageAlloc()
	if ccsmpErr != nil {
		t.Error("did not expect error, got " + ccsmpErr.GetMessageAsString())
	}
	msg := NewInboundMessage(msgP, false)
	if msg.messagePointer == ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}

	// test setting the transport context value
	ok := msg.SetTransportTraceContext(transportCtxTraceID16, transportCtxSpanID8, sampledValue, &traceStateValue)
	if !ok {
		t.Error("expected SetTransportTraceContext() function for valid values to succeed and return true")
	}

	// test setting the transport context value - empty TraceID
	okEmptyTraceID := msg.SetTransportTraceContext(emptyTransportCtxTraceID16, transportCtxSpanID8, sampledValue, &traceStateValue)
	if !okEmptyTraceID {
		t.Error("expected SetTransportTraceContext() function for empty TraceID to succeed and return true")
	}

	// test setting the transport context value - empty SpanID
	okEmptySpanID := msg.SetTransportTraceContext(transportCtxTraceID16, emptyTransportCtxSpanID8, sampledValue, &traceStateValue)
	if !okEmptySpanID {
		t.Error("expected SetTransportTraceContext() function for empty SpanID to succeed and return true")
	}

	// test setting the transport context value - empty trace state
	okEmptyTraceState := msg.SetTransportTraceContext(transportCtxTraceID16, transportCtxSpanID8, sampledValue, &emptyTraceStateValue)
	if !okEmptyTraceState {
		t.Error("expected SetTransportTraceContext() function for empty traceState to succeed and return true")
	}

	// test setting the transport context value - nil trace state
	okNilTraceState := msg.SetTransportTraceContext(transportCtxTraceID16, transportCtxSpanID8, sampledValue, nil)
	if !okNilTraceState {
		t.Error("expected SetTransportTraceContext() function for nil traceState to succeed and return true")
	}

	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
}

func TestGetTransportTraceContext(t *testing.T) {
	// the transport context value to test
	transportCtxTraceID, _ := hex.DecodeString("55d30916c9a3dad1eb4b328e00469e45")
	transportCtxSpanID, _ := hex.DecodeString("a7164712c4e1f17f")
	sampledValue := true
	traceStateValue := "sometrace=Example"

	var transportCtxTraceID16 [16]byte
	var transportCtxSpanID8 [8]byte
	copy(transportCtxTraceID16[:], transportCtxTraceID)
	copy(transportCtxSpanID8[:], transportCtxSpanID)

	msgP, ccsmpErr := ccsmp.SolClientMessageAlloc()
	if ccsmpErr != nil {
		t.Error("did not expect error, got " + ccsmpErr.GetMessageAsString())
	}
	msg := NewInboundMessage(msgP, false)
	if msg.messagePointer == ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}

	// test setting the transport context value
	msg.SetCreationTraceContext(transportCtxTraceID16, transportCtxSpanID8, false, nil) // have to set creation context to prevent undesired behaviour
	setValuesOk := msg.SetTransportTraceContext(transportCtxTraceID16, transportCtxSpanID8, sampledValue, &traceStateValue)
	if !setValuesOk {
		t.Error("expected SetTransportTraceContext() function for valid values to succeed and return true")
	}

	// get the transport context values
	traceID, spanID, sampled, traceState, ok := msg.GetTransportTraceContext()
	if !ok {
		t.Error("expected GetTransportTraceContext() function to return transport context values and return true")
	}

	// test traceID equality
	if traceID != transportCtxTraceID16 {
		t.Error("expected GetTransportTraceContext() traceID from message should be the same as what was set in message")
	}

	// test spanID equality
	if spanID != transportCtxSpanID8 {
		t.Error("expected GetTransportTraceContext() spanID from message should be the same as what was set in message")
	}

	// test traceID equality
	if sampled != sampledValue {
		t.Error("expected GetTransportTraceContext() sampled value from message should be the same as what was set in message")
	}

	// test traceState equality
	if strings.Compare(traceState, traceStateValue) != 0 {
		t.Error("expected GetTransportTraceContext() traceState from message should be the same as what was set in message")
	}

	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
}

func TestSetBaggage(t *testing.T) {
	// the baggage value to test
	baggageValue := "baggage=value1;example=value2"

	msgP, ccsmpErr := ccsmp.SolClientMessageAlloc()
	if ccsmpErr != nil {
		t.Error("did not expect error, got " + ccsmpErr.GetMessageAsString())
	}
	msg := NewInboundMessage(msgP, false)
	if msg.messagePointer == ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}

	// test setting a valid baggage value
	err := msg.SetBaggage(baggageValue)
	if err != nil {
		t.Error(err)
	}

	// test setting empty baggage
	emptyCaseErr := msg.SetBaggage("")
	if emptyCaseErr != nil {
		t.Error(emptyCaseErr)
	}

	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
}

func TestGetBaggage(t *testing.T) {
	// the baggage value to test
	baggageValue := "baggage=value1;example=value2"

	msgP, ccsmpErr := ccsmp.SolClientMessageAlloc()
	if ccsmpErr != nil {
		t.Error("did not expect error, got " + ccsmpErr.GetMessageAsString())
	}
	msg := NewInboundMessage(msgP, false)
	if msg.messagePointer == ccsmp.SolClientOpaquePointerInvalidValue {
		t.Error("expected message pointer to not be nil")
	}
	if msg.IsDisposed() {
		t.Error("message is disposed before disposed called")
	}
	// should not throw any errors
	err := msg.SetBaggage(baggageValue)
	if err != nil {
		t.Error(err)
	}

	baggage, ok := msg.GetBaggage()
	if !ok {
		t.Error("expected GetBaggage() function to return baggage and true")
	}
	if baggage == "" {
		t.Error("expected baggage not to be an empty string")
	}
	if baggage != baggageValue {
		t.Error("expected baggage from message should be the same baggage set on the message")
	}

	msg.Dispose()
	if !msg.IsDisposed() {
		t.Error("IsDisposed returned false, expected true")
	}
}
