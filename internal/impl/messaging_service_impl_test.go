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
	"fmt"
	"testing"
	"time"

	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace"
)

func TestMessagingServiceLifecycleValidations(t *testing.T) {
	service := newMessagingServiceImpl(logging.Default)
	mockTransport := &solClientTransportMock{}
	service.transport = mockTransport
	connectCalled := make(chan struct{})
	disconnectCalled := make(chan struct{})
	connectLatch := make(chan struct{})
	disconnectLatch := make(chan struct{})
	mockTransport.connect = func() error {
		select {
		case <-connectCalled:
			t.Error("connect was called twice, expected only once")
		default:
			close(connectCalled)
		}
		<-connectLatch
		return nil
	}
	mockTransport.disconnect = func() error {
		select {
		case <-disconnectCalled:
			t.Error("disconnect was called twice, expected only once")
		default:
			close(disconnectCalled)
		}
		<-disconnectLatch
		return nil
	}
	var err error

	// not connected
	if service.IsConnected() {
		t.Error("returned IsConnected true before the service was connected")
	}
	err = service.Disconnect()
	if err == nil {
		t.Error("expected disconnect call to be rejected when state is not connected")
	}
	// connecting
	errChan := service.ConnectAsync()
	select {
	case <-connectCalled:
		// success
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for transport connect to be called")
	}
	if service.IsConnected() {
		t.Error("returned IsConnected true before the service was connected")
	}
	err = service.Disconnect()
	if err == nil {
		t.Error("expected disconnect call to be rejected when state is connecting")
	}

	// connected
	close(connectLatch)
	select {
	case err = <-errChan:
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for connect to complete")
	}
	if err != nil {
		t.Errorf("expected error to be nil, got %s", err)
	}
	if !service.IsConnected() {
		t.Error("returned IsConnected false after the service was connected")
	}
	// disconnecting
	errChan = service.DisconnectAsync()
	select {
	case <-disconnectCalled:
		// success
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for transport disconnect to be called")
	}
	if service.IsConnected() {
		t.Error("returned IsConnected true after the service was disconnecting")
	}
	err = service.Connect()
	if err == nil {
		t.Error("expected connect call to be rejected when state is disconnecting")
	}

	// disconnected
	close(disconnectLatch)
	select {
	case err = <-errChan:
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for connect to complete")
	}
	if err != nil {
		t.Errorf("expected error to be nil, got %s", err)
	}
	if service.IsConnected() {
		t.Error("returned IsConnected true after the service was disconnected")
	}
	err = service.Connect()
	if err == nil {
		t.Error("expected connect call to be rejected when state is disconnected")
	}
}

func TestMessagingServiceConnectIdempotence(t *testing.T) {
	service := newMessagingServiceImpl(logging.Default)
	mockTransport := &solClientTransportMock{}
	service.transport = mockTransport

	myError := fmt.Errorf("hello world")
	called := make(chan struct{})
	mockTransport.connect = func() error {
		select {
		case <-called:
			t.Error("connect was called twice, expected only once")
		default:
			close(called)
		}
		return myError
	}

	err := service.Connect()
	if err != myError {
		t.Errorf("expected error to equal myError, got %s", err)
	}
	err2 := service.Connect()
	if err2 != err {
		t.Errorf("expected to get the same error twice, got %s instead", err2)
	}
	err3 := <-service.ConnectAsync()
	if err3 != err {
		t.Errorf("expected to get the same error when calling connect async, got %s instead", err3)
	}
	errChan := make(chan error)
	service.ConnectAsyncWithCallback(func(passedService solace.MessagingService, err error) {
		if service != passedService {
			t.Error("expected service passed to callback to be the same as the calling service")
		}
		errChan <- err
	})
	err4 := <-errChan
	if err4 != err {
		t.Errorf("expected to get the same error when calling connect async callback, got %s instead", err4)
	}
}

func TestMessagingServiceConnectBlockingUntilComplete(t *testing.T) {
	service := newMessagingServiceImpl(logging.Default)
	mockTransport := &solClientTransportMock{}
	service.transport = mockTransport

	called := make(chan struct{})
	blockingChannel := make(chan struct{})
	mockTransport.connect = func() error {
		select {
		case <-called:
			t.Error("connect was called twice, expected only once")
		default:
			close(called)
		}
		<-blockingChannel
		return nil
	}

	result := service.ConnectAsync()
	secondResult := service.ConnectAsync()

	select {
	case <-result:
		t.Error("did not expect return until blocking channel was closed")
	case <-secondResult:
		t.Error("did not expect return until blocking channel was closed")
	case <-time.After(100 * time.Millisecond):
		// success
	}

	close(blockingChannel)
	select {
	case <-result:
		//success
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for result")
	}

	select {
	case <-secondResult:
		//success
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for second result")
	}
}

func TestMessagingServiceDisconnectIdempotence(t *testing.T) {
	service := newMessagingServiceImpl(logging.Default)
	mockTransport := &solClientTransportMock{}
	service.transport = mockTransport

	myError := fmt.Errorf("hello world")
	called := make(chan struct{})
	mockTransport.disconnect = func() error {
		select {
		case <-called:
			t.Error("connect was called twice, expected only once")
		default:
			close(called)
		}
		return myError
	}

	service.Connect()

	err := service.Disconnect()
	if err != myError {
		t.Errorf("expected error to equal myError, got %s", err)
	}
	err2 := service.Disconnect()
	if err2 != err {
		t.Errorf("expected to get the same error twice, got %s instead", err2)
	}
	err3 := <-service.DisconnectAsync()
	if err3 != err {
		t.Errorf("expected to get the same error when calling disconnect async, got %s instead", err3)
	}
	errChan := make(chan error)
	service.DisconnectAsyncWithCallback(func(err error) {
		errChan <- err
	})
	err4 := <-errChan
	if err4 != err {
		t.Errorf("expected to get the same error when calling disconnect async callback, got %s instead", err4)
	}
}

func TestMessagingServiceDisconnectBlockingUntilComplete(t *testing.T) {
	service := newMessagingServiceImpl(logging.Default)
	mockTransport := &solClientTransportMock{}
	service.transport = mockTransport

	called := make(chan struct{})
	blockingChannel := make(chan struct{})
	mockTransport.disconnect = func() error {
		select {
		case <-called:
			t.Error("disconnect was called twice, expected only once")
		default:
			close(called)
		}
		<-blockingChannel
		return nil
	}

	service.Connect()

	result := service.DisconnectAsync()
	secondResult := service.DisconnectAsync()

	select {
	case <-result:
		t.Error("did not expect return until blocking channel was closed")
	case <-secondResult:
		t.Error("did not expect return until blocking channel was closed")
	case <-time.After(100 * time.Millisecond):
		// success
	}

	close(blockingChannel)
	select {
	case <-result:
		//success
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for result")
	}

	select {
	case <-secondResult:
		//success
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for second result")
	}
}

type solClientTransportMock struct {
	connect             func() error
	disconnect          func() error
	close               func() error
	publisher           func() core.Publisher
	receiver            func() core.Receiver
	metrics             func() core.Metrics
	events              func() core.Events
	endpointProvisioner func() core.EndpointProvisioner
}

func (transport *solClientTransportMock) Connect() error {
	if transport.connect != nil {
		return transport.connect()
	}
	return nil
}

func (transport *solClientTransportMock) Disconnect() error {
	if transport.disconnect != nil {
		return transport.disconnect()
	}
	return nil
}

func (transport *solClientTransportMock) Close() error {
	if transport.close != nil {
		return transport.close()
	}
	return nil
}

func (transport *solClientTransportMock) Publisher() core.Publisher {
	if transport.publisher != nil {
		return transport.publisher()
	}
	return nil
}

func (transport *solClientTransportMock) Receiver() core.Receiver {
	if transport.receiver != nil {
		return transport.receiver()
	}
	return nil
}

func (transport *solClientTransportMock) EndpointProvisioner() core.EndpointProvisioner {
	if transport.endpointProvisioner != nil {
		return transport.endpointProvisioner()
	}
	return nil
}

func (transport *solClientTransportMock) Metrics() core.Metrics {
	if transport.metrics != nil {
		return transport.metrics()
	}
	return nil
}

func (transport *solClientTransportMock) Events() core.Events {
	if transport.events != nil {
		return transport.events()
	}
	return &solClientTransportEventsMock{}
}

func (transport *solClientTransportMock) ID() string {
	return ""
}

func (transport *solClientTransportMock) Host() string {
	return ""
}

func (transport *solClientTransportMock) ModifySessionProperties(_ []string) error {
	// FFC: This function is added to pass a check during building. Currently it
	// is not used, so there is no implementation. The implementation will need
	// to be added as a part of future work.
	return nil
}

type solClientTransportEventsMock struct {
}

func (events *solClientTransportEventsMock) AddEventHandler(sessionEvent core.Event, responseCode core.EventHandler) uint {
	return 0
}

func (events *solClientTransportEventsMock) RemoveEventHandler(id uint) {
}
