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

package publisher

import (
	"fmt"
	"testing"
	"time"

	"solace.dev/go/messaging/internal/impl/core"
)

func TestMessagePublisherStartStateChecks(t *testing.T) {
	publisher := &basicMessagePublisher{}
	publisher.construct(&mockInternalPublisher{})

	// Not started
	if publisher.IsRunning() {
		t.Error("expected IsRunning to return false when not started")
	}
	if publisher.IsTerminating() {
		t.Error("expected IsTerminating to return false when not terminating")
	}
	if publisher.IsTerminated() {
		t.Error("expected IsTerminated to return false when not terminating")
	}
	if err := publisher.checkStartedStateForPublish(); err == nil {
		t.Error("expected checkStartedStateForPublish to return an error when not in running state, got nil")
	}

	// Starting
	publisher.starting()
	if publisher.IsRunning() {
		t.Error("expected IsRunning to return false when starting")
	}
	if publisher.IsTerminating() {
		t.Error("expected IsTerminating to return false when not terminating")
	}
	if publisher.IsTerminated() {
		t.Error("expected IsTerminated to return false when not terminating")
	}
	if proceed, err := publisher.terminate(); proceed || err == nil {
		t.Error("expected terminate to return an error when not in running state, got nil")
	}
	if err := publisher.checkStartedStateForPublish(); err == nil {
		t.Error("expected checkStartedStateForPublish to return an error when not in running state, got nil")
	}

	// Started
	publisher.started(nil)
	if !publisher.IsRunning() {
		t.Error("expected IsRunning to return true when started")
	}
	if publisher.IsTerminating() {
		t.Error("expected IsTerminating to return false when not terminating")
	}
	if publisher.IsTerminated() {
		t.Error("expected IsTerminated to return false when not terminating")
	}
	if err := publisher.checkStartedStateForPublish(); err != nil {
		t.Errorf("expected checkStartedStateForPublish to not an error when in running state, got %s", err)
	}

	// Termianting
	publisher.terminate()
	if proceed, err := publisher.starting(); proceed || err == nil {
		t.Error("expected starting to return an error when in terminating state, got nil")
	}
	if publisher.IsRunning() {
		t.Error("expected IsRunning to return false when terminating")
	}
	if !publisher.IsTerminating() {
		t.Error("expected IsTerminating to return true when terminating")
	}
	if publisher.IsTerminated() {
		t.Error("expected IsTerminated to return false when not terminating")
	}
	if err := publisher.checkStartedStateForPublish(); err == nil {
		t.Error("expected checkStartedStateForPublish to return an error when not in running state, got nil")
	}

	// Terminated
	publisher.terminated(nil)
	if publisher.IsRunning() {
		t.Error("expected IsRunning to return false when terminated")
	}
	if publisher.IsTerminating() {
		t.Error("expected IsTerminating to return false when terminated")
	}
	if !publisher.IsTerminated() {
		t.Error("expected IsTerminated to return true when terminated")
	}
	if proceed, err := publisher.starting(); proceed || err == nil {
		t.Error("expected starting to return an error when in terminated state, got nil")
	}
	if err := publisher.checkStartedStateForPublish(); err == nil {
		t.Error("expected checkStartedStateForPublish to return an error when not in running state, got nil")
	}
}

func TestMessagePublisherTerminateWhenNotStarted(t *testing.T) {
	publisher := &basicMessagePublisher{}
	publisher.construct(&mockInternalPublisher{})

	proceed, err := publisher.terminate()
	if proceed {
		t.Error("expected proceed to be false")
	}
	if err != nil {
		t.Error("expected error to be nil")
	}
	if !publisher.IsTerminated() {
		t.Error("expected IsTerminated to return true when terminated")
	}
	proceed, err = publisher.starting()
	if proceed {
		t.Error("expected proceed to be false")
	}
	if err == nil {
		t.Error("expected illegal state error")
	}
	// try calling terminate again
	proceed, err = publisher.terminate()
	if proceed {
		t.Error("expected proceed to be false")
	}
	if err != nil {
		t.Error("expected error to be nil")
	}
}

func TestMessagePublisherStartIdempotence(t *testing.T) {
	publisher := &basicMessagePublisher{}
	publisher.construct(&mockInternalPublisher{})
	proceed, err := publisher.starting()
	if !proceed || err != nil {
		t.Errorf("expected first call to start to be successful, got proceed %t and error %s", proceed, err)
	}
	numWaiters := 3
	type result struct {
		proceed bool
		err     error
	}
	results := make(chan result, numWaiters)
	for i := 0; i < numWaiters; i++ {
		go func() {
			proceed, err := publisher.starting()
			results <- result{
				proceed: proceed,
				err:     err,
			}
		}()
	}
	select {
	case <-results:
		t.Error("did not expect subsequent calls to starting to return")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	myError := fmt.Errorf("hello world")
	publisher.started(myError)
	for i := 0; i < numWaiters; i++ {
		select {
		case r := <-results:
			if r.proceed || r.err != myError {
				t.Errorf("got bad response: proceed %t, error %s", r.proceed, r.err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for results to be published")
		}
	}
}

func TestMessagePublisherTerminateIdempotence(t *testing.T) {
	publisher := &basicMessagePublisher{}
	publisher.construct(&mockInternalPublisher{})
	publisher.starting()
	publisher.started(nil)
	proceed, err := publisher.terminate()
	if !proceed || err != nil {
		t.Errorf("expected first call to terminate to be successful, got proceed %t and error %s", proceed, err)
	}
	numWaiters := 3
	results := make(chan result, numWaiters)
	for i := 0; i < numWaiters; i++ {
		go func() {
			proceed, err := publisher.terminate()
			results <- result{
				proceed: proceed,
				err:     err,
			}
		}()
	}
	select {
	case <-results:
		t.Error("did not expect subsequent calls to terminate to return")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	myError := fmt.Errorf("hello world")
	publisher.terminated(myError)
	for i := 0; i < numWaiters; i++ {
		select {
		case r := <-results:
			if r.proceed || r.err != myError {
				t.Errorf("got bad response: proceed %t, error %s", r.proceed, r.err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for results to be published")
		}
	}
}

func TestMessagePublisherStartAtomic(t *testing.T) {
	first := true
	notify := make(chan struct{})
	isRunningDelayed := func() bool {
		if first {
			first = false
			<-notify
		}
		return true
	}
	publisher := &basicMessagePublisher{}
	publisher.construct(&mockInternalPublisher{
		isRunning: isRunningDelayed,
	})
	firstStart := make(chan result)
	go func() {
		proceed, err := publisher.starting()
		firstStart <- result{
			proceed: proceed,
			err:     err,
		}
	}()
	// give the goroutine above a chance to start
	<-time.After(10 * time.Millisecond)
	proceed, err := publisher.starting()
	if !proceed || err != nil {
		t.Errorf("expected procceed to be true and err to be nil, got proceed: %t, err: %s", proceed, err)
	}
	close(notify)
	select {
	case <-firstStart:
		t.Error("did not expect first call to starting to return")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	myError := fmt.Errorf("hello world")
	publisher.started(myError)
	select {
	case r := <-firstStart:
		if r.proceed || r.err != myError {
			t.Errorf("got bad response: proceed %t, error %s", r.proceed, r.err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for results to be published")
	}
}

// can't test terminate in the same way as start..

func TestStartWhenInternalPublisherIsNotRunning(t *testing.T) {
	publisher := &basicMessagePublisher{}
	publisher.construct(&mockInternalPublisher{
		isRunning: func() bool {
			return false
		},
	})
	proceed, err := publisher.starting()
	if proceed {
		t.Error("expected publisher to not start when internal publisher is not started")
	}
	if err == nil {
		t.Error("expected error to not be nil and instead indicate that internal publisher is down")
	}
}

type result struct {
	proceed bool
	err     error
}

type mockInternalPublisher struct {
	publish                      func(message core.Publishable) core.ErrorInfo
	events                       func() core.Events
	requestor                    func() core.Requestor
	awaitWritable                func(terminateSignal chan struct{}) error
	taskQueue                    func() chan core.SendTask
	isRunning                    func() bool
	incrementMetric              func(metric core.NextGenMetric, amount uint64)
	addAcknowledgementHandler    func(core.AcknowledgementHandler) (uint64, func() (messageId uint64, correlationTag []byte))
	removeAcknowledgementHandler func(uint64)
}

func (mock *mockInternalPublisher) Publish(message core.Publishable) core.ErrorInfo {
	if mock.publish != nil {
		return mock.publish(message)
	}
	return nil
}

func (mock *mockInternalPublisher) Events() core.Events {
	if mock.events != nil {
		return mock.events()
	}
	return &mockEvents{}
}

func (mock *mockInternalPublisher) Requestor() core.Requestor {
	if mock.requestor != nil {
		return mock.requestor()
	}
	return &mockRequestor{}
}

func (mock *mockInternalPublisher) AwaitWritable(terminateSignal chan struct{}) error {
	if mock.awaitWritable != nil {
		return mock.awaitWritable(terminateSignal)
	}
	return nil
}

func (mock *mockInternalPublisher) TaskQueue() chan core.SendTask {
	if mock.taskQueue != nil {
		return mock.taskQueue()
	}
	return nil
}

func (mock *mockInternalPublisher) IsRunning() bool {
	if mock.isRunning != nil {
		return mock.isRunning()
	}
	return true
}

func (mock *mockInternalPublisher) IncrementMetric(metric core.NextGenMetric, amount uint64) {
	if mock.incrementMetric != nil {
		mock.incrementMetric(metric, amount)
	}
}

func (mock *mockInternalPublisher) Acknowledgements() core.Acknowledgements {
	return mock
}

func (mock *mockInternalPublisher) AddAcknowledgementHandler(ackHandler core.AcknowledgementHandler) (uint64, func() (messageId uint64, correlationTag []byte)) {
	if mock.addAcknowledgementHandler != nil {
		return mock.addAcknowledgementHandler(ackHandler)
	}
	return 0, func() (messageId uint64, correlationTag []byte) {
		return 0, []byte{}
	}
}

func (mock *mockInternalPublisher) RemoveAcknowledgementHandler(pubID uint64) {
	if mock.removeAcknowledgementHandler != nil {
		mock.removeAcknowledgementHandler(pubID)
	}
}

type mockEvents struct {
}

func (events *mockEvents) AddEventHandler(sessionEvent core.Event, responseCode core.EventHandler) uint {
	return 0
}

func (events *mockEvents) RemoveEventHandler(id uint) {
}

type mockRequestor struct {
	createReplyToTopic          func(publisherID string) string
	addRequestorReplyHandler    func(replyHandler core.RequestorReplyHandler) (string, func() (messageID uint64, correlationId string), core.ErrorInfo)
	removeRequestorReplyHandler func(replyToTopic string) core.ErrorInfo
}

func (requestor *mockRequestor) CreateReplyToTopic(publisherID string) string {
	if requestor.createReplyToTopic != nil {
		return requestor.createReplyToTopic(publisherID)
	}
	return ""
}

func (requestor *mockRequestor) AddRequestorReplyHandler(replyHandler core.RequestorReplyHandler) (string, func() (messageID uint64, correlationID string), core.ErrorInfo) {
	if requestor.addRequestorReplyHandler != nil {
		return requestor.addRequestorReplyHandler(replyHandler)
	}
	return "", func() (uint64, string) {
		return uint64(0), ""
	}, nil
}

func (requestor *mockRequestor) RemoveRequestorReplyHandler(replyToTopic string) core.ErrorInfo {
	if requestor.removeRequestorReplyHandler != nil {
		return requestor.removeRequestorReplyHandler(replyToTopic)
	}
	return nil
}
