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

package receiver

import (
	"fmt"
	"testing"
	"time"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/logging"
	messageimpl "solace.dev/go/messaging/internal/impl/message"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
)

func TestPersistentBuilderWithSubscriptions(t *testing.T) {
	builder := NewPersistentMessageReceiverBuilderImpl(nil)
	subscriptions := []resource.Subscription{resource.TopicSubscriptionOf("mytopic")}
	builder.WithSubscriptions(subscriptions...)
	receiver, err := builder.Build(resource.QueueDurableNonExclusive("hello"))
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*persistentMessageReceiverImpl)
	if !ok {
		t.Error("expected to get a persistentMessageReceiverImpl returned")
	}
	for i, subscription := range receiverImpl.subscriptions {
		if subscriptions[i].GetName() != subscription {
			t.Error("expected to get list of matching subscriptions")
		}
	}
}

func TestPersistentBuilderSetReplay(t *testing.T) {
	builder := &persistentMessageReceiverBuilderImpl{
		internalReceiver: nil,
		properties:       constants.DefaultPersistentReceiverProperties.GetConfiguration(),
	}
	builder.WithMessageReplay(config.ReplayStrategyAllMessages())
	if builder.properties[config.ReceiverPropertyPersistentMessageReplayStrategy] != config.PersistentReplayAll {
		t.Error(config.ReceiverPropertyPersistentMessageReplayStrategy + " was not set to " + config.PersistentReplayAll)
	}
	_, err := builder.Build(resource.QueueDurableNonExclusive("hello"))
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	time := time.Now()
	builder.WithMessageReplay(config.ReplayStrategyTimeBased(time))
	if builder.properties[config.ReceiverPropertyPersistentMessageReplayStrategy] != config.PersistentReplayTimeBased {
		t.Error(config.ReceiverPropertyPersistentMessageReplayStrategy + " was not set to " + config.PersistentReplayTimeBased)
	}
	if builder.properties[config.ReceiverPropertyPersistentMessageReplayStrategyTimeBasedStartTime] != time {
		t.Error("got unexpected value for " + string(config.ReceiverPropertyPersistentMessageReplayStrategyTimeBasedStartTime) +
			": " + fmt.Sprint(builder.properties[config.ReceiverPropertyPersistentMessageReplayStrategyTimeBasedStartTime]))
	}
	_, err = builder.Build(resource.QueueDurableNonExclusive("hello"))
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	msgID := "rmid1:0d77c-b0b2e66aece-00000000-00000001"
	rmidFromString, err := messageimpl.ReplicationGroupMessageIDFromString(msgID)
	if err != nil {
		t.Error(err)
	}
	builder.WithMessageReplay(config.ReplayStrategyReplicationGroupMessageID(rmidFromString))
	if builder.properties[config.ReceiverPropertyPersistentMessageReplayStrategy] != config.PersistentReplayIDBased {
		t.Error(config.ReceiverPropertyPersistentMessageReplayStrategy + " was not set to " + config.PersistentReplayIDBased)
	}
	if builder.properties[config.ReceiverPropertyPersistentMessageReplayStrategyIDBasedReplicationGroupMessageID] != rmidFromString {
		t.Error("got unexpected value for " + string(config.ReceiverPropertyPersistentMessageReplayStrategyIDBasedReplicationGroupMessageID) +
			": " + fmt.Sprint(builder.properties[config.ReceiverPropertyPersistentMessageReplayStrategyIDBasedReplicationGroupMessageID]))
	}
	_, err = builder.Build(resource.QueueDurableNonExclusive("hello"))
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
}

func TestPersistentMessageReceiverImplLifecycle(t *testing.T) {
	gracePeriod := 10 * time.Second

	// parameterize this test with the various start and terminate functions (sync/async)
	startAndTerminatFunctions := []struct {
		start     func(receiver *persistentMessageReceiverImpl)
		terminate func(receiver *persistentMessageReceiverImpl)
	}{
		{
			start: func(receiver *persistentMessageReceiverImpl) {
				err := receiver.Start()
				if err != nil {
					t.Error("expected error to be nil, got " + err.Error())
				}
			},
			terminate: func(receiver *persistentMessageReceiverImpl) {
				err := receiver.Terminate(gracePeriod)
				if err != nil {
					t.Error("expected error to be nil, got " + err.Error())
				}
			},
		},
		{
			start: func(receiver *persistentMessageReceiverImpl) {
				select {
				case err := <-receiver.StartAsync():
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
				case <-time.After(100 * time.Millisecond):
					t.Error("timed out waiting for receiver to start")
				}
			},
			terminate: func(receiver *persistentMessageReceiverImpl) {
				select {
				case err := <-receiver.TerminateAsync(gracePeriod):
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
				case <-time.After(gracePeriod + 5*time.Second):
					t.Error("timed out waiting for receiver to terminate")
				}
			},
		},
		{
			start: func(receiver *persistentMessageReceiverImpl) {
				done := make(chan struct{})
				receiver.StartAsyncCallback(func(retPub solace.PersistentMessageReceiver, err error) {
					if receiver != retPub {
						t.Error("got a different receiver returned to the start callback")
					}
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
					close(done)
				})
				select {
				case <-done:
					// success
				case <-time.After(100 * time.Millisecond):
					t.Error("timed out waiting for persistent receiver to start")
				}
			},
			terminate: func(receiver *persistentMessageReceiverImpl) {
				done := make(chan struct{})
				receiver.TerminateAsyncCallback(gracePeriod, func(err error) {
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
					close(done)
				})
				select {
				case <-done:
					// success
				case <-time.After(100 * time.Millisecond):
					t.Error("timed out waiting for persistent receiver to start")
				}
			},
		},
	}
	for _, fns := range startAndTerminatFunctions {
		subscriptions := []resource.Subscription{
			resource.TopicSubscriptionOf("hello world"),
			resource.TopicSubscriptionOf("goodbye world"),
		}
		internalFlow := &mockPersistentReceiver{}
		internalReceiver := &mockInternalReceiver{}
		internalReceiver.newPersistentReceiver = func(props []string, callback core.RxCallback, eventCallback core.PersistentEventCallback) (core.PersistentReceiver, core.ErrorInfo) {
			return internalFlow, nil
		}
		subscribeCalled := false
		subscribeCount := 0
		internalFlow.subscribe = func(topic string) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			if subscribeCount == len(subscriptions)-1 {
				subscribeCalled = true
			}
			if subscriptions[subscribeCount].GetName() != topic {
				t.Errorf("got unexpected subscription name: %s", topic)
			}
			subscribeCount++
			c := make(chan core.SubscriptionEvent, 1)
			c <- mockSubscriptionEvent{}
			return 0, c, nil
		}
		destroyCalled := false
		internalFlow.destroy = func(freeMemory bool) core.ErrorInfo {
			destroyCalled = true
			return nil
		}

		receiver := &persistentMessageReceiverImpl{}
		receiver.construct(
			&persistentMessageReceiverProps{
				flowProperties:       []string{},
				internalReceiver:     internalReceiver,
				startupSubscriptions: subscriptions,
				bufferHighwater:      50,
				bufferLowwater:       40,
				endpoint:             resource.QueueDurableExclusive("hello"),
			},
		)

		// pre start
		if receiver.IsRunning() {
			t.Error("expected receiver to not be running")
		}
		if receiver.IsTerminating() {
			t.Error("expected terminating to be false, was true")
		}
		if receiver.IsTerminated() {
			t.Error("expected receiver to not yet be terminated")
		}

		// start
		fns.start(receiver)
		// check started states
		if !receiver.IsRunning() {
			t.Error("expected receiver to be running, it was not")
		}
		if receiver.IsTerminating() {
			t.Error("expected terminating to be false, was true")
		}
		if receiver.IsTerminated() {
			t.Error("expected receiver to not yet be terminated")
		}
		if !subscribeCalled {
			t.Error("receiver did not register subscriptions on startup")
		}

		msgP, err := ccsmp.SolClientMessageAlloc()
		if err != nil {
			t.Errorf("got error allocating message: %s", err.GetMessageAsString())
		}
		called := make(chan struct{})
		receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
			if _, ok := inboundMessage.(*messageimpl.InboundMessageImpl); !ok {
				t.Errorf("inbound message was not of type *InboundMessageImpl, was %T", inboundMessage)
			}
			close(called)
		})
		receiver.messageCallback(msgP)
		select {
		case <-called:
			// success
		case <-time.After(10 * time.Millisecond):
			t.Error("timed out waiting for mesasge handler to be called")
		}

		// terminate
		fns.terminate(receiver)
		// check terminated states
		if receiver.IsRunning() {
			t.Error("expected receiver to not be running")
		}
		if receiver.IsTerminating() {
			t.Error("expected receiver to not be terminating")
		}
		if !receiver.IsTerminated() {
			t.Error("expected receiver to be terminated")
		}
		if !destroyCalled {
			t.Error("expected flow to be destroyed")
		}
	}
}

func TestPersistentBuilderWithInvalidSubscriptionType(t *testing.T) {
	builder := NewPersistentMessageReceiverBuilderImpl(&mockInternalReceiver{})
	builder.WithSubscriptions(&notASubscription{})
	receiver, err := builder.Build(resource.QueueDurableNonExclusive("hello"))
	if _, ok := err.(*solace.IllegalArgumentError); !ok {
		t.Errorf("expected illegal argument error, got %s", err)
	}
	if receiver != nil {
		t.Error("expected receiver to equal nil, it was not")
	}
}

func TestPersistentReceiverSubscribeWithInvalidSubscriptionType(t *testing.T) {
	receiver := persistentMessageReceiverImpl{
		subscriptions:        []string{},
		basicMessageReceiver: basicMessageReceiver{internalReceiver: &mockInternalReceiver{}, state: messageReceiverStateStarted},
	}
	subscriptionFunctions := []func(subscription resource.Subscription) error{
		func(subscription resource.Subscription) error {
			return receiver.AddSubscription(subscription)
		},
		func(subscription resource.Subscription) error {
			return receiver.AddSubscriptionAsync(subscription, func(passedSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
				t.Error("did not expect callback to be called")
			})
		},
	}
	for _, fn := range subscriptionFunctions {
		err := fn(&notASubscription{})
		if _, ok := err.(*solace.IllegalArgumentError); !ok {
			t.Errorf("expected illegal argument error, got %s", err)
		}
	}
}

func TestPersistentReceiverSubscribe(t *testing.T) {
	internalFlow := &mockPersistentReceiver{}
	receiver := persistentMessageReceiverImpl{
		logger:                        logging.Default,
		internalFlow:                  internalFlow,
		basicMessageReceiver:          basicMessageReceiver{internalReceiver: &mockInternalReceiver{}, state: messageReceiverStateStarted},
		outstandingSubscriptionEvents: make(map[uintptr]struct{}),
	}
	subscriptionFunctions := []func(subscription resource.Subscription) error{
		func(subscription resource.Subscription) error {
			return receiver.AddSubscription(subscription)
		},
		func(subscription resource.Subscription) error {
			errChan := make(chan error)
			receiver.AddSubscriptionAsync(subscription, func(passedSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
				if passedSubscription != subscription {
					t.Error("expected subscriptions to be equal, they were not")
				}
				if operation != solace.SubscriptionAdded {
					t.Errorf("expected oepration to be SubscriptionAdded, got %d", operation)
				}
				errChan <- errOrNil
			})
			select {
			case ret := <-errChan:
				return ret
			case <-time.After(10 * time.Millisecond):
				t.Error("timed out wiating for callback to be called")
				return nil
			}
		},
	}
	topic := "some topic"
	subscription := resource.TopicSubscriptionOf(topic)
	for _, fn := range subscriptionFunctions {
		receiver.subscriptions = []string{}
		internalReceiverCalled := false
		internalFlow.subscribe = func(passedTopic string) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			if topic != passedTopic {
				t.Errorf("expected topic to equal %s, got %s", topic, passedTopic)
			}
			internalReceiverCalled = true
			c := make(chan core.SubscriptionEvent, 1)
			c <- mockSubscriptionEvent{}
			return 0, c, nil
		}
		err := fn(subscription)
		if err != nil {
			t.Errorf("expected error to be nil, got %s", err)
		}
		if !internalReceiverCalled {
			t.Error("internal subscribe not called")
		}
		if len(receiver.subscriptions) == 0 || receiver.subscriptions[0] != topic {
			t.Error("expected subscription to be added to receiver subscriptions")
		}
	}
}

func TestPersistentReceiverSubscribeWithError(t *testing.T) {
	internalFlow := &mockPersistentReceiver{}
	receiver := persistentMessageReceiverImpl{
		logger:                        logging.Default,
		internalFlow:                  internalFlow,
		basicMessageReceiver:          basicMessageReceiver{internalReceiver: &mockInternalReceiver{}, state: messageReceiverStateStarted},
		outstandingSubscriptionEvents: make(map[uintptr]struct{}),
	}
	subscriptionFunctions := []func(subscription resource.Subscription) error{
		func(subscription resource.Subscription) error {
			return receiver.AddSubscription(subscription)
		},
		func(subscription resource.Subscription) error {
			errChan := make(chan error)
			receiver.AddSubscriptionAsync(subscription, func(passedSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
				if passedSubscription != subscription {
					t.Error("expected subscriptions to be equal, they were not")
				}
				if operation != solace.SubscriptionAdded {
					t.Errorf("expected oepration to be SubscriptionAdded, got %d", operation)
				}
				errChan <- errOrNil
			})
			select {
			case ret := <-errChan:
				return ret
			case <-time.After(10 * time.Millisecond):
				t.Error("timed out wiating for callback to be called")
				return nil
			}
		},
	}
	topic := "some topic"
	subscription := resource.TopicSubscriptionOf(topic)
	subCode := 123
	solClientErr := ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCode(0),
		ccsmp.SolClientSubCode(subCode),
		ccsmp.SolClientResponseCode(0),
		"This is a generated error ErrorInfo")
	for _, fn := range subscriptionFunctions {
		receiver.subscriptions = []string{}
		internalReceiverCalled := false
		internalFlow.subscribe = func(passedTopic string) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			if topic != passedTopic {
				t.Errorf("expected topic to equal %s, got %s", topic, passedTopic)
			}
			internalReceiverCalled = true
			c := make(chan core.SubscriptionEvent, 1)
			c <- mockSubscriptionEvent{core.ToNativeError(solClientErr)}
			return 0, c, nil
		}
		err := fn(subscription)
		if internalErr, ok := err.(*solace.NativeError); ok {
			if internalErr.SubCode() != subcode.Code(123) {
				t.Errorf("expected sub code to be 123, got %d", internalErr.SubCode())
			}
		} else {
			t.Errorf("expected error to be nil, got %s", err)
		}
		if !internalReceiverCalled {
			t.Error("internal subscribe not called")
		}
	}
}

func TestPersistentUnsubscribeWithInvalidSubscriptionType(t *testing.T) {
	receiver := persistentMessageReceiverImpl{
		logger:                        logging.Default,
		subscriptions:                 []string{},
		basicMessageReceiver:          basicMessageReceiver{internalReceiver: &mockInternalReceiver{}, state: messageReceiverStateStarted},
		outstandingSubscriptionEvents: make(map[uintptr]struct{}),
	}
	subscriptionFunctions := []func(subscription resource.Subscription) error{
		func(subscription resource.Subscription) error {
			return receiver.RemoveSubscription(subscription)
		},
		func(subscription resource.Subscription) error {
			return receiver.RemoveSubscriptionAsync(subscription, func(passedSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
				t.Error("did not expect callback to be called")
			})
		},
	}
	for _, fn := range subscriptionFunctions {
		err := fn(&notASubscription{})
		if _, ok := err.(*solace.IllegalArgumentError); !ok {
			t.Errorf("expected illegal argument error, got %s", err)
		}
	}
}

func TestPersistentReceiverUnsubscribe(t *testing.T) {
	internalFlow := &mockPersistentReceiver{}
	receiver := persistentMessageReceiverImpl{
		logger:                        logging.Default,
		internalFlow:                  internalFlow,
		subscriptions:                 []string{},
		basicMessageReceiver:          basicMessageReceiver{internalReceiver: &mockInternalReceiver{}, state: messageReceiverStateStarted},
		outstandingSubscriptionEvents: make(map[uintptr]struct{}),
	}
	unsubscriptionFunctions := []func(subscription resource.Subscription) error{
		func(subscription resource.Subscription) error {
			return receiver.RemoveSubscription(subscription)
		},
		func(subscription resource.Subscription) error {
			errChan := make(chan error)
			receiver.RemoveSubscriptionAsync(subscription, func(passedSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
				if passedSubscription != subscription {
					t.Error("expected subscriptions to be equal, they were not")
				}
				if operation != solace.SubscriptionRemoved {
					t.Errorf("expected oepration to be SubscriptionRemoved, got %d", operation)
				}
				errChan <- errOrNil
			})
			select {
			case ret := <-errChan:
				return ret
			case <-time.After(10 * time.Millisecond):
				t.Error("timed out wiating for callback to be called")
				return nil
			}
		},
	}
	topic := "some topic"
	subscription := resource.TopicSubscriptionOf(topic)
	for _, fn := range unsubscriptionFunctions {
		receiver.subscriptions = []string{topic}
		internalReceiverCalled := false
		internalFlow.unsubscribe = func(passedTopic string) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			if topic != passedTopic {
				t.Errorf("expected topic to equal %s, got %s", topic, passedTopic)
			}
			internalReceiverCalled = true
			c := make(chan core.SubscriptionEvent, 1)
			c <- mockSubscriptionEvent{}
			return 0, c, nil
		}
		err := fn(subscription)
		if err != nil {
			t.Errorf("expected error to be nil, got %s", err)
		}
		if !internalReceiverCalled {
			t.Error("internal unsubscribe not called")
		}
		if len(receiver.subscriptions) > 0 {
			t.Error("expected subscription to be removed")
		}
	}
}

func TestPersistentReceiverUnsubscribeWithError(t *testing.T) {
	internalFlow := &mockPersistentReceiver{}
	receiver := persistentMessageReceiverImpl{
		logger:                        logging.Default,
		internalFlow:                  internalFlow,
		subscriptions:                 []string{},
		basicMessageReceiver:          basicMessageReceiver{internalReceiver: &mockInternalReceiver{}, state: messageReceiverStateStarted},
		outstandingSubscriptionEvents: make(map[uintptr]struct{}),
	}
	unsubscriptionFunctions := []func(subscription resource.Subscription) error{
		func(subscription resource.Subscription) error {
			return receiver.RemoveSubscription(subscription)
		},
		func(subscription resource.Subscription) error {
			errChan := make(chan error)
			receiver.RemoveSubscriptionAsync(subscription, func(passedSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
				if passedSubscription != subscription {
					t.Error("expected subscriptions to be equal, they were not")
				}
				if operation != solace.SubscriptionRemoved {
					t.Errorf("expected oepration to be SubscriptionRemoved, got %d", operation)
				}
				errChan <- errOrNil
			})
			select {
			case ret := <-errChan:
				return ret
			case <-time.After(10 * time.Millisecond):
				t.Error("timed out wiating for callback to be called")
				return nil
			}
		},
	}
	topic := "some topic"
	subscription := resource.TopicSubscriptionOf(topic)
	subCode := 123
	solClientErr := ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCode(0),
		ccsmp.SolClientSubCode(subCode),
		ccsmp.SolClientResponseCode(0),
		"This is a generated error info")
	for _, fn := range unsubscriptionFunctions {
		receiver.subscriptions = []string{topic}
		internalReceiverCalled := false
		internalFlow.unsubscribe = func(passedTopic string) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			if topic != passedTopic {
				t.Errorf("expected topic to equal %s, got %s", topic, passedTopic)
			}
			internalReceiverCalled = true
			c := make(chan core.SubscriptionEvent, 1)
			c <- mockSubscriptionEvent{core.ToNativeError(solClientErr)}
			return 0, c, nil
		}
		err := fn(subscription)
		if internalErr, ok := err.(*solace.NativeError); ok {
			if internalErr.SubCode() != subcode.Code(123) {
				t.Errorf("expected sub code to be 123, got %d", internalErr.SubCode())
			}
		} else {
			t.Errorf("expected error to be nil, got %s", err)
		}
		if !internalReceiverCalled {
			t.Error("internal unsubscribe not called")
		}
	}
}

func TestPersistentReceiverSubscriptionsInBadStates(t *testing.T) {
	states := []messageReceiverState{messageReceiverStateNotStarted, messageReceiverStateStarting, messageReceiverStateTerminating, messageReceiverStateTerminated}
	for _, state := range states {
		receiver := &persistentMessageReceiverImpl{
			basicMessageReceiver: basicMessageReceiver{
				state: state,
			},
		}
		fns := []func(resource.Subscription) error{
			receiver.AddSubscription,
			receiver.RemoveSubscription,
			func(s resource.Subscription) error {
				return receiver.AddSubscriptionAsync(s, nil)
			},
			func(s resource.Subscription) error {
				return receiver.RemoveSubscriptionAsync(s, nil)
			},
		}
		for _, fn := range fns {
			err := fn(resource.TopicSubscriptionOf("some strings"))
			if _, ok := err.(*solace.IllegalStateError); !ok {
				t.Errorf("expected illegal state error when in state %d", state)
			}
		}
	}
}

func TestPersistentReceiverAsyncFunctionsWithNilCallback(t *testing.T) {
	internalFlow := &mockPersistentReceiver{}
	receiver := persistentMessageReceiverImpl{
		logger:                        logging.Default,
		internalFlow:                  internalFlow,
		basicMessageReceiver:          basicMessageReceiver{internalReceiver: &mockInternalReceiver{}, state: messageReceiverStateStarted},
		outstandingSubscriptionEvents: make(map[uintptr]struct{}),
	}
	subscription := resource.TopicSubscriptionOf("hello world")
	calledChan := make(chan struct{})
	internalFlow.subscribe = func(topic string) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
		if topic != subscription.GetName() {
			t.Error("got wrong subscription")
		}
		close(calledChan)
		c := make(chan core.SubscriptionEvent, 1)
		c <- mockSubscriptionEvent{}
		return 0, c, nil
	}
	err := receiver.AddSubscriptionAsync(subscription, nil)
	if err != nil {
		t.Errorf("got unexpected error on add subscription: %s", err)
	}
	select {
	case <-calledChan:
		// success
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for internal receiver subscribe to be called")
	}

	calledChan = make(chan struct{})
	internalFlow.unsubscribe = func(topic string) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
		if topic != subscription.GetName() {
			t.Error("got wrong subscription")
		}
		close(calledChan)
		c := make(chan core.SubscriptionEvent, 1)
		c <- mockSubscriptionEvent{}
		return 0, c, nil
	}
	err = receiver.RemoveSubscriptionAsync(subscription, nil)
	if err != nil {
		t.Errorf("got unexpected error on add subscription: %s", err)
	}
	select {
	case <-calledChan:
		// success
	case <-time.After(10 * time.Millisecond):
		t.Error("timed out waiting for internal receiver subscribe to be called")
	}
}

func TestPersistentReceiverSetCallbackInBadState(t *testing.T) {
	states := []messageReceiverState{messageReceiverStateTerminating, messageReceiverStateTerminated}
	for _, state := range states {
		receiver := &persistentMessageReceiverImpl{
			basicMessageReceiver: basicMessageReceiver{
				state: state,
			},
		}
		err := receiver.ReceiveAsync(nil)
		if _, ok := err.(*solace.IllegalStateError); !ok {
			t.Errorf("expected illegal state error when in state %d", state)
		}
	}
}

func TestPersistentReceiverRaceWithClosedBuffer(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &persistentMessageReceiverImpl{
		logger: logging.Default,
		basicMessageReceiver: basicMessageReceiver{
			internalReceiver: internalReceiver,
			state:            messageReceiverStateStarted,
		},
	}
	msgP, err := ccsmp.SolClientMessageAlloc()
	if err != nil {
		t.Error(err)
	}
	receiver.buffer = make(chan ccsmp.SolClientMessagePt, 1)
	close(receiver.buffer)
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("did not expect message callback to panic, but it did with panic: %s", r)
		}
	}()
	receiver.messageCallback(msgP)
}

func TestPersistentReceiverTerminateWithSlowConsumer(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &persistentMessageReceiverImpl{}
	receiver.construct(
		&persistentMessageReceiverProps{
			flowProperties:       []string{},
			internalReceiver:     internalReceiver,
			startupSubscriptions: []resource.Subscription{},
			bufferHighwater:      50,
			bufferLowwater:       40,
			endpoint:             resource.QueueDurableExclusive("hello"),
		},
	)
	blocker := make(chan struct{})
	receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
		<-blocker
	})
	receiver.Start()
	msgP, err := ccsmp.SolClientMessageAlloc()
	if err != nil {
		t.Error(err)
	}
	receiver.messageCallback(msgP)
	terminateChan := receiver.TerminateAsync(100 * time.Millisecond)
	select {
	case <-terminateChan:
		t.Error("expected termiantion to block until complete")
	case <-time.After(200 * time.Millisecond):
		// success
	}
	close(blocker)
	select {
	case err := <-terminateChan:
		if err != nil {
			t.Errorf("expected error %s to be nil", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timed out waiting for terminate to time out")
	}
}

func TestPersistentReceiverPauseMessageReception(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	persistentReceiver := &mockPersistentReceiver{}
	internalReceiver.newPersistentReceiver = func(props []string, callback core.RxCallback, eventCallback core.PersistentEventCallback) (core.PersistentReceiver, core.ErrorInfo) {
		return persistentReceiver, nil
	}
	wasStarted := false
	wasStopped := false
	persistentReceiver.start = func() core.ErrorInfo {
		wasStarted = true
		return nil
	}
	persistentReceiver.stop = func() core.ErrorInfo {
		wasStopped = true
		return nil
	}
	receiver := &persistentMessageReceiverImpl{}
	bufferHighwater := 10
	bufferLowwater := 5
	receiver.construct(
		&persistentMessageReceiverProps{
			flowProperties:       []string{},
			internalReceiver:     internalReceiver,
			startupSubscriptions: []resource.Subscription{},
			bufferHighwater:      bufferHighwater,
			bufferLowwater:       bufferLowwater,
			endpoint:             resource.QueueDurableExclusive("hello"),
		},
	)
	receiver.Start()
	msgP, err := ccsmp.SolClientMessageAlloc()
	if err != nil {
		t.Error(err)
	}
	for i := 0; i <= bufferHighwater; i++ {
		receiver.messageCallback(msgP)
	}
	if !wasStopped {
		t.Error("expected flow to be stopped")
	}
	for i := 0; i <= bufferHighwater-bufferLowwater; i++ {
		receiver.ReceiveMessage(100 * time.Millisecond)
	}
	if !wasStarted {
		t.Error("expected flow to be started")
	}
	for i := 0; i < bufferLowwater; i++ {
		receiver.ReceiveMessage(100 * time.Millisecond)
	}

	terminateChan := receiver.TerminateAsync(100 * time.Millisecond)
	select {
	case err := <-terminateChan:
		if err != nil {
			t.Errorf("expected error %s to be nil", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timed out waiting for terminate to time out")
	}
}

func TestPersistentReceiverPauseMessageReceptionAsyncReceive(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	persistentReceiver := &mockPersistentReceiver{}
	internalReceiver.newPersistentReceiver = func(props []string, callback core.RxCallback, eventCallback core.PersistentEventCallback) (core.PersistentReceiver, core.ErrorInfo) {
		return persistentReceiver, nil
	}
	wasStarted := false
	wasStopped := false
	persistentReceiver.start = func() core.ErrorInfo {
		wasStarted = true
		return nil
	}
	persistentReceiver.stop = func() core.ErrorInfo {
		wasStopped = true
		return nil
	}
	receiver := &persistentMessageReceiverImpl{}
	bufferHighwater := 10
	bufferLowwater := 5
	receiver.construct(
		&persistentMessageReceiverProps{
			flowProperties:       []string{},
			internalReceiver:     internalReceiver,
			startupSubscriptions: []resource.Subscription{},
			bufferHighwater:      bufferHighwater,
			bufferLowwater:       bufferLowwater,
			endpoint:             resource.QueueDurableExclusive("hello"),
		},
	)
	receiver.Start()
	msgP, err := ccsmp.SolClientMessageAlloc()
	if err != nil {
		t.Error(err)
	}
	for i := 0; i <= bufferHighwater; i++ {
		receiver.messageCallback(msgP)
	}
	if !wasStopped {
		t.Error("expected flow to be stopped")
	}
	isDone := make(chan struct{})
	msgsReceived := 0
	receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
		msgsReceived++
		if msgsReceived == bufferHighwater {
			close(isDone)
		}
	})
	select {
	case <-isDone:
		// success
	case <-time.After(10 * time.Second):
		t.Error("timed out waiting for all messages to be received")
	}
	if !wasStarted {
		t.Error("expected flow to be started")
	}
	for i := 0; i < bufferLowwater; i++ {
		receiver.ReceiveMessage(100 * time.Millisecond)
	}

	terminateChan := receiver.TerminateAsync(100 * time.Millisecond)
	select {
	case err := <-terminateChan:
		if err != nil {
			t.Errorf("expected error %s to be nil", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timed out waiting for terminate to time out")
	}
}

func TestPersistentReceiverTerminateWithUndeliveredMessages(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &persistentMessageReceiverImpl{}
	receiver.construct(
		&persistentMessageReceiverProps{
			flowProperties:       []string{},
			internalReceiver:     internalReceiver,
			startupSubscriptions: []resource.Subscription{},
			bufferHighwater:      50,
			bufferLowwater:       40,
			endpoint:             resource.QueueDurableExclusive("hello"),
		},
	)
	blocker := make(chan struct{})
	receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
		<-blocker
	})
	metricsIncremented := false
	expectedDropped := 2
	internalReceiver.incrementMetric = func(metric core.NextGenMetric, amount uint64) {
		if metric != core.MetricReceivedMessagesTerminationDiscarded {
			t.Errorf("expected metric to be %d, got %d", core.MetricReceivedMessagesTerminationDiscarded, metric)
		}
		if amount != uint64(expectedDropped) {
			t.Errorf("expected to see %d messages dropped, got %d", expectedDropped, amount)
		}
		metricsIncremented = true
	}
	receiver.Start()
	msgP, err := ccsmp.SolClientMessageAlloc()
	if err != nil {
		t.Error(err)
	}
	receiver.messageCallback(msgP)
	for i := 0; i < expectedDropped; i++ {
		receiver.messageCallback(msgP)
	}
	terminateChan := receiver.TerminateAsync(100 * time.Millisecond)
	select {
	case <-terminateChan:
		t.Error("expected termiantion to block until complete")
	case <-time.After(200 * time.Millisecond):
		// success
	}
	close(blocker)
	select {
	case err := <-terminateChan:
		if _, ok := err.(*solace.IncompleteMessageDeliveryError); !ok {
			t.Errorf("expected incomplete message delivery error, got %T", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timed out waiting for terminate to time out")
	}
	if !metricsIncremented {
		t.Error("Expected IncrementMetric to be called")
	}
}

func TestPersistentReceiverStartIdempotence(t *testing.T) {
	startFunctions := []func(receiver solace.PersistentMessageReceiver) <-chan error{
		func(receiver solace.PersistentMessageReceiver) <-chan error {
			c := make(chan error, 1)
			go func() {
				c <- receiver.Start()
			}()
			return c
		},
		func(receiver solace.PersistentMessageReceiver) <-chan error {
			return receiver.StartAsync()
		},
		func(receiver solace.PersistentMessageReceiver) <-chan error {
			c := make(chan error, 1)
			receiver.StartAsyncCallback(func(dmr solace.PersistentMessageReceiver, e error) {
				c <- e
			})
			return c
		},
	}
	for _, start := range startFunctions {
		internalFlow := &mockPersistentReceiver{}
		internalReceiver := &mockInternalReceiver{}
		internalReceiver.newPersistentReceiver = func(props []string, callback core.RxCallback, eventCallback core.PersistentEventCallback) (core.PersistentReceiver, core.ErrorInfo) {
			return internalFlow, nil
		}
		subscriptions := []resource.Subscription{
			resource.TopicSubscriptionOf("hello world"),
		}
		receiver := &persistentMessageReceiverImpl{}
		receiver.construct(&persistentMessageReceiverProps{
			flowProperties:       []string{},
			internalReceiver:     internalReceiver,
			startupSubscriptions: subscriptions,
			bufferHighwater:      50,
			bufferLowwater:       40,
			endpoint:             resource.QueueDurableExclusive("hello"),
		})

		callCount := 0
		subscribeBlock := make(chan struct{})
		internalFlow.subscribe = func(topic string) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			<-subscribeBlock
			callCount++
			c := make(chan core.SubscriptionEvent, 1)
			c <- mockSubscriptionEvent{}
			return 0, c, nil
		}
		first := start(receiver)
		second := start(receiver)

		select {
		case <-first:
			t.Error("did not expect channels to return")
		case <-second:
			t.Error("did not expect channels to return")
		case <-time.After(1 * time.Millisecond):
			// do nothing
		}
		close(subscribeBlock)
		select {
		case <-first:
			// success
		case <-time.After(10 * time.Millisecond):
			t.Error("timed out waiting for start to complete")
		}
		select {
		case <-second:
			// success
		case <-time.After(10 * time.Millisecond):
			t.Error("timed out waiting for second start to complete")
		}
		if callCount > 1 {
			t.Errorf("expected subscribe to be called once, was called %d times", callCount)
		}
		receiver.Terminate(10 * time.Millisecond)
	}
}

func TestPersistentMessageReceiverTerminateIdempotence(t *testing.T) {
	terminateFunctions := []func(receiver solace.PersistentMessageReceiver) <-chan error{
		func(receiver solace.PersistentMessageReceiver) <-chan error {
			c := make(chan error, 1)
			go func() {
				c <- receiver.Terminate(10 * time.Millisecond)
			}()
			return c
		},
		func(receiver solace.PersistentMessageReceiver) <-chan error {
			return receiver.TerminateAsync(10 * time.Millisecond)
		},
		func(receiver solace.PersistentMessageReceiver) <-chan error {
			c := make(chan error)
			receiver.TerminateAsyncCallback(10*time.Millisecond, func(e error) {
				c <- e
			})
			return c
		},
	}
	for _, terminate := range terminateFunctions {
		internalFlow := &mockPersistentReceiver{}
		internalReceiver := &mockInternalReceiver{}
		internalReceiver.newPersistentReceiver = func(props []string, callback core.RxCallback, eventCallback core.PersistentEventCallback) (core.PersistentReceiver, core.ErrorInfo) {
			return internalFlow, nil
		}
		subscriptions := []resource.Subscription{
			resource.TopicSubscriptionOf("hello world"),
		}
		receiver := &persistentMessageReceiverImpl{}
		receiver.construct(&persistentMessageReceiverProps{
			flowProperties:       []string{},
			internalReceiver:     internalReceiver,
			startupSubscriptions: subscriptions,
			bufferHighwater:      50,
			bufferLowwater:       40,
			endpoint:             resource.QueueDurableExclusive("hello"),
		})

		callCount := 0
		unsubscribeBlock := make(chan struct{})
		internalFlow.destroy = func(b bool) core.ErrorInfo {
			<-unsubscribeBlock
			callCount++
			return nil
		}
		receiver.Start()

		first := terminate(receiver)
		second := terminate(receiver)

		select {
		case <-first:
			t.Error("did not expect channels to return")
		case <-second:
			t.Error("did not expect channels to return")
		case <-time.After(1 * time.Millisecond):
			// do nothing
		}
		close(unsubscribeBlock)
		select {
		case <-first:
			// success
		case <-time.After(10 * time.Millisecond):
			t.Error("timed out waiting for start to complete")
		}
		select {
		case <-second:
			// success
		case <-time.After(10 * time.Millisecond):
			t.Error("timed out waiting for second terminate to complete")
		}
		if callCount > 1 {
			t.Errorf("expected subscribe to be called once, was called %d times", callCount)
		}
	}
}

func TestPersistentReceiverUnsolicitedTermination(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &persistentMessageReceiverImpl{}
	receiver.construct(&persistentMessageReceiverProps{
		flowProperties:       []string{},
		internalReceiver:     internalReceiver,
		startupSubscriptions: []resource.Subscription{},
		bufferHighwater:      50,
		bufferLowwater:       40,
		endpoint:             resource.QueueDurableExclusive("hello"),
	})

	expectedDropped := 2
	metricsIncremented := false
	internalReceiver.incrementMetric = func(metric core.NextGenMetric, amount uint64) {
		if metric != core.MetricReceivedMessagesTerminationDiscarded {
			t.Errorf("expected metric to be %d, got %d", core.MetricReceivedMessagesTerminationDiscarded, metric)
		}
		if amount != uint64(expectedDropped) {
			t.Errorf("expected to see %d messages dropped, got %d", expectedDropped, amount)
		}
		metricsIncremented = true
	}

	listenerCalled := make(chan error)
	receiver.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
		delta := time.Since(te.GetTimestamp())
		if delta < 0 || delta > 100*time.Millisecond {
			t.Errorf("Timestamp delta too large! Timestamp: %s, now: %s", te.GetTimestamp(), time.Now())
		}
		if !receiver.IsTerminated() {
			t.Error("Expected publisher to be terminated when notification listener is called")
		}
		if te.GetMessage() == "" {
			t.Error("Expected message in termination event")
		}
		listenerCalled <- te.GetCause()
	})
	blocker := make(chan struct{})
	receiveCalled := make(chan struct{})
	receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
		close(receiveCalled)
		<-blocker
	})
	receiver.Start()
	msgP, err := ccsmp.SolClientMessageAlloc()
	if err != nil {
		t.Error(err)
	}
	receiver.messageCallback(msgP)
	for i := 0; i < expectedDropped; i++ {
		receiver.messageCallback(msgP)
	}

	select {
	case <-receiveCalled:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("receive not called")
	}

	errForEvent := fmt.Errorf("some error")
	receiver.onDownEvent(&mockEvent{err: errForEvent})
	select {
	case err := <-listenerCalled:
		if err != errForEvent {
			t.Errorf("expected %s, got %s", errForEvent, err)
		}
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for termination listener to be called")
	}
	// allow thread to finish
	close(blocker)
	termErr := receiver.Terminate(100 * time.Millisecond)
	if _, ok := termErr.(*solace.IncompleteMessageDeliveryError); !ok {
		t.Errorf("expected incomplete message delivery error, got %s", err)
	}
	if !metricsIncremented {
		t.Error("metrics not incremented on incomplete delivery")
	}
}
