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
	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/logging"
	messageimpl "solace.dev/go/messaging/internal/impl/message"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
)

func TestBuilderWithInvalidBackpressureStrategy(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureStrategy: "notastrategy",
	})
	_, err := builder.Build()
	if err == nil {
		t.Error("expected to get error when building with invalid backpressure strategy")
	}
}

func TestBuilderWithInvalidBackpressureBufferSize(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureBufferCapacity: 0,
	})
	_, err := builder.Build()
	if err == nil {
		t.Error("expected to get error when building with invalid backpressure buffer size")
	}
}

func TestBuilderWithInvalidBackpressureBufferSizeType(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureBufferCapacity: "huh this isn't an int",
	})
	_, err := builder.Build()
	if err == nil {
		t.Error("expected to get error when building with invalid backpressure buffer size")
	}
}

func TestBuilderWithValidConfigurationMapLatestDrop(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureBufferCapacity: 2,
		config.ReceiverPropertyDirectBackPressureStrategy:       config.ReceiverBackPressureStrategyDropLatest,
	})
	receiver, err := builder.Build()
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*directMessageReceiverImpl)
	if !ok {
		t.Error("expected to get directMessageReceiverImpl back")
	}
	if receiverImpl.backpressureStrategy != strategyDropLatest {
		t.Error("expected to get backpressure drop oldest")
	}
	if cap(receiverImpl.buffer) != 2 {
		t.Error("expected buffer size of 2")
	}
}

func TestBuilderWithValidConfigurationMapOldestDrop(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureBufferCapacity: 2,
		config.ReceiverPropertyDirectBackPressureStrategy:       config.ReceiverBackPressureStrategyDropOldest,
	})
	receiver, err := builder.Build()
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*directMessageReceiverImpl)
	if !ok {
		t.Error("expected to get directMessageReceiverImpl back")
	}
	if receiverImpl.backpressureStrategy != strategyDropOldest {
		t.Error("expected to get backpressure drop oldest")
	}
	if cap(receiverImpl.buffer) != 2 {
		t.Error("expected buffer size of 2")
	}
}

func TestBuilderBuildWithConfigurationOldestDrop(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(nil)
	builder.OnBackPressureDropOldest(2)
	receiver, err := builder.Build()
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*directMessageReceiverImpl)
	if !ok {
		t.Error("expected to get directMessageReceiverImpl back")
	}
	if receiverImpl.backpressureStrategy != strategyDropOldest {
		t.Error("expected to get backpressure drop oldest")
	}
	if cap(receiverImpl.buffer) != 2 {
		t.Error("expected buffer size of 2")
	}
}

func TestBuilderBuildWithConfigurationLatestDrop(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(nil)
	builder.OnBackPressureDropLatest(2)
	receiver, err := builder.Build()
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*directMessageReceiverImpl)
	if !ok {
		t.Error("expected to get directMessageReceiverImpl back")
	}
	if receiverImpl.backpressureStrategy != strategyDropLatest {
		t.Error("expected to get backpressure drop oldest")
	}
	if cap(receiverImpl.buffer) != 2 {
		t.Error("expected buffer size of 2")
	}
}

func TestBuilderWithSubscriptions(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(nil)
	subscriptions := []resource.Subscription{resource.TopicSubscriptionOf("mytopic")}
	builder.WithSubscriptions(subscriptions...)
	receiver, err := builder.Build()
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*directMessageReceiverImpl)
	if !ok {
		t.Error("expected to get a directMessageReceiverImpl returned")
	}
	for i, subscription := range receiverImpl.subscriptions {
		if subscriptions[i].GetName() != subscription {
			t.Error("expected to get list of matching subscriptions")
		}
	}
}

func TestDirectMessageReceiverImplLifecycle(t *testing.T) {
	gracePeriod := 10 * time.Second

	// parameterize this test with the various start and terminate functions (sync/async)
	startAndTerminatFunctions := []struct {
		start     func(receiver *directMessageReceiverImpl)
		terminate func(receiver *directMessageReceiverImpl)
	}{
		{
			start: func(receiver *directMessageReceiverImpl) {
				err := receiver.Start()
				if err != nil {
					t.Error("expected error to be nil, got " + err.Error())
				}
			},
			terminate: func(receiver *directMessageReceiverImpl) {
				err := receiver.Terminate(gracePeriod)
				if err != nil {
					t.Error("expected error to be nil, got " + err.Error())
				}
			},
		},
		{
			start: func(receiver *directMessageReceiverImpl) {
				select {
				case err := <-receiver.StartAsync():
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
				case <-time.After(100 * time.Millisecond):
					t.Error("timed out waiting for receiver to start")
				}
			},
			terminate: func(receiver *directMessageReceiverImpl) {
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
			start: func(receiver *directMessageReceiverImpl) {
				done := make(chan struct{})
				receiver.StartAsyncCallback(func(retPub solace.DirectMessageReceiver, err error) {
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
					t.Error("timed out waiting for direct receiver to start")
				}
			},
			terminate: func(receiver *directMessageReceiverImpl) {
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
					t.Error("timed out waiting for direct receiver to start")
				}
			},
		},
	}
	for _, fns := range startAndTerminatFunctions {
		subscriptions := []resource.Subscription{
			resource.TopicSubscriptionOf("hello world"),
			resource.TopicSubscriptionOf("goodbye world"),
		}
		myInt := 1
		dispatch := uintptr(myInt)
		internalReceiver := &mockInternalReceiver{}
		internalReceiver.registerRxCallback = func(callback core.RxCallback) uintptr {
			return dispatch
		}
		subscribeCalled := false
		unsubscribeCalled := false
		subscribeCount := 0
		unsubscribeCount := 0
		internalReceiver.subscribe = func(topic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			if ptr != dispatch {
				t.Error("got unexpected pointer as dispatch reference")
			}
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
		internalReceiver.unsubscribe = func(topic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			if ptr != dispatch {
				t.Error("got unexpected pointer as dispatch reference")
			}
			if unsubscribeCount == len(subscriptions)-1 {
				unsubscribeCalled = true
			}
			if subscriptions[unsubscribeCount].GetName() != topic {
				t.Errorf("got unexpected subscription name: %s", topic)
			}
			unsubscribeCount++
			c := make(chan core.SubscriptionEvent, 1)
			c <- mockSubscriptionEvent{}
			return 0, c, nil
		}
		unregisterCalled := false
		internalReceiver.unregisterRxCallback = func(ptr uintptr) {
			if ptr != dispatch {
				t.Error("expected unregister rx callback to be called")
			}
			unregisterCalled = true
		}

		receiver := &directMessageReceiverImpl{}
		receiver.construct(&directMessageReceiverProps{
			internalReceiver:       internalReceiver,
			startupSubscriptions:   subscriptions,
			backpressureStrategy:   strategyDropLatest,
			backpressureBufferSize: 1,
		})

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
		if !unsubscribeCalled {
			t.Error("expected unsubscribe to be called on termination")
		}
		if !unregisterCalled {
			t.Error("expected dispatch function to be unregistered")
		}
	}
}

func TestBuilderWithInvalidSubscriptionType(t *testing.T) {
	builder := NewDirectMessageReceiverBuilderImpl(&mockInternalReceiver{})
	builder.WithSubscriptions(&notASubscription{})
	receiver, err := builder.Build()
	if _, ok := err.(*solace.IllegalArgumentError); !ok {
		t.Errorf("expected illegal argument error, got %s", err)
	}
	if receiver != nil {
		t.Error("expected receiver to equal nil, it was not")
	}
}

func TestDirectReceiverSubscribeWithInvalidSubscriptionType(t *testing.T) {
	receiver := directMessageReceiverImpl{
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

func TestDirectReceiverSubscribe(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := directMessageReceiverImpl{
		logger:               logging.Default,
		basicMessageReceiver: basicMessageReceiver{internalReceiver: internalReceiver, state: messageReceiverStateStarted},
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
		internalReceiver.subscribe = func(passedTopic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
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

func TestDirectReceiverSubscribeWithError(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := directMessageReceiverImpl{
		logger:               logging.Default,
		basicMessageReceiver: basicMessageReceiver{internalReceiver: internalReceiver, state: messageReceiverStateStarted},
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
		"This is a generated error info")
	for _, fn := range subscriptionFunctions {
		receiver.subscriptions = []string{}
		internalReceiverCalled := false
		internalReceiver.subscribe = func(passedTopic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
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

func TestUnsubscribeWithInvalidSubscriptionType(t *testing.T) {
	receiver := directMessageReceiverImpl{
		logger:               logging.Default,
		subscriptions:        []string{},
		basicMessageReceiver: basicMessageReceiver{internalReceiver: &mockInternalReceiver{}, state: messageReceiverStateStarted},
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

func TestDirectReceiverUnsubscribe(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := directMessageReceiverImpl{
		logger:               logging.Default,
		subscriptions:        []string{},
		basicMessageReceiver: basicMessageReceiver{internalReceiver: internalReceiver, state: messageReceiverStateStarted},
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
		internalReceiver.unsubscribe = func(passedTopic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
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

func TestDirectReceiverUnsubscribeWithError(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := directMessageReceiverImpl{
		logger:               logging.Default,
		subscriptions:        []string{},
		basicMessageReceiver: basicMessageReceiver{internalReceiver: internalReceiver, state: messageReceiverStateStarted},
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
		internalReceiver.unsubscribe = func(passedTopic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
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

func TestDirectReceiverSubscriptionsInBadStates(t *testing.T) {
	states := []messageReceiverState{messageReceiverStateNotStarted, messageReceiverStateStarting, messageReceiverStateTerminating, messageReceiverStateTerminated}
	for _, state := range states {
		receiver := &directMessageReceiverImpl{
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

func TestDirectReceiverAsyncFunctionsWithNilCallback(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &directMessageReceiverImpl{
		logger: logging.Default,
		basicMessageReceiver: basicMessageReceiver{
			internalReceiver: internalReceiver,
			state:            messageReceiverStateStarted,
		},
	}
	subscription := resource.TopicSubscriptionOf("hello world")
	calledChan := make(chan struct{})
	internalReceiver.subscribe = func(topic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
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
	internalReceiver.unsubscribe = func(topic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
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

func TestDirectReceiverSetCallbackInBadState(t *testing.T) {
	states := []messageReceiverState{messageReceiverStateTerminating, messageReceiverStateTerminated}
	for _, state := range states {
		receiver := &directMessageReceiverImpl{
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

func TestDirectReceiverBackpressureStrategyDropOldest(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &directMessageReceiverImpl{
		backpressureStrategy: strategyDropOldest,
		basicMessageReceiver: basicMessageReceiver{
			internalReceiver: internalReceiver,
			state:            messageReceiverStateStarted,
		},
	}
	metricsIncremented := false
	internalReceiver.incrementMetric = func(metric core.NextGenMetric, amount uint64) {
		if metric != core.MetricReceivedMessagesBackpressureDiscarded {
			t.Errorf("expected metric to be %d, got %d", core.MetricReceivedMessagesBackpressureDiscarded, metric)
		}
		if amount != 1 {
			t.Errorf("expected to see 1 messages dropped, got %d", amount)
		}
		metricsIncremented = true
	}
	bufferSize := 2
	messages := make([]ccsmp.SolClientMessagePt, bufferSize+1)
	for i := 0; i <= bufferSize; i++ {
		msgP, err := ccsmp.SolClientMessageAlloc()
		if err != nil {
			t.Error(err)
		}
		messages[i] = msgP
	}
	receiver.buffer = make(chan *directInboundMessage, bufferSize)
	for i := 0; i <= bufferSize; i++ {
		receiver.messageCallback(messages[i])
	}
	close(receiver.buffer)
	if len(receiver.buffer) != 2 {
		t.Error("messages never reached buffer")
	}
	index := 1
	for msg := range receiver.buffer {
		if msg.pointer != messages[index] {
			t.Errorf("expected message pointers to be equal: %v != %v", msg, messages[index])
		}
		index++
	}
	if !metricsIncremented {
		t.Error("metrics not incremented when message was dropped")
	}
}

func TestDirectReceiverBackpressureStrategyDropNewest(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &directMessageReceiverImpl{
		backpressureStrategy: strategyDropLatest,
		basicMessageReceiver: basicMessageReceiver{
			internalReceiver: internalReceiver,
			state:            messageReceiverStateStarted,
		},
	}
	metricsIncremented := false
	internalReceiver.incrementMetric = func(metric core.NextGenMetric, amount uint64) {
		if metric != core.MetricReceivedMessagesBackpressureDiscarded {
			t.Errorf("expected metric to be %d, got %d", core.MetricReceivedMessagesBackpressureDiscarded, metric)
		}
		if amount != 1 {
			t.Errorf("expected to see 1 messages dropped, got %d", amount)
		}
		metricsIncremented = true
	}
	bufferSize := 2
	messages := make([]ccsmp.SolClientMessagePt, bufferSize+1)
	for i := 0; i <= bufferSize; i++ {
		msgP, err := ccsmp.SolClientMessageAlloc()
		if err != nil {
			t.Error(err)
		}
		messages[i] = msgP
	}
	receiver.buffer = make(chan *directInboundMessage, bufferSize)
	for i := 0; i <= bufferSize; i++ {
		receiver.messageCallback(messages[i])
	}
	close(receiver.buffer)
	if len(receiver.buffer) != 2 {
		t.Error("messages never reached buffer")
	}
	index := 0
	for msg := range receiver.buffer {
		if msg.pointer != messages[index] {
			t.Errorf("expected message pointers to be equal: %v != %v", msg, messages[index])
		}
		index++
	}
	if !metricsIncremented {
		t.Error("metrics not incremented when message was dropped")
	}
}

func TestDirectReceiverRaceWithClosedBuffer(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &directMessageReceiverImpl{
		logger:               logging.Default,
		backpressureStrategy: strategyDropLatest,
		basicMessageReceiver: basicMessageReceiver{
			internalReceiver: internalReceiver,
			state:            messageReceiverStateStarted,
		},
	}
	msgP, err := ccsmp.SolClientMessageAlloc()
	if err != nil {
		t.Error(err)
	}
	receiver.buffer = make(chan *directInboundMessage, 1)
	close(receiver.buffer)
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("did not expect message callback to panic, but it did with panic: %s", r)
		}
	}()
	receiver.messageCallback(msgP)
}

func TestDirectReceiverTerminateWithSlowConsumer(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &directMessageReceiverImpl{}
	receiver.construct(&directMessageReceiverProps{
		internalReceiver:       internalReceiver,
		backpressureStrategy:   strategyDropLatest,
		backpressureBufferSize: 5,
	})
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

func TestDirectReceiverTerminateWithUndeliveredMessages(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &directMessageReceiverImpl{}
	receiver.construct(&directMessageReceiverProps{
		internalReceiver:       internalReceiver,
		backpressureStrategy:   strategyDropLatest,
		backpressureBufferSize: 5,
	})
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

func TestDirectReceiverTerminateUnsubscribe(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	expectedSubscriptions := []string{
		"hello world",
		"goodbye world",
	}
	receiver := &directMessageReceiverImpl{}
	receiver.construct(&directMessageReceiverProps{
		internalReceiver:       internalReceiver,
		backpressureStrategy:   strategyDropLatest,
		backpressureBufferSize: 1,
	})
	receiver.Start()
	receiver.subscriptions = expectedSubscriptions
	unsubscribeCalled := false
	unsubscribeIndex := 0
	internalReceiver.unsubscribe = func(topic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
		if topic != expectedSubscriptions[unsubscribeIndex] {
			t.Errorf("expected to get unsubscribe to topic %s, got %s", expectedSubscriptions[unsubscribeIndex], topic)
		}
		unsubscribeIndex++
		if unsubscribeIndex == len(expectedSubscriptions) {
			unsubscribeCalled = true
		}
		c := make(chan core.SubscriptionEvent, 1)
		c <- mockSubscriptionEvent{}
		return 0, c, nil
	}
	receiver.Terminate(10 * time.Millisecond)
	if !unsubscribeCalled {
		t.Error("topics were not unsubscribed from")
	}
}

func TestDirectReceiverStartIdempotence(t *testing.T) {
	startFunctions := []func(receiver solace.DirectMessageReceiver) <-chan error{
		func(receiver solace.DirectMessageReceiver) <-chan error {
			c := make(chan error, 1)
			go func() {
				c <- receiver.Start()
			}()
			return c
		},
		func(receiver solace.DirectMessageReceiver) <-chan error {
			return receiver.StartAsync()
		},
		func(receiver solace.DirectMessageReceiver) <-chan error {
			c := make(chan error, 1)
			receiver.StartAsyncCallback(func(dmr solace.DirectMessageReceiver, e error) {
				c <- e
			})
			return c
		},
	}
	for _, start := range startFunctions {
		internalReceiver := &mockInternalReceiver{}
		subscriptions := []resource.Subscription{
			resource.TopicSubscriptionOf("hello world"),
		}
		receiver := &directMessageReceiverImpl{}
		receiver.construct(&directMessageReceiverProps{
			internalReceiver:       internalReceiver,
			startupSubscriptions:   subscriptions,
			backpressureStrategy:   strategyDropLatest,
			backpressureBufferSize: 1,
		})

		callCount := 0
		subscribeBlock := make(chan struct{})
		internalReceiver.subscribe = func(topic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
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

func TestDirectMessageReceiverTerminateIdempotence(t *testing.T) {
	terminateFunctions := []func(receiver solace.DirectMessageReceiver) <-chan error{
		func(receiver solace.DirectMessageReceiver) <-chan error {
			c := make(chan error, 1)
			go func() {
				c <- receiver.Terminate(10 * time.Millisecond)
			}()
			return c
		},
		func(receiver solace.DirectMessageReceiver) <-chan error {
			return receiver.TerminateAsync(10 * time.Millisecond)
		},
		func(receiver solace.DirectMessageReceiver) <-chan error {
			c := make(chan error)
			receiver.TerminateAsyncCallback(10*time.Millisecond, func(e error) {
				c <- e
			})
			return c
		},
	}
	for _, terminate := range terminateFunctions {
		internalReceiver := &mockInternalReceiver{}
		subscriptions := []resource.Subscription{
			resource.TopicSubscriptionOf("hello world"),
		}
		receiver := &directMessageReceiverImpl{}
		receiver.construct(&directMessageReceiverProps{
			internalReceiver:       internalReceiver,
			startupSubscriptions:   subscriptions,
			backpressureStrategy:   strategyDropLatest,
			backpressureBufferSize: 5,
		})

		callCount := 0
		unsubscribeBlock := make(chan struct{})
		internalReceiver.unsubscribe = func(topic string, ptr uintptr) (core.SubscriptionCorrelationID, <-chan core.SubscriptionEvent, core.ErrorInfo) {
			<-unsubscribeBlock
			callCount++
			c := make(chan core.SubscriptionEvent, 1)
			c <- mockSubscriptionEvent{}
			return 0, c, nil
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

func TestDirectReceiverUnsolicitedTermination(t *testing.T) {
	internalReceiver := &mockInternalReceiver{}
	receiver := &directMessageReceiverImpl{}
	receiver.construct(&directMessageReceiverProps{
		internalReceiver:       internalReceiver,
		backpressureStrategy:   strategyDropLatest,
		backpressureBufferSize: 5,
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
