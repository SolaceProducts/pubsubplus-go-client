// pubsubplus-go-client
//
// Copyright 2024-2025 Solace Corporation. All rights reserved.
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
	"encoding/json"
	"fmt"
	"testing"
	"time"
	//    "unsafe"

	"solace.dev/go/messaging/internal/ccsmp"

	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/message"

	"solace.dev/go/messaging/internal/impl/constants"

	"solace.dev/go/messaging/internal/impl/executor"
	"solace.dev/go/messaging/internal/impl/publisher/buffer"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
)

func TestRequestReplyMessagePublisherBuilderWithValidBackpressure(t *testing.T) {
	backpressureConfigurations := []func(builder solace.RequestReplyMessagePublisherBuilder) (solace.RequestReplyMessagePublisherBuilder, backpressureConfiguration, int){
		func(builder solace.RequestReplyMessagePublisherBuilder) (solace.RequestReplyMessagePublisherBuilder, backpressureConfiguration, int) {
			return builder.OnBackPressureReject(0), backpressureConfigurationDirect, 0

		},
		func(builder solace.RequestReplyMessagePublisherBuilder) (solace.RequestReplyMessagePublisherBuilder, backpressureConfiguration, int) {
			bufferSize := 1
			return builder.OnBackPressureReject(uint(bufferSize)), backpressureConfigurationReject, bufferSize
		},
		func(builder solace.RequestReplyMessagePublisherBuilder) (solace.RequestReplyMessagePublisherBuilder, backpressureConfiguration, int) {
			bufferSize := 1
			return builder.OnBackPressureWait(uint(bufferSize)), backpressureConfigurationWait, bufferSize
		},
	}
	shared := &mockInternalPublisher{}
	for _, config := range backpressureConfigurations {
		builder, backpressureConfig, capacity := config(NewRequestReplyMessagePublisherBuilderImpl(shared))
		publisher, err := builder.Build()
		if err != nil {
			t.Error(err)
		}
		if publisher == nil {
			t.Error("expected publisher to not be nil")
		}
		publisherImpl := publisher.(*requestReplyMessagePublisherImpl)
		if publisherImpl.backpressureConfiguration != backpressureConfig {
			t.Errorf("expected backpressure config to equal %d, was %d", backpressureConfig, publisherImpl.backpressureConfiguration)
		}
		if cap(publisherImpl.buffer) != capacity {
			t.Errorf("expected backpressure capacity to equal %d, was %d", capacity, cap(publisherImpl.buffer))
		}
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidBackpressureWait(t *testing.T) {
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).OnBackPressureWait(0).Build()
	// we should get an error saying that buffer must be > 0 for wait
	if err == nil {
		t.Error("expected error to not be nil")
	}
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
}

func TestRequestReplyMessagePublisherBuilderWithCustomPropertiesStructFromJSON(t *testing.T) {
	jsonData := `{"solace":{"messaging":{"publisher":{"back-pressure":{"strategy":"BUFFER_WAIT_WHEN_FULL","buffer-capacity": 100,"buffer-wait-timeout": 1000}}}}}`
	baselineProperties := make(config.PublisherPropertyMap)
	err := json.Unmarshal([]byte(jsonData), &baselineProperties)
	if err != nil {
		t.Error(err)
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher == nil {
		t.Error("expected publisher to not be nil")
	}
	if err != nil {
		t.Error(err)
	}
	publisherImpl := publisher.(*requestReplyMessagePublisherImpl)
	if publisherImpl.backpressureConfiguration != backpressureConfigurationWait {
		t.Errorf("expected backpressure config to equal %d, was %d", backpressureConfigurationWait, publisherImpl.backpressureConfiguration)
	}
	if cap(publisherImpl.buffer) != 100 {
		t.Errorf("expected backpressure capacity to equal %d, was %d", 100, cap(publisherImpl.buffer))
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidCustomPropertiesMapNegativeBufferCapacity(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: -1,
		config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure capacity is negative")
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongTypeBufferCapacity(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: "hello",
		config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferWaitWhenFull,
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure capacity is a string")
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongTypeStrategy(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: 1,
		config.PublisherPropertyBackPressureStrategy:       23,
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure strategy is an integer")
	}
}

func TestRequestReplyMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongStrategy(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: 1,
		config.PublisherPropertyBackPressureStrategy:       "hello world",
	}
	publisher, err := NewRequestReplyMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure strategy is an integer")
	}
}

func TestRequestReplyMessagePublisherImplLifecycle(t *testing.T) {
	gracePeriod := 10 * time.Second

	// parameterize this test with the various start and terminate functions (sync/async)
	startAndTerminatFunctions := []struct {
		start     func(publisher *requestReplyMessagePublisherImpl)
		terminate func(publisher *requestReplyMessagePublisherImpl)
	}{
		{
			start: func(publisher *requestReplyMessagePublisherImpl) {
				err := publisher.Start()
				if err != nil {
					t.Error("expected error to be nil, got " + err.Error())
				}
			},
			terminate: func(publisher *requestReplyMessagePublisherImpl) {
				err := publisher.Terminate(gracePeriod)
				if err != nil {
					t.Error("expected error to be nil, got " + err.Error())
				}
			},
		},
		{
			start: func(publisher *requestReplyMessagePublisherImpl) {
				select {
				case err := <-publisher.StartAsync():
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
				case <-time.After(100 * time.Millisecond):
					t.Error("timed out waiting for publisher to start")
				}
			},
			terminate: func(publisher *requestReplyMessagePublisherImpl) {
				select {
				case err := <-publisher.TerminateAsync(gracePeriod):
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
				case <-time.After(gracePeriod + 5*time.Second):
					t.Error("timed out waiting for publisher to terminate")
				}
			},
		},
		{
			start: func(publisher *requestReplyMessagePublisherImpl) {
				done := make(chan struct{})
				publisher.StartAsyncCallback(func(retPub solace.RequestReplyMessagePublisher, err error) {
					if publisher != retPub {
						t.Error("got a different publisher returned to the start callback")
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
					t.Error("timed out waiting for request reply publisher to start")
				}
			},
			terminate: func(publisher *requestReplyMessagePublisherImpl) {
				done := make(chan struct{})
				publisher.TerminateAsyncCallback(gracePeriod, func(err error) {
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
					close(done)
				})
				select {
				case <-done:
					// success
				case <-time.After(100 * time.Millisecond):
					t.Error("timed out waiting for request reply publisher to start")
				}
			},
		},
	}
	for _, fns := range startAndTerminatFunctions {
		publisher := &requestReplyMessagePublisherImpl{}
		publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
		eventExecutor := &mockEventExecutor{}
		taskBuffer := &mockTaskBuffer{}
		publisher.eventExecutor = eventExecutor
		publisher.taskBuffer = taskBuffer
		// pre start
		if publisher.IsReady() {
			t.Error("expected publisher to not be ready")
		}
		if publisher.IsRunning() {
			t.Error("expected publisher to not be running")
		}
		if publisher.IsTerminating() {
			t.Error("expected terminating to be false, was true")
		}
		if publisher.IsTerminated() {
			t.Error("expected publisher to not yet be terminated")
		}

		// start
		eventExecutorStarted := make(chan struct{})
		eventExecutor.run = func() {
			close(eventExecutorStarted)
		}
		taskBufferStarted := make(chan struct{})
		taskBuffer.run = func() {
			close(taskBufferStarted)
		}
		fns.start(publisher)
		// check started
		select {
		case <-eventExecutorStarted:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for event executor to start")
		}
		select {
		case <-taskBufferStarted:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for event executor to start")
		}
		// check started states
		if !publisher.IsReady() {
			t.Error("expected publisher to be ready, it was not")
		}
		if !publisher.IsRunning() {
			t.Error("expected publisher to be running, it was not")
		}
		if publisher.IsTerminating() {
			t.Error("expected terminating to be false, was true")
		}
		if publisher.IsTerminated() {
			t.Error("expected publisher to not yet be terminated")
		}

		// terminate
		eventExecutorTerminated := make(chan struct{})
		eventExecutor.awaitTermination = func() {
			close(eventExecutorTerminated)
		}
		taskBufferTerminated := make(chan struct{})
		taskBuffer.terminate = func(timer *time.Timer) bool {
			// this should be shutdown first
			select {
			case <-eventExecutorTerminated:
				t.Error("expected task buffer to be shutdown first")
			default:
				// success
			}
			// check terminating state
			if publisher.IsReady() {
				t.Error("expected publisher to not be ready")
			}
			if publisher.IsRunning() {
				t.Error("expected publisher to not be running")
			}
			if publisher.IsTerminated() {
				t.Error("expected publisher to not yet be terminated")
			}
			if !publisher.IsTerminating() {
				t.Error("expected publisher to be terminating")
			}
			close(taskBufferTerminated)
			return true
		}
		fns.terminate(publisher)
		// check terminated
		select {
		case <-eventExecutorTerminated:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for event executor to terminate")
		}
		select {
		case <-taskBufferTerminated:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for task buffer to terminate")
		}
		select {
		case <-publisher.requestCorrelateComplete:
			//success
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for correlation request to terminate")
		}
		select {
		case <-publisher.correlationComplete:
			//success
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for correlation table empty channel to terminate")
		}
		// check terminated states
		if publisher.IsReady() {
			t.Error("expected publisher to not be ready")
		}
		if publisher.IsRunning() {
			t.Error("expected publisher to not be running")
		}
		if publisher.IsTerminating() {
			t.Error("expected publisher to not be terminating")
		}
		if !publisher.IsTerminated() {
			t.Error("expected publisher to be terminated")
		}
	}
}

func TestRequestReplyMessagePublisherImplLifecycleNoBuffer(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationDirect, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	// start
	eventExecutorStarted := make(chan struct{})
	eventExecutor.run = func() {
		close(eventExecutorStarted)
	}
	taskBufferStarted := make(chan struct{})
	taskBuffer.run = func() {
		close(taskBufferStarted)
	}
	err := publisher.Start()
	if err != nil {
		t.Error("expected error to be nil, got " + err.Error())
	}
	// check started
	select {
	case <-eventExecutorStarted:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for event executor to start")
	}
	select {
	case <-taskBufferStarted:
		t.Error("did not expect task buffer to start")
	case <-time.After(100 * time.Millisecond):
		// success
	}

	// terminate
	gracePeriod := 10 * time.Second
	eventExecutorTerminated := make(chan struct{})
	eventExecutor.awaitTermination = func() {
		close(eventExecutorTerminated)
	}
	taskBufferTerminated := make(chan struct{})
	taskBuffer.terminate = func(timer *time.Timer) bool {
		// this should be shutdown first
		select {
		case <-eventExecutorTerminated:
			t.Error("expected task buffer to be shutdown first")
		default:
			// success
		}
		close(taskBufferTerminated)
		return true
	}
	err = publisher.Terminate(gracePeriod)
	// check terminated
	select {
	case <-eventExecutorTerminated:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for event executor to terminate")
	}
	select {
	case <-taskBufferTerminated:
		t.Error("did not expect task buffer to be terminated as it was never started")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	select {
	case <-publisher.requestCorrelateComplete:
		//success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for correlation request to terminate")
	}
	select {
	case <-publisher.correlationComplete:
		//success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for correlation table empty channel to terminate")
	}
	// check terminated states
	if err != nil {
		t.Error("expected error to be nil, got " + err.Error())
	}
}

func TestRequestReplyMessagePublisherLifecycleIdempotence(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	// expect channels to be closed on second call only
	eventExecutorStarted := make(chan interface{}, 2)
	eventExecutor.run = func() {
		eventExecutorStarted <- nil
	}
	taskBufferStarted := make(chan interface{}, 2)
	taskBuffer.run = func() {
		taskBufferStarted <- nil
	}

	// start
	err := publisher.Start()
	if err != nil {
		t.Error("expected error to be nil, got " + err.Error())
	}
	// check started
	select {
	case <-eventExecutorStarted:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for event executor to start")
	}
	select {
	case <-taskBufferStarted:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for event executor to start")
	}

	// start again
	err = publisher.Start()
	if err != nil {
		t.Error("did not expect an error on subsequent start, got " + err.Error())
	}
	select {
	case <-eventExecutorStarted:
		t.Error("did not expect event executor to be run on subsequent starts")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	select {
	case <-taskBufferStarted:
		t.Error("did not expect task buffer to be run on subsequent starts")
	case <-time.After(100 * time.Millisecond):
		// success
	}

	// check started states
	if !publisher.IsReady() {
		t.Error("expected publisher to be ready, it was not")
	}
	if !publisher.IsRunning() {
		t.Error("expected publisher to be running, it was not")
	}
	if publisher.IsTerminating() {
		t.Error("expected terminating to be false, was true")
	}
	if publisher.IsTerminated() {
		t.Error("expected publisher to not yet be terminated")
	}

	// terminate functions
	eventExecutorTerminated := make(chan interface{}, 2)
	eventExecutor.awaitTermination = func() {
		eventExecutorTerminated <- nil
	}
	taskBufferTerminated := make(chan interface{}, 2)
	taskBuffer.terminate = func(timer *time.Timer) bool {
		taskBufferTerminated <- nil
		return true
	}

	gracePeriod := 10 * time.Second
	// terminate
	err = publisher.Terminate(gracePeriod)
	if err != nil {
		t.Error("expected error to be nil, got " + err.Error())
	}
	// make sure termiante was called
	select {
	case <-eventExecutorTerminated:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for event executor to termiante")
	}
	select {
	case <-taskBufferTerminated:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for event executor to terminate")
	}
	select {
	case <-publisher.requestCorrelateComplete:
		//success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for correlation request to terminate")
	}
	select {
	case <-publisher.correlationComplete:
		//success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for correlation table empty channel to terminate")
	}

	err = publisher.Terminate(gracePeriod)
	// check terminated states
	if err != nil {
		t.Error("expected error to be nil, got " + err.Error())
	}
	// check terminated
	select {
	case <-eventExecutorTerminated:
		t.Error("did not expect event executor to be terminated again")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	select {
	case <-taskBufferTerminated:
		t.Error("did not expect task buffer to be terminated again")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	select {
	case <-publisher.requestCorrelateComplete:
		//success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for correlation request to terminate")
	}
	select {
	case <-publisher.correlationComplete:
		//success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for correlation table empty channel to terminate")
	}
}

func TestRequestReplyMessagePublisherTerminateWithUnpublishedMessages(t *testing.T) {
	internalPublisher := &mockInternalPublisher{}
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(internalPublisher, backpressureConfigurationWait, 10)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	unpublishedCount := 2
	metricsIncremented := false
	internalPublisher.incrementMetric = func(metric core.NextGenMetric, amount uint64) {
		if metric != core.MetricPublishMessagesTerminationDiscarded {
			t.Errorf("expected metric %d to be incremented, got %d", core.MetricPublishMessagesTerminationDiscarded, metric)
		}
		if amount != uint64(unpublishedCount) {
			t.Errorf("expected %d unpublished messages, got %d", unpublishedCount, amount)
		}
		metricsIncremented = true
	}

	publisher.Start()
	for i := 0; i < unpublishedCount; i++ {
		unpublished := &publishable{}
		unpublished.message = &message.OutboundMessageImpl{}
		publisher.buffer <- unpublished
	}
	err := publisher.Terminate(10 * time.Second)
	expected := fmt.Sprintf(constants.IncompleteMessageDeliveryMessage, unpublishedCount)
	if err == nil || err.Error() != expected {
		t.Errorf("did not get expected error. Expected '%s', got '%s'", expected, err)
	}
	if !metricsIncremented {
		t.Error("IncrementMetric not called")
	}
}

func TestRequestReplyMessagePublisherUnsolicitedTerminationWithUnpublishedMessages(t *testing.T) {
	internalPublisher := &mockInternalPublisher{}
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(internalPublisher, backpressureConfigurationWait, 10)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	unpublishedCount := 2
	metricsIncremented := false
	internalPublisher.incrementMetric = func(metric core.NextGenMetric, amount uint64) {
		if metric != core.MetricPublishMessagesTerminationDiscarded {
			t.Errorf("expected metric %d to be incremented, got %d", core.MetricPublishMessagesTerminationDiscarded, metric)
		}
		if amount != uint64(unpublishedCount) {
			t.Errorf("expected %d unpublished messages, got %d", unpublishedCount, amount)
		}
		metricsIncremented = true
	}

	terminationListenerCalled := make(chan error)
	publisher.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
		delta := time.Since(te.GetTimestamp())
		if delta < 0 || delta > 100*time.Millisecond {
			t.Errorf("Timestamp delta too large! Timestamp: %s, now: %s", te.GetTimestamp(), time.Now())
		}
		if !publisher.IsTerminated() {
			t.Error("Expected publisher to be terminated when notification listener is called")
		}
		if te.GetMessage() == "" {
			t.Error("Expected message in termination event")
		}
		terminationListenerCalled <- te.GetCause()
	})

	publisher.Start()

	eventExecutorTerminated := make(chan interface{})
	eventExecutor.terminate = func() {
		close(eventExecutorTerminated)
	}
	taskBufferTerminated := make(chan interface{})
	taskBuffer.terminateNow = func() {
		close(taskBufferTerminated)
	}

	for i := 0; i < unpublishedCount; i++ {
		unpublished := &publishable{}
		unpublished.message = &message.OutboundMessageImpl{}
		publisher.buffer <- unpublished
	}
	errForEvent := fmt.Errorf("some error")
	publisher.onDownEvent(&mockEvent{err: errForEvent})

	select {
	case <-eventExecutorTerminated:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for event executor to terminate")
	}
	select {
	case <-taskBufferTerminated:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for event executor to terminate")
	}
	select {
	case err := <-terminationListenerCalled:
		if err != errForEvent {
			t.Errorf("expected %s, got %s", errForEvent, err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for termination listener to be called")
	}
	err := publisher.Terminate(100 * time.Millisecond)
	if _, ok := err.(*solace.IncompleteMessageDeliveryError); !ok {
		t.Error("expected to get incomplete message delivery error from subsequent calls to terminate")
	}
	if !metricsIncremented {
		t.Error("IncrementMetrics not called")
	}
}

func TestRequestReplyCallPublishWhenNotStarted(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	replyHandler := func(replyMsg apimessage.InboundMessage, context interface{}, err error) {}
	err := publisher.Publish(testMessage, replyHandler, testTopic, 100*time.Millisecond, nil /* usercontext */, nil /* property provider */)
	if err == nil {
		t.Error("expected publish to fail when publisher not started")
	}
}

func TestRequestReplyCallPublishWhenAlreadyTerminated(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()
	publisher.Terminate(1 * time.Second)

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	replyHandler := func(replyMsg apimessage.InboundMessage, context interface{}, err error) {}
	err := publisher.Publish(testMessage, replyHandler, testTopic, 100*time.Millisecond, nil /* usercontext */, nil /* property provider */)
	if err == nil {
		t.Error("expected publish to fail when publisher already terminated")
	}
}

func TestRequestReplyStartFailedToGetReplyTo(t *testing.T) {
	subCode := 58 // MissingReplyTo, note this subcode does not matter and does not represent a real scenario

	publisher := &requestReplyMessagePublisherImpl{}

	internalPublisher := &mockInternalPublisher{}
	internalPublisher.requestor = func() core.Requestor {
		mock := &mockRequestor{}
		mock.addRequestorReplyHandler = func(handler core.RequestorReplyHandler) (string, func() (messageID uint64, correlationID string), core.ErrorInfo) {
			return "", nil, ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCodeFail,
				ccsmp.SolClientSubCode(subCode),
				ccsmp.SolClientResponseCode(0),
				"This is a generated error info")
		}
		return mock
	}

	publisher.construct(internalPublisher, backpressureConfigurationWait, 1)
	//eventExecutor := &mockEventExecutor{}
	//taskBuffer := &mockTaskBuffer{}
	//publisher.eventExecutor = eventExecutor
	//publisher.taskBuffer = taskBuffer

	err := publisher.Start()
	if err == nil {
		t.Error("Failed to get error on start when replyto topic was not acquired by publisher")
	}
	publisher.Terminate(1 * time.Second)
}

func TestRequestReplyCallPublishWithBadPayload(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	testTopic := resource.TopicOf("hello/world")
	replyHandler := func(replyMsg apimessage.InboundMessage, context interface{}, err error) {}
	err := publisher.Publish(nil, replyHandler, testTopic, 100*time.Millisecond, nil /* usercontext */, nil /* property provider */)
	if err == nil {
		t.Error("expected publish to fail when message parameter is nil")
	}
	_, err = publisher.PublishAwaitResponse(nil, testTopic, 100*time.Millisecond, nil /* property provider */)
	if err == nil {
		t.Error("expected publish await response to fail when message parameter is nil")
	}
}

func TestRequestReplyCallPublishWithBadReplyHandler(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	testTopic := resource.TopicOf("hello/world")
	timeoutDuration := 100 * time.Millisecond
	pubFuncs := []func(pub solace.RequestReplyMessagePublisher) (string, error){
		func(pub solace.RequestReplyMessagePublisher) (string, error) {
			// pub message
			testMessage, _ := message.NewOutboundMessage()
			return "Message", pub.Publish(testMessage, nil /* replyHandler */, testTopic, timeoutDuration, nil /* usercontext */, nil /* property provider */)
		},
		func(pub solace.RequestReplyMessagePublisher) (string, error) {
			// pub string
			testString := "RequestReply"
			return "String", pub.PublishString(testString, nil /* replyHandler */, testTopic, timeoutDuration, nil /* usercontext */)
		},
		func(pub solace.RequestReplyMessagePublisher) (string, error) {
			// pub bytes
			testBytes := []byte{0x01, 0x02, 0x03}
			return "Bytes", pub.PublishBytes(testBytes, nil /* replyHandler */, testTopic, timeoutDuration, nil /* usercontext */)
		},
	}

	for _, pubFunc := range pubFuncs {
		description, err := pubFunc(publisher)
		if err == nil {
			t.Errorf("expected publish %s to fail when replyHandler parameter is nil", description)
		}
	}
}

func TestRequestReplyCallPublishWithNegativeDuration(t *testing.T) {
	// test wait beahviour for negative timeouts
	// expects no timeout error for any response with no replier
	publisherReplyToTopic := "testReplyTopic"
	testTopic := resource.TopicOf("hello/world")
	var coreReplyHandler core.RequestorReplyHandler = nil
	messagePublishedChan := make(chan bool, 1)
	corePublisher := &mockInternalPublisher{}
	corePublisher.requestor = func() core.Requestor {
		mock := &mockRequestor{}
		mock.addRequestorReplyHandler = func(handler core.RequestorReplyHandler) (string, func() (messageID uint64, correlationID string), core.ErrorInfo) {
			count := uint64(0)
			coreReplyHandler = handler
			return publisherReplyToTopic, func() (uint64, string) {
				count += 1
				return count, fmt.Sprintf("TEST%d", count)
			}, nil
		}
		return mock
	}
	corePublisher.publish = func(message core.Publishable) core.ErrorInfo {
		messagePublishedChan <- true
		return nil
	}
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(corePublisher, backpressureConfigurationDirect, 0)

	publisher.Start()

	if coreReplyHandler == nil {
		t.Error("coreReplyHandler was not set from call to start")
	}

	if publisher.replyToTopic != publisherReplyToTopic {
		t.Error("replyToTopic was not set from start")
	}

	replyMessageResponseChan := make(chan error, 1)

	replyHandler := func(replyMsg apimessage.InboundMessage, context interface{}, err error) {
		replyMessageResponseChan <- err
	}

	err := publisher.PublishString("testPayload", replyHandler, testTopic, time.Duration(-1), nil /* usercontext */)
	if err != nil {
		t.Errorf("Error publishing request message with negative timeout, error: %s", err)
	}

	select {
	case sent := <-messagePublishedChan:
		if !sent {
			t.Error("Message was not sent")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for request message to publish")
	}

	terminateChan := publisher.TerminateAsync(1 * time.Second)

	select {
	case replyError := <-replyMessageResponseChan:
		if replyError == nil {
			t.Error("Publisher did not receive reply error response")
		} else if _, ok := replyError.(*solace.TimeoutError); ok {
			t.Error("Publisher received timeout error response")
		}
	case <-time.After(2 * time.Second):
		t.Error("did not receive response after terminate completes")
	}

	termError := <-terminateChan
	// terminate compelete
	if termError != nil {
		t.Errorf("Got unexpected termination error %s", termError)
	}

}

func TestRequestReplyMessagePublisherPublishFailureFromTimeout(t *testing.T) {
	// test wait beahviour for negative timeouts
	// expects no timeout error for any response with no replier
	publisherReplyToTopic := "testReplyTopic"
	testTopic := resource.TopicOf("hello/world")
	var coreReplyHandler core.RequestorReplyHandler = nil
	messagePublishedChan := make(chan bool, 1)
	corePublisher := &mockInternalPublisher{}
	corePublisher.requestor = func() core.Requestor {
		mock := &mockRequestor{}
		mock.addRequestorReplyHandler = func(handler core.RequestorReplyHandler) (string, func() (messageID uint64, correlationID string), core.ErrorInfo) {
			count := uint64(0)
			coreReplyHandler = handler
			return publisherReplyToTopic, func() (uint64, string) {
				count += 1
				return count, fmt.Sprintf("TEST%d", count)
			}, nil
		}
		return mock
	}
	corePublisher.publish = func(message core.Publishable) core.ErrorInfo {
		messagePublishedChan <- true
		return nil
	}
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(corePublisher, backpressureConfigurationDirect, 0)

	publisher.Start()

	if coreReplyHandler == nil {
		t.Error("coreReplyHandler was not set from call to start")
	}

	if publisher.replyToTopic != publisherReplyToTopic {
		t.Error("replyToTopic was not set from start")
	}

	timeout := 1 * time.Millisecond // set to timeout imemdiately

	// test timeout from reply handler
	replyMessageResponseChan := make(chan error, 1)

	replyHandler := func(replyMsg apimessage.InboundMessage, context interface{}, err error) {
		replyMessageResponseChan <- err
	}

	err := publisher.PublishString("testPayload", replyHandler, testTopic, timeout, nil /* usercontext */)
	if err != nil {
		t.Errorf("Error publishing request message with negative timeout, error: %s", err)
	}

	select {
	case sent := <-messagePublishedChan:
		if !sent {
			t.Error("Message was not sent")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for request message to publish")
	}

	select {
	case replyError := <-replyMessageResponseChan:
		if replyError == nil {
			t.Error("Publisher did not receive reply error response")
		} else if _, ok := replyError.(*solace.TimeoutError); !ok {
			t.Errorf("Publisher received error response that was not timeout. Error %s", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("did not receive response after expected timeout")
	}

	// test timeout for publishAwaitResponse
	// make message
	builder := message.NewOutboundMessageBuilder()
	outMsg, err := builder.BuildWithStringPayload("testpayload", nil)
	if err != nil {
		t.Error("Failed to build message to publish with PublishAwaitResponse")
	}

	go func() {
		_, pubErr := publisher.PublishAwaitResponse(outMsg, testTopic, timeout, nil /* properties */)
		replyMessageResponseChan <- pubErr
	}()

	select {
	case sent := <-messagePublishedChan:
		if !sent {
			t.Error("Message was not sent")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for request message to publish")
	}

	select {
	case replyError := <-replyMessageResponseChan:
		if replyError == nil {
			t.Error("Publisher did not receive reply error response")
		} else if _, ok := replyError.(*solace.TimeoutError); !ok {
			t.Errorf("Publisher received error response that was not timeout. Error %s", replyError)
		}
	case <-time.After(2 * time.Second):
		t.Error("did not receive response after expected timout")
	}

	// terminate the publisher
	terminateChan := publisher.TerminateAsync(1 * time.Second)

	termError := <-terminateChan
	// terminate compelete
	if termError != nil {
		t.Errorf("Got unexpected termination error %s", termError)
	}
}

func TestRequestReplyMessagePublisherPublishReplyByCorrelation(t *testing.T) {
	// publish request message
	// then receive a reply message without matching correlation id, expect no reply in handler
	// then receive a reply message with matching correlation id, expect reply message in handler
	// then receive a reply message the matching correlation id, expect no reply in handler
	// this test can mock a reply message by calling publisher the reply handler from addRequestHandler in core requestor.
	// the mock reply messages can be constructed by setting the following fields:
	//  - correlation id
	// in real scenario more fields are required:
	//  - smf header bit
	//  - destination set as the reply to destination for the publisher

	publisherReplyToTopic := "testReplyTopic"
	testTopic := resource.TopicOf("hello/world")
	var coreReplyHandler core.RequestorReplyHandler = nil
	messagePublishedChan := make(chan string, 1)
	corePublisher := &mockInternalPublisher{}
	corePublisher.requestor = func() core.Requestor {
		mock := &mockRequestor{}
		mock.addRequestorReplyHandler = func(handler core.RequestorReplyHandler) (string, func() (messageID uint64, correlationID string), core.ErrorInfo) {
			count := uint64(0)
			coreReplyHandler = handler
			return publisherReplyToTopic, func() (uint64, string) {
				count += 1
				return count, fmt.Sprintf("TEST%d", count)
			}, nil
		}
		return mock
	}
	corePublisher.publish = func(message core.Publishable) core.ErrorInfo {
		id, errinfo := ccsmp.SolClientMessageGetCorrelationID(message)
		if errinfo == nil {
			messagePublishedChan <- id
		}
		return nil
	}

	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(corePublisher, backpressureConfigurationDirect, 0)

	createRepliable := func(correlationID string, destination string, setReplyHeaderFlag bool) core.Repliable {
		// construct inbound message with both fields
		// use outbound message to construct the message
		builder := message.NewOutboundMessageBuilder()
		// set correlationID
		outMsg, err := builder.WithCorrelationID(correlationID).BuildWithStringPayload("testpayload", nil)
		if err != nil {
			t.Error("Failed to build message with correlation id")
		}
		outMsgImpl, ok := outMsg.(*message.OutboundMessageImpl)
		if !ok {
			t.Error("Failed to access internal outbound message impl struct")
		}
		// set reply to destination
		err = message.SetDestination(outMsgImpl, destination)
		if err != nil {
			t.Error("Failed to set the replyTo destination")
		}
		msgP, errInfo := ccsmp.SolClientMessageDup(message.GetOutboundMessagePointer(outMsgImpl))
		if errInfo != nil {
			t.Error("Failed to extract duplicate core message from constructed message")
		}
		return msgP
	}

	publisher.Start()

	if coreReplyHandler == nil {
		t.Error("coreReplyHandler was not set from call to start")
	}

	if publisher.replyToTopic != publisherReplyToTopic {
		t.Error("replyToTopic was not set from start")
	}

	replyMessageResponseChan := make(chan apimessage.InboundMessage, 1)
	replyMessageErrorResponseChan := make(chan error, 1)

	replyHandler := func(replyMsg apimessage.InboundMessage, context interface{}, err error) {
		if err != nil {
			replyMessageErrorResponseChan <- err
		}
		if replyMsg != nil {
			replyMessageResponseChan <- replyMsg
		}
	}

	err := publisher.PublishString("testPayload", replyHandler, testTopic, time.Duration(-1), nil /* usercontext */)
	if err != nil {
		t.Errorf("Error publishing request message with negative timeout, error: %s", err)
	}

	coreReplyHandlerChannel := make(chan string, 1)

	// mock ccsmp context thread pushing replies
	go func() {
		for {
			replyCorrelationID, ok := <-coreReplyHandlerChannel
			if !ok {
				return
			}
			repliable := createRepliable(replyCorrelationID, publisherReplyToTopic, true)
			takeMsg := coreReplyHandler(repliable, replyCorrelationID)
			if !takeMsg {
				// cleanup repliable like ccsmp context thread would
				ccsmp.SolClientMessageFree(&repliable)
			}
		}
	}()

	var publishedID string
	select {
	case publishedID = <-messagePublishedChan:
		if len(publishedID) < 1 {
			t.Error("Message was not sent and could not get correlation ID")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for request message to publish")
	}

	// push reply with mismatch correlation id
	coreReplyHandlerChannel <- "NotAPublisherCorrelationID"

	select {
	case replyError := <-replyMessageErrorResponseChan:
		if replyError != nil {
			t.Errorf("Publisher did receive reply error response. Error %s", replyError)
		}
	case replyMessage := <-replyMessageResponseChan:
		if cID, present := replyMessage.GetCorrelationID(); present {
			t.Errorf("Received replyMessage for published correlation ID[%s] for mismatch reply correlation ID[%s]", publishedID, cID)
		} else {
			t.Errorf("Got replyMessage for correlationID[%s] without a correlation id", publishedID)
		}
	case <-time.After(100 * time.Millisecond):
		// success did not receive any reply message
	}

	// push reply with mathcing correlation id
	coreReplyHandlerChannel <- publishedID
	select {
	case replyError := <-replyMessageErrorResponseChan:
		if replyError != nil {
			t.Errorf("Publisher did receive reply error response. Error %s", replyError)
		}
	case replyMessage := <-replyMessageResponseChan:
		if cID, present := replyMessage.GetCorrelationID(); present {
			if cID != publishedID {
				t.Errorf("Received replyMessage for published correlation ID[%s] for matching reply correlation ID[%s] did not match", publishedID, cID)
			}
		} else {
			t.Errorf("Got replyMessage for correlationID[%s] without a correlation id", publishedID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Failed to get reply outcome in time")
	}

	// push reply with matching correlation id again should get no reply
	coreReplyHandlerChannel <- publishedID
	select {
	case replyError := <-replyMessageErrorResponseChan:
		if replyError != nil {
			t.Errorf("Publisher did receive unexpected reply error response. Error %s", replyError)
		}
	case replyMessage := <-replyMessageResponseChan:
		if cID, present := replyMessage.GetCorrelationID(); present {
			t.Errorf("Received second replyMessage for published correlation ID[%s] for matching reply correlation ID[%s]", publishedID, cID)
		} else {
			t.Errorf("Got second replyMessage for correlationID[%s] without a correlation id", publishedID)
		}
	case <-time.After(100 * time.Millisecond):
		// success did not receive any reply message
	}

	close(coreReplyHandlerChannel)
	publisher.Terminate(1)
}

func TestRequestReplyMessagePublisherPublishFunctionalityBufferedWait(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	taskBufferSubmitCalled := make(chan interface{}, 10)
	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		taskBufferSubmitCalled <- nil
		return true
	}

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	testReplyHandler := func(msg apimessage.InboundMessage, usercontext interface{}, err error) {}
	testTimeout := time.Duration(-1) // forever
	err := publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err != nil {
		t.Error(err)
	}

	select {
	case <-taskBufferSubmitCalled:
		// success
	default:
		t.Error("Expect task buffer submit to be called")
	}

	publishComplete := make(chan struct{})
	go func() {
		// this should block
		err := publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
		if err != nil {
			t.Error(err)
		}
		close(publishComplete)
	}()

	select {
	case <-publishComplete:
		t.Error("expected publish to block a while")
	case <-time.After(50 * time.Millisecond):
		// success
	}
	select {
	case <-publisher.buffer:
	default:
		t.Error("expected message to be present in publisher buffer")
	}

	select {
	case <-publishComplete:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("timed out waiting for publish to complete")
	}

	publishFailed := make(chan error)
	go func() {
		publishFailed <- publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	}()
	select {
	case <-publishFailed:
		t.Error("expected to block indefinitely, returned")
	case <-time.After(1 * time.Second):
		// success
	}

	select {
	case <-publisher.buffer:
	default:
		t.Error("expected message to be present in publisher buffer")
	}
	select {
	case <-publishFailed:
		// success
	case <-time.After(1 * time.Second):
		t.Error("expected long running block to successfully push message")
	}
	select {
	case <-publisher.buffer:
	default:
		t.Error("expected message to be present in publisher buffer")
	}
	err = publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err != nil {
		t.Error(err)
	}
}

func TestRequestReplyMessagePublisherPublishFunctionalityBufferedReject(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationReject, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	taskBufferSubmitCalled := make(chan interface{}, 10)
	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		taskBufferSubmitCalled <- nil
		return true
	}

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	testReplyHandler := func(msg apimessage.InboundMessage, usercontext interface{}, err error) {}
	testTimeout := time.Duration(-1) // forever
	err := publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err != nil {
		t.Error(err)
	}

	select {
	case <-taskBufferSubmitCalled:
		// success
	default:
		t.Error("Expect task buffer submit to be called")
	}

	err = publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if _, ok := err.(*solace.PublisherOverflowError); !ok {
		t.Errorf("expected would block error, got %s", err)
	}

	select {
	case <-publisher.buffer:
	default:
		t.Error("expected message to be present in publisher buffer")
	}

	err = publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err != nil {
		t.Error(err)
	}
}

func TestRequestReplyMessagePublisherPublishFunctionalityDirect(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	internalPublisher := &mockInternalPublisher{}
	publisher.construct(internalPublisher, backpressureConfigurationDirect, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	taskBufferSubmitCalled := make(chan struct{})
	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		close(taskBufferSubmitCalled)
		return true
	}

	publishCalled := false
	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		publishCalled = true
		return nil
	}

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	testReplyHandler := func(msg apimessage.InboundMessage, usercontext interface{}, err error) {}
	testTimeout := time.Duration(-1) // forever
	err := publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err != nil {
		t.Error(err)
	}
	if !publishCalled {
		t.Error("expected internal publisher's publish function to be called directly")
	}

	select {
	case <-taskBufferSubmitCalled:
		t.Error("Expect task buffer submit to not be called")
	default:
		// success
	}

	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		return &ccsmp.SolClientErrorInfoWrapper{
			ReturnCode: ccsmp.SolClientReturnCodeWouldBlock,
		}
	}
	err = publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if _, ok := err.(*solace.PublisherOverflowError); !ok {
		t.Errorf("expected would block error, got %s", err)
	}

	subCode := 21 // ClientDeleteInProgress , note this subcode does not matter just need a subcode that is not OK.
	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		return ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCodeFail,
			ccsmp.SolClientSubCode(subCode),
			ccsmp.SolClientResponseCode(0),
			"This is a generated error info")
	}

	err = publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if msg, ok := err.(*solace.NativeError); ok {
		if msg.SubCode() != subcode.Code(subCode) {
			t.Errorf("expected sub code to be %d, got %d", subCode, msg.SubCode())
		}
	} else {
		t.Errorf("expected pubsubplus client error, got %s", err)
	}
}

func TestRequestReplyMessagePublisherTask(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	internalPublisher := &mockInternalPublisher{}
	publisher.construct(internalPublisher, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	err := publisher.Start()
	if err != nil {
		t.Error(err)
	}

	publishCalled := false
	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		publishCalled = true
		return nil
	}

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	testReplyHandler := func(msg apimessage.InboundMessage, usercontext interface{}, err error) {}
	testTimeout := time.Duration(-1) // forever

	sendTaskChannel := make(chan buffer.PublisherTask, 1)
	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		sendTaskChannel <- task
		return true
	}
	err = publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)

	if err != nil {
		t.Error(err)
	}

	var sendTask buffer.PublisherTask
	select {
	case sendTask = <-sendTaskChannel:
	default:
		t.Error("did not encounter a send task")
	}
	sendTask(make(chan struct{}))
	if !publishCalled {
		t.Error("internal publisher publish was never called")
	}
}

func TestRequestReplyMessagePublisherTaskWithWouldBlock(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	internalPublisher := &mockInternalPublisher{}
	publisher.construct(internalPublisher, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	publishCalled := false
	publishRecalled := false
	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		if !publishCalled {
			publishCalled = true
			return &ccsmp.SolClientErrorInfoWrapper{
				ReturnCode: ccsmp.SolClientReturnCodeWouldBlock,
			}
		}
		publishRecalled = true
		// subsequent calls are successful
		return nil
	}

	interruptChannel := make(chan struct{})
	awaitWritableCalled := false
	internalPublisher.awaitWritable = func(terminateSignal chan struct{}) error {
		if interruptChannel != terminateSignal {
			t.Error("expected terminate signal passed to awaitWritable to be the event executors terminate signal, it was not")
		}
		awaitWritableCalled = true
		return nil
	}

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	testReplyHandler := func(msg apimessage.InboundMessage, usercontext interface{}, err error) {}
	testTimeout := time.Duration(-1) // forever

	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		task(interruptChannel)
		return true
	}

	err := publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err != nil {
		t.Error(err)
	}
	if !awaitWritableCalled {
		t.Error("await writable not called despite being passed would block")
	}
	if !publishRecalled {
		t.Error("expected redelivery to be attempted, it was not")
	}
}

func TestRequestReplyMessagePublisherTaskWithWouldBlockInterrupted(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	internalPublisher := &mockInternalPublisher{}
	publisher.construct(internalPublisher, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	publishCalled := false
	publishRecalled := false
	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		if !publishCalled {
			publishCalled = true
			return &ccsmp.SolClientErrorInfoWrapper{
				ReturnCode: ccsmp.SolClientReturnCodeWouldBlock,
			}
		}
		t.Error("did not expect publisher's publish to be reattempted after returning error from awaitWritable")
		// subsequent calls are successful
		return nil
	}

	interruptChannel := make(chan struct{})
	awaitWritableCalled := false
	internalPublisher.awaitWritable = func(terminateSignal chan struct{}) error {
		if interruptChannel != terminateSignal {
			t.Error("expected terminate signal passed to awaitWritable to be the event executors terminate signal, it was not")
		}
		awaitWritableCalled = true
		return fmt.Errorf("some error")
	}

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	testReplyHandler := func(msg apimessage.InboundMessage, usercontext interface{}, err error) {}
	testTimeout := time.Duration(-1) // forever

	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		task(interruptChannel)
		return true
	}

	err := publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err != nil {
		t.Error(err)
	}
	if !awaitWritableCalled {
		t.Error("await writable not called despite being passed would block")
	}
	if publishRecalled {
		t.Error("expected redelivery to not be attempted when an error was received from awaitWritable")
	}
}

func TestRequestReplyMessagePublisherTaskFailureReplyOutcome(t *testing.T) {
	publisherReplyToTopic := "testReplyTopic"

	publisher := &requestReplyMessagePublisherImpl{}

	internalPublisher := &mockInternalPublisher{}
	internalPublisher.requestor = func() core.Requestor {
		mock := &mockRequestor{}
		mock.addRequestorReplyHandler = func(handler core.RequestorReplyHandler) (string, func() (messageID uint64, correlationID string), core.ErrorInfo) {
			count := uint64(0)
			return publisherReplyToTopic, func() (uint64, string) {
				count += 1
				return count, fmt.Sprintf("TEST%d", count)
			}, nil
		}
		return mock
	}

	publisher.construct(internalPublisher, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	subCode := 58 // MissingReplyTo, note this subcode does not matter and does not represent a real scenario
	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		return ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCodeFail,
			ccsmp.SolClientSubCode(subCode),
			ccsmp.SolClientResponseCode(0),
			"This is a generated error info")
	}

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	testTimeout := time.Duration(-1) // forever
	testUserContext := uint64(27182)

	testReplyHandler := func(msg apimessage.InboundMessage, usercontext interface{}, err error) {
		if msg != nil {
			t.Error("expected inbound reply message to be nil")
		}
		if usercontext != testUserContext {
			t.Error("expected usercontext to match published context")
		}

		if err == nil {
			t.Error("expected error to not be nil")
		} else {
			if casted, ok := err.(*solace.NativeError); ok {
				if casted.SubCode() != subcode.Code(subCode) {
					t.Errorf("expected sub code %d, got %d", subCode, casted.SubCode())
				}
			} else {
				t.Errorf("expected to get a PubSubPlusClientError, got %T", err)
			}
		}
	}

	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		task(make(chan struct{}))
		return true
	}

	eventExecutor.submit = func(event executor.Task) bool {
		event()
		return true
	}

	err := publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, testUserContext)
	if err != nil {
		t.Error(err)
	}
	publisher.Terminate(1 * time.Second)
}

func TestRequestReplyMessagePublisherReadinessListener(t *testing.T) {
	publisher := &requestReplyMessagePublisherImpl{}
	internalPublisher := &mockInternalPublisher{}
	publisher.construct(internalPublisher, backpressureConfigurationReject, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	testReplyHandler := func(msg apimessage.InboundMessage, usercontext interface{}, err error) {}
	testTimeout := time.Duration(-1) // forever

	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		task(make(chan struct{}))
		return true
	}

	eventExecutor.submit = func(event executor.Task) bool {
		event()
		return true
	}

	readinessCalled := false
	publisher.SetPublisherReadinessListener(func() {
		readinessCalled = true
	})

	err := publisher.Publish(testMessage, testReplyHandler, testTopic, testTimeout, nil /*properties*/, nil /*usercontext*/)
	if err != nil {
		t.Error(err)
	}
	if !readinessCalled {
		t.Error("expected readiness listener to be called, it was not")
	}
}
