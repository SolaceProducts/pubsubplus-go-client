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
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"solace.dev/go/messaging/internal/ccsmp"

	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/message"

	"solace.dev/go/messaging/internal/impl/constants"

	"solace.dev/go/messaging/internal/impl/executor"
	"solace.dev/go/messaging/internal/impl/publisher/buffer"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
)

func TestDirectMessagePublisherBuilderWithValidBackpressure(t *testing.T) {
	backpressureConfigurations := []func(builder solace.DirectMessagePublisherBuilder) (solace.DirectMessagePublisherBuilder, backpressureConfiguration, int){
		func(builder solace.DirectMessagePublisherBuilder) (solace.DirectMessagePublisherBuilder, backpressureConfiguration, int) {
			return builder.OnBackPressureReject(0), backpressureConfigurationDirect, 0

		},
		func(builder solace.DirectMessagePublisherBuilder) (solace.DirectMessagePublisherBuilder, backpressureConfiguration, int) {
			bufferSize := 1
			return builder.OnBackPressureReject(uint(bufferSize)), backpressureConfigurationReject, bufferSize
		},
		func(builder solace.DirectMessagePublisherBuilder) (solace.DirectMessagePublisherBuilder, backpressureConfiguration, int) {
			bufferSize := 1
			return builder.OnBackPressureWait(uint(bufferSize)), backpressureConfigurationWait, bufferSize
		},
	}
	shared := &mockInternalPublisher{}
	for _, config := range backpressureConfigurations {
		builder, backpressureConfig, capacity := config(NewDirectMessagePublisherBuilderImpl(shared))
		publisher, err := builder.Build()
		if err != nil {
			t.Error(err)
		}
		if publisher == nil {
			t.Error("expected publisher to not be nil")
		}
		publisherImpl := publisher.(*directMessagePublisherImpl)
		if publisherImpl.backpressureConfiguration != backpressureConfig {
			t.Errorf("expected backpressure config to equal %d, was %d", backpressureConfig, publisherImpl.backpressureConfiguration)
		}
		if cap(publisherImpl.buffer) != capacity {
			t.Errorf("expected backpressure capacity to equal %d, was %d", capacity, cap(publisherImpl.buffer))
		}
	}
}

func TestDirectMessagePublisherBuilderWithInvalidBackpressureWait(t *testing.T) {
	publisher, err := NewDirectMessagePublisherBuilderImpl(&mockInternalPublisher{}).OnBackPressureWait(0).Build()
	// we should get an error saying that buffer must be > 0 for wait
	if err == nil {
		t.Error("expected error to not be nil")
	}
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
}

func TestDirectMessagePublisherBuilderWithCustomPropertiesStructFromJSON(t *testing.T) {
	jsonData := `{"solace":{"messaging":{"publisher":{"back-pressure":{"strategy":"BUFFER_WAIT_WHEN_FULL","buffer-capacity": 100,"buffer-wait-timeout": 1000}}}}}`
	baselineProperties := make(config.PublisherPropertyMap)
	err := json.Unmarshal([]byte(jsonData), &baselineProperties)
	if err != nil {
		t.Error(err)
	}
	publisher, err := NewDirectMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher == nil {
		t.Error("expected publisher to not be nil")
	}
	if err != nil {
		t.Error(err)
	}
	publisherImpl := publisher.(*directMessagePublisherImpl)
	if publisherImpl.backpressureConfiguration != backpressureConfigurationWait {
		t.Errorf("expected backpressure config to equal %d, was %d", backpressureConfigurationWait, publisherImpl.backpressureConfiguration)
	}
	if cap(publisherImpl.buffer) != 100 {
		t.Errorf("expected backpressure capacity to equal %d, was %d", 100, cap(publisherImpl.buffer))
	}
}

func TestDirectMessagePublisherBuilderWithInvalidCustomPropertiesMapNegativeBufferCapacity(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: -1,
		config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
	}
	publisher, err := NewDirectMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure capacity is negative")
	}
}

func TestDirectMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongTypeBufferCapacity(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: "hello",
		config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferWaitWhenFull,
	}
	publisher, err := NewDirectMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure capacity is a string")
	}
}

func TestDirectMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongTypeStrategy(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: 1,
		config.PublisherPropertyBackPressureStrategy:       23,
	}
	publisher, err := NewDirectMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure strategy is an integer")
	}
}

func TestDirectMessagePublisherBuilderWithInvalidCustomPropertiesMapWrongStrategy(t *testing.T) {
	baselineProperties := config.PublisherPropertyMap{
		config.PublisherPropertyBackPressureBufferCapacity: 1,
		config.PublisherPropertyBackPressureStrategy:       "hello world",
	}
	publisher, err := NewDirectMessagePublisherBuilderImpl(&mockInternalPublisher{}).FromConfigurationProvider(baselineProperties).Build()
	if publisher != nil {
		t.Error("expected publisher to be nil")
	}
	if err == nil {
		t.Error("expected error when backpressure strategy is an integer")
	}
}

func TestDirectMessagePublisherImplLifecycle(t *testing.T) {
	gracePeriod := 10 * time.Second

	// parameterize this test with the various start and terminate functions (sync/async)
	startAndTerminatFunctions := []struct {
		start     func(publisher *directMessagePublisherImpl)
		terminate func(publisher *directMessagePublisherImpl)
	}{
		{
			start: func(publisher *directMessagePublisherImpl) {
				err := publisher.Start()
				if err != nil {
					t.Error("expected error to be nil, got " + err.Error())
				}
			},
			terminate: func(publisher *directMessagePublisherImpl) {
				err := publisher.Terminate(gracePeriod)
				if err != nil {
					t.Error("expected error to be nil, got " + err.Error())
				}
			},
		},
		{
			start: func(publisher *directMessagePublisherImpl) {
				select {
				case err := <-publisher.StartAsync():
					if err != nil {
						t.Error("expected error to be nil, got " + err.Error())
					}
				case <-time.After(100 * time.Millisecond):
					t.Error("timed out waiting for publisher to start")
				}
			},
			terminate: func(publisher *directMessagePublisherImpl) {
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
			start: func(publisher *directMessagePublisherImpl) {
				done := make(chan struct{})
				publisher.StartAsyncCallback(func(retPub solace.DirectMessagePublisher, err error) {
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
					t.Error("timed out waiting for direct publisher to start")
				}
			},
			terminate: func(publisher *directMessagePublisherImpl) {
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
					t.Error("timed out waiting for direct publisher to start")
				}
			},
		},
	}
	for _, fns := range startAndTerminatFunctions {
		publisher := &directMessagePublisherImpl{}
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
			t.Error("timed out waiting for event executor to termiante")
		}
		select {
		case <-taskBufferTerminated:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Error("timed out waiting for event executor to terminate")
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

func TestDirectMessagePublisherImplLifecycleNoBuffer(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
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
		t.Error("timed out waiting for event executor to termiante")
	}
	select {
	case <-taskBufferTerminated:
		t.Error("did not expect task buffer to be terminated as it was never started")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	// check terminated states
	if err != nil {
		t.Error("expected error to be nil, got " + err.Error())
	}
}

func TestDirectMessagePublisherLifecycleIdempotence(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
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
}

func TestDirectMessagePublisherTerminateWithUnpublishedMessages(t *testing.T) {
	internalPublisher := &mockInternalPublisher{}
	publisher := &directMessagePublisherImpl{}
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
		publisher.buffer <- nil
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

func TestDirectMessagePublisherUnsolicitedTerminationWithUnpublishedMessages(t *testing.T) {
	internalPublisher := &mockInternalPublisher{}
	publisher := &directMessagePublisherImpl{}
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
		publisher.buffer <- nil
	}
	errForEvent := fmt.Errorf("some error")
	publisher.onDownEvent(&mockEvent{err: errForEvent})

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

func TestCallPublishWhenNotStarted(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	err := publisher.Publish(testMessage, testTopic)
	if err == nil {
		t.Error("expected publish to fail when publisher not started")
	}
}

func TestCallPublishWhenAlreadyTerminated(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()
	publisher.Terminate(1 * time.Second)

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")
	err := publisher.Publish(testMessage, testTopic)
	if err == nil {
		t.Error("expected publish to fail when publisher already terminated")
	}
}

func TestCallPublishWithBadPayload(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
	publisher.construct(&mockInternalPublisher{}, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	testTopic := resource.TopicOf("hello/world")
	err := publisher.Publish(nil, testTopic)
	if err == nil {
		t.Error("expected publish to fail when publisher already terminated")
	}
}

func TestDirectMessagePublisherPublishFunctionalityBufferedWait(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
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
	err := publisher.Publish(testMessage, testTopic)
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
		err = publisher.Publish(testMessage, testTopic)
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
		publishFailed <- publisher.Publish(testMessage, testTopic)
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

	err = publisher.Publish(testMessage, testTopic)
	if err != nil {
		t.Error(err)
	}
}

func TestDirectMessagePublisherPublishFunctionalityBufferedReject(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
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
	err := publisher.Publish(testMessage, testTopic)
	if err != nil {
		t.Error(err)
	}

	select {
	case <-taskBufferSubmitCalled:
		// success
	default:
		t.Error("Expect task buffer submit to be called")
	}

	err = publisher.Publish(testMessage, testTopic)
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

	err = publisher.Publish(testMessage, testTopic)
	if err != nil {
		t.Error(err)
	}
}

func TestDirectMessagePublisherPublishFunctionalityDirect(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
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
	err := publisher.Publish(testMessage, testTopic)
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
	err = publisher.Publish(testMessage, testTopic)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if _, ok := err.(*solace.PublisherOverflowError); !ok {
		t.Errorf("expected would block error, got %s", err)
	}

	subCode := 21
	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		return ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCodeFail,
			ccsmp.SolClientSubCode(subCode),
			ccsmp.SolClientResponseCode(0),
			"This error info is generated")
	}

	err = publisher.Publish(testMessage, testTopic)
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

func TestDirectMessagePublisherTask(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
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

	sendTaskChannel := make(chan buffer.PublisherTask, 1)
	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		sendTaskChannel <- task
		return true
	}

	err = publisher.Publish(testMessage, testTopic)
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

func TestDirectMessagePublisherTaskWithWouldBlock(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
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

	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		task(interruptChannel)
		return true
	}

	err := publisher.Publish(testMessage, testTopic)
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

func TestDirectMessagePublisherTaskWithWouldBlockInterrupted(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
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

	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		task(interruptChannel)
		return true
	}

	err := publisher.Publish(testMessage, testTopic)
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

func TestDirectMessagePublisherTaskFailure(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
	internalPublisher := &mockInternalPublisher{}
	publisher.construct(internalPublisher, backpressureConfigurationWait, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	subCode := 23
	internalPublisher.publish = func(message ccsmp.SolClientMessagePt) core.ErrorInfo {
		return ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCodeFail,
			ccsmp.SolClientSubCode(subCode),
			ccsmp.SolClientResponseCode(0),
			"This is a generated error info")
	}

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")

	taskBuffer.submit = func(task buffer.PublisherTask) bool {
		task(make(chan struct{}))
		return true
	}

	eventExecutor.submit = func(event executor.Task) bool {
		event()
		return true
	}

	publisher.SetPublishFailureListener(func(event solace.FailedPublishEvent) {
		// TODO validate message
		if event.GetMessage() == nil {
			t.Error("expected message to not be nil")
		}
		if event.GetDestination() == nil {
			t.Error("expected destination to not be nil")
		} else if event.GetDestination().GetName() != testTopic.GetName() {
			t.Errorf("expected destination name to match %s, got %s", testTopic.GetName(), event.GetDestination().GetName())
		}
		if time.Since(event.GetTimeStamp()) > 1*time.Second {
			t.Errorf("event timestamp was outside of grace period of 1 second. now is %s, timestamp is %s", time.Now(), event.GetTimeStamp())
		}
		if event.GetError() == nil {
			t.Error("expected error to not be nil")
		} else {
			if casted, ok := event.GetError().(*solace.NativeError); ok {
				if casted.SubCode() != subcode.Code(subCode) {
					t.Errorf("expected sub code %d, got %d", subCode, casted.SubCode())
				}
			} else {
				t.Errorf("expected to get a PubSubPlusClientError, got %T", event.GetError())
			}
		}
	})

	err := publisher.Publish(testMessage, testTopic)
	if err != nil {
		t.Error(err)
	}
}

func TestDirectMessagePublisherReadinessListener(t *testing.T) {
	publisher := &directMessagePublisherImpl{}
	internalPublisher := &mockInternalPublisher{}
	publisher.construct(internalPublisher, backpressureConfigurationReject, 1)
	eventExecutor := &mockEventExecutor{}
	taskBuffer := &mockTaskBuffer{}
	publisher.eventExecutor = eventExecutor
	publisher.taskBuffer = taskBuffer

	publisher.Start()

	testMessage, _ := message.NewOutboundMessage()
	testTopic := resource.TopicOf("hello/world")

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

	err := publisher.Publish(testMessage, testTopic)
	if err != nil {
		t.Error(err)
	}
	if !readinessCalled {
		t.Error("expected readiness listener to be called, it was not")
	}
}

type mockTaskBuffer struct {
	run          func()
	submit       func(task buffer.PublisherTask) bool
	terminate    func(timer *time.Timer) bool
	terminateNow func()
}

// the main executor loop that should be started on a new goroutine
func (buffer *mockTaskBuffer) Run() {
	if buffer.run != nil {
		buffer.run()
	}
}

// Call to submit into the buffer, will succeed unless we are terminating
func (buffer *mockTaskBuffer) Submit(task buffer.PublisherTask) bool {
	if buffer.submit != nil {
		return buffer.submit(task)
	}
	return true
}

// Call to terminate that will attempt to shutdown gracefully
func (buffer *mockTaskBuffer) Terminate(timer *time.Timer) bool {
	if buffer.terminate != nil {
		return buffer.terminate(timer)
	}
	return true
}

func (buffer *mockTaskBuffer) TerminateNow() {
	if buffer.terminateNow != nil {
		buffer.terminateNow()
	}
}

type mockEventExecutor struct {
	run              func()
	submit           func(task executor.Task) bool
	terminate        func()
	awaitTermination func()
	terminateNow     func()
}

func (executor *mockEventExecutor) Run() {
	if executor.run != nil {
		executor.run()
	}
}

func (executor *mockEventExecutor) Submit(task executor.Task) bool {
	if executor.submit != nil {
		return executor.submit(task)
	}
	return true
}

func (executor *mockEventExecutor) Terminate() {
	if executor.terminate != nil {
		executor.terminate()
	}
}

func (executor *mockEventExecutor) TerminateNow() {
	if executor.terminateNow != nil {
		executor.terminateNow()
	}
}

func (executor *mockEventExecutor) AwaitTermination() {
	if executor.awaitTermination != nil {
		executor.awaitTermination()
	}
}

type mockEvent struct {
	err error
}

func (event mockEvent) GetError() error {
	return event.err
}

func (event mockEvent) GetInfoString() string {
	return ""
}

func (event mockEvent) GetCorrelationPointer() unsafe.Pointer {
	return nil
}

func (event mockEvent) GetUserPointer() unsafe.Pointer {
	return nil
}
