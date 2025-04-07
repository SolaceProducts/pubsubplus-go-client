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

// Package publisher is defined below
package publisher

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/executor"

	"solace.dev/go/messaging/internal/impl/publisher/buffer"

	"solace.dev/go/messaging/internal/impl/logging"

	"solace.dev/go/messaging/internal/ccsmp"

	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/message"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

type correlationEntryImpl struct {
	userContext interface{}
	handler     solace.ReplyMessageHandler
	timeout     time.Duration
	received    bool
	result      chan core.Repliable
	sentChan    chan error
}

type CorrelationEntry = *correlationEntryImpl

type ReplyOutcome = func() (apimessage.InboundMessage, error)

type requestReplyMessagePublisherImpl struct {
	basicMessagePublisher
	logger logging.LogLevelLogger

	downEventHandlerID    uint
	canSendEventHandlerID uint

	// the parameters for backpressure
	backpressureConfiguration backpressureConfiguration
	// buffers for backpressure
	buffer            chan *publishable
	taskBuffer        buffer.PublisherTaskBuffer
	bufferPublishLock sync.Mutex

	// correlation management
	rxLock                   sync.Mutex
	requestCorrelationMap    map[string]CorrelationEntry
	nextCorrelationID        func() (uint64, string)
	correlationComplete      chan struct{}
	requestCorrelateComplete chan struct{}

	// replyto subcription management
	replyToTopic string

	terminateWaitInterrupt chan struct{}
}

func (publisher *requestReplyMessagePublisherImpl) construct(internalPublisher core.Publisher, backpressureConfig backpressureConfiguration, bufferSize int) {
	publisher.basicMessagePublisher.construct(internalPublisher)
	publisher.replyToTopic = ""
	publisher.requestCorrelationMap = make(map[string]CorrelationEntry)
	publisher.backpressureConfiguration = backpressureConfig
	if publisher.backpressureConfiguration != backpressureConfigurationDirect {
		// allocate buffers
		publisher.buffer = make(chan *publishable, bufferSize)
		publisher.taskBuffer = buffer.NewChannelBasedPublisherTaskBuffer(bufferSize, publisher.internalPublisher.TaskQueue)
	}
	publisher.terminateWaitInterrupt = make(chan struct{})
	publisher.correlationComplete = make(chan struct{})
	publisher.requestCorrelateComplete = make(chan struct{})
	publisher.logger = logging.For(publisher)
}

func (publisher *requestReplyMessagePublisherImpl) onDownEvent(eventInfo core.SessionEventInfo) {
	go publisher.unsolicitedTermination(eventInfo)
}

func (publisher *requestReplyMessagePublisherImpl) onCanSend(eventInfo core.SessionEventInfo) {
	// We want to offload from the context thread whenever possible, thus we will pass this
	// task off to a new goroutine. This should be sufficient as you are guaranteed to get the
	// can send, it is just not immediate.
	go publisher.notifyReady()
}

// Start will start the service synchronously.
// Before this function is called, the service is considered
// off-duty. To operate normally, this function must be called on
// a receiver or publisher instance. This function is idempotent.
// Returns an error if one occurred or nil if successful.
func (publisher *requestReplyMessagePublisherImpl) Start() (err error) {
	// this will block until we are started if we are not first
	if proceed, err := publisher.starting(); !proceed {
		return err
	}
	publisher.logger.Debug("Start direct publisher start")
	defer func() {
		if err == nil {
			publisher.started(err)
			publisher.logger.Debug("Start publisher complete")
		} else {
			publisher.logger.Debug("Start complete with error: " + err.Error())
			publisher.internalPublisher.Events().RemoveEventHandler(publisher.downEventHandlerID)
			publisher.internalPublisher.Events().RemoveEventHandler(publisher.canSendEventHandlerID)
			publisher.terminated(nil)
			publisher.startFuture.Complete(err)
		}
	}()
	publisher.downEventHandlerID = publisher.internalPublisher.Events().AddEventHandler(core.SolClientEventDown, publisher.onDownEvent)
	// startup functionality
	if publisher.backpressureConfiguration != backpressureConfigurationDirect {
		go publisher.taskBuffer.Run()
	} else {
		// if we are direct, we want to register to receive can send events
		publisher.canSendEventHandlerID = publisher.internalPublisher.Events().AddEventHandler(core.SolClientEventCanSend, publisher.onCanSend)
	}
	var errorInfo core.ErrorInfo
	publisher.replyToTopic, publisher.nextCorrelationID, errorInfo = publisher.internalPublisher.Requestor().AddRequestorReplyHandler(func(msg core.Repliable, correlationId string) bool {
		return publisher.handleReplyMessage(msg, correlationId)
	})
	if errorInfo != nil {
		return core.ToNativeError(errorInfo, "encountered error while adding publisher reply message callback: ")
	}
	go publisher.eventExecutor.Run()
	return nil
}

// StartAsync will start the service asynchronously.
// Before this function is called, the service is considered
// off-duty. To operate normally, this function must be called on
// a receiver or publisher instance. This function is idempotent.
// Returns a channel that will receive an error if one occurred or
// nil if successful. Subsequent calls will return additional
// channels that can await an error, or nil if already started.
func (publisher *requestReplyMessagePublisherImpl) StartAsync() <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- publisher.Start()
		close(result)
	}()
	return result
}

// StartAsyncCallback will start the RequestReplyMessagePublisher asynchronously.
// Calls the callback when started with an error if one occurred or nil
// if successful.
func (publisher *requestReplyMessagePublisherImpl) StartAsyncCallback(callback func(solace.RequestReplyMessagePublisher, error)) {
	go func() {
		callback(publisher, publisher.Start())
	}()
}

// Terminate will terminate the service gracefully and synchronously.
// This function is idempotent. The only way to resume operation
// after this function is called is to create a new instance.
// Any attempt to call this function renders the instance
// permanently terminated, even if this function completes.
// A graceful shutdown will be attempted within the grace period.
// A grace period of 0 implies a non-graceful shutdown that ignores
// unfinished tasks or in-flight messages.
// This function blocks until the service is terminated.
// If gracePeriod is less than 0, the function will wait indefinitely.
func (publisher *requestReplyMessagePublisherImpl) Terminate(gracePeriod time.Duration) (err error) {
	if proceed, err := publisher.terminate(); !proceed {
		return err
	}
	publisher.logger.Debug("Terminate direct publisher start")
	// make sure the service is marked as terminated
	defer func() {
		publisher.terminated(err)
		if err != nil {
			publisher.logger.Debug("Terminate complete with error: " + err.Error())
		} else {
			publisher.logger.Debug("Terminate complete")
		}
	}()

	defer func() {
		publisher.logger.Debug("Awaiting termination of event executor")
		// Allow the event executor to terminate, blocking until it does
		publisher.eventExecutor.AwaitTermination()
	}()

	// We're terminating, we do not care about the down event handler anymore
	publisher.internalPublisher.Events().RemoveEventHandler(publisher.downEventHandlerID)

	// create timer if needed
	var timer *time.Timer
	if gracePeriod >= 0 {
		timer = time.NewTimer(gracePeriod)
	}

	// handle graceful shutdown
	graceful := true

	publisher.logger.Debug("Have buffered backpressure, terminating the task buffer gracefully")

	if publisher.backpressureConfiguration != backpressureConfigurationDirect {
		// First we interrupt all backpressure wait functions
		close(publisher.terminateWaitInterrupt)
		graceful = publisher.taskBuffer.Terminate(timer)
	} else {
		publisher.internalPublisher.Events().RemoveEventHandler(publisher.canSendEventHandlerID)
	}

	// wait for the task buffer to drain
	if !graceful {
		publisher.logger.Debug("Task buffer terminated ungracefully, will not wait for acknowledgements to be processed")
	} else {
		publisher.logger.Debug("Waiting for correlation table to drain")
		// wait for request reply outcomes
		publisher.rxLock.Lock()
		outstandingReplies := len(publisher.requestCorrelationMap)
		close(publisher.requestCorrelateComplete)
		publisher.rxLock.Unlock()
		if outstandingReplies > 0 {
			if timer != nil {
				select {
				case <-publisher.correlationComplete:
					// success
				case <-timer.C:
					graceful = false
				}
			} else {
				// Block forever as our grace period is negative
				<-publisher.correlationComplete
			}
		}
	}
	// remove reply handler for publisher
	publisher.internalPublisher.Requestor().RemoveRequestorReplyHandler(publisher.replyToTopic)

	// close the buffer, failing any publishes
	// this must happen before counting the remaining publish message and replies

	publisherTerminatedError := solace.NewError(&solace.IncompleteMessageDeliveryError{}, constants.UnableToPublishAlreadyTerminated, nil)
	var undeliveredCount uint64 = 0
	if publisher.backpressureConfiguration != backpressureConfigurationDirect {
		// only drain the queue if we are in buffered backpressure scenarios
		undeliveredCount = publisher.drainQueue(time.Now(), publisherTerminatedError)
	}

	// close correlation channel
	publisher.rxLock.Lock()
	select {
	case <-publisher.correlationComplete:
		//already closed
	default:
		// close channel
		close(publisher.correlationComplete)
	}
	// unblock all correlation for drain

	for _, entry := range publisher.requestCorrelationMap {
		// close sent channel signal
		select {
		case <-entry.sentChan:
			// already closed
		default:
			close(entry.sentChan)
		}
		// close result channel
		close(entry.result)
	}
	publisher.rxLock.Unlock()

	// block until all replies are complete
	<-publisher.correlationComplete

	if undeliveredCount > 0 {
		// return an error if we have one
		publisher.internalPublisher.IncrementMetric(core.MetricPublishMessagesTerminationDiscarded, uint64(undeliveredCount))
		err := solace.NewError(&solace.IncompleteMessageDeliveryError{}, fmt.Sprintf(constants.IncompleteMessageDeliveryMessage, undeliveredCount), nil)
		return err
	}

	// finish cleanup successfully
	return nil
}

func (publisher *requestReplyMessagePublisherImpl) unsolicitedTermination(errorInfo core.SessionEventInfo) {
	if proceed, _ := publisher.terminate(); !proceed {
		return
	}
	if publisher.logger.IsDebugEnabled() {
		publisher.logger.Debug("Received unsolicited termination with event info " + errorInfo.GetInfoString())
		defer publisher.logger.Debug("Unsolicited termination complete")
	}
	timestamp := time.Now()
	publisher.internalPublisher.Events().RemoveEventHandler(publisher.downEventHandlerID)

	var err error = nil
	if publisher.backpressureConfiguration != backpressureConfigurationDirect {
		close(publisher.terminateWaitInterrupt)
		// Close the task buffer without waiting for any more tasks to be processed
		publisher.taskBuffer.TerminateNow()
		// check that all messages have been delivered, and return an error if they have not been
		undeliveredCount := publisher.drainQueue(timestamp, errorInfo.GetError())
		if undeliveredCount > 0 {
			if publisher.logger.IsDebugEnabled() {
				publisher.logger.Debug(fmt.Sprintf("Terminated with %d undelivered messages", undeliveredCount))
			}
			// return an error if we have one
			err = solace.NewError(&solace.IncompleteMessageDeliveryError{}, fmt.Sprintf(constants.IncompleteMessageDeliveryMessage, undeliveredCount), nil)
			publisher.internalPublisher.IncrementMetric(core.MetricPublishMessagesTerminationDiscarded, undeliveredCount)
		}
	} else {
		publisher.internalPublisher.Events().RemoveEventHandler(publisher.canSendEventHandlerID)
	}
	// Terminate the event executor without waiting for the termination to complete
	publisher.eventExecutor.Terminate()
	publisher.terminated(err)
	// Call the callback
	if publisher.terminationListener != nil {
		publisher.terminationListener(&publisherTerminationEvent{
			timestamp,
			errorInfo.GetError(),
		})
	}
}

// TerminateAsync will terminate the service asynchronously.
// This function is idempotent. The only way to resume operation
// after this function is called is to create a new instance.
// Any attempt to call this function renders the instance
// permanently terminated, even if this function completes.
// A graceful shutdown will be attempted within the grace period.
// A grace period of 0 implies a non-graceful shutdown that ignores
// unfinished tasks or in-flight messages.
// Returns a channel that will receive an error if one occurred or
// nil if successfully and gracefully terminated.
// If gracePeriod is less than 0, the function will wait indefinitely.
func (publisher *requestReplyMessagePublisherImpl) TerminateAsync(gracePeriod time.Duration) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- publisher.Terminate(gracePeriod)
		close(result)
	}()
	return result
}

// TerminateAsyncCallback will terminate the RequestReplyMessagePublisher asynchronously.
// Calls the callback when terminated with nil if successful or an error if
// one occurred. If gracePeriod is less than 0, the function will wait indefinitely.
func (publisher *requestReplyMessagePublisherImpl) TerminateAsyncCallback(gracePeriod time.Duration, callback func(error)) {
	go func() {
		callback(publisher.Terminate(gracePeriod))
	}()
}

// IsReady checks if the publisher can publish messages. Returns true if the
// publisher can publish messages, false if the publisher is presvented from
// sending messages (e.g., full buffer or I/O problems)
func (publisher *requestReplyMessagePublisherImpl) IsReady() bool {
	return publisher.IsRunning() && (publisher.backpressureConfiguration != backpressureConfigurationReject || len(publisher.buffer) != cap(publisher.buffer))
}

// NotifyWhenReady makes a request to notify the application when the
// publisher is ready. This function will block until the publisher
// is ready.
func (publisher *requestReplyMessagePublisherImpl) NotifyWhenReady() {
	if publisher.IsReady() {
		publisher.notifyReady()
	}
}

// queues a new ready event on the event executor
func (publisher *requestReplyMessagePublisherImpl) notifyReady() {
	readinessListener := publisher.readinessListener
	if readinessListener != nil {
		publisher.eventExecutor.Submit(executor.Task(readinessListener))
	}
}

// drainQueue will drain the message buffer and return the number of undelivered messages. For each remaining correlation entry push out replyHandler callback
func (publisher *requestReplyMessagePublisherImpl) drainQueue(shutdownTime time.Time, err error) uint64 {
	close(publisher.buffer)
	undeliveredCount := uint64(0)
	for undelivered := range publisher.buffer {
		underliveredRef := undelivered
		undeliveredCount++
		// handle each unsent request
		correlationID, ok := underliveredRef.message.GetCorrelationID()
		if ok {
			publisher.signalRequestCorrelationSent(correlationID, err)
		} else {
			publisher.logger.Info(fmt.Sprintf("Failed to unblock unsent publish %p without correlationID, is the publisher terminated?", underliveredRef.message))
		}
	}
	return undeliveredCount
}

// PublishBytes will publish a message of type byte array to the given destination.
// Returns an error if one occurred while attempting to publish or if the publisher
// is not started/terminated. Returns an error if one occurred. Possible errors include
// - solace/solace.*PubSubPlusClientError if the message could not be sent and all retry attempts failed.
// - solace/solace.*PublisherOverflowError if publishing messages faster than publisher's I/O
// capabilities allow. When publishing can be resumed, registered PublisherReadinessListeners
// will be called.
func (publisher *requestReplyMessagePublisherImpl) PublishBytes(bytes []byte, replyMessageHandler solace.ReplyMessageHandler, dest *resource.Topic, replyTimeout time.Duration, userContext interface{}) error {
	msg, err := publisher.messageBuilder.BuildWithByteArrayPayload(bytes)
	if err != nil {
		return err
	}
	// we built the message so it is safe to cast
	outcomeHandler, err := publisher.publishAsync(msg.(*message.OutboundMessageImpl), replyMessageHandler, dest, replyTimeout, userContext)
	if err != nil {
		return err
	}
	go outcomeHandler()
	return nil
}

// PublishString will publish a message of type string to the given destination.
// Returns an error if one occurred. Possible errors include:
// - solace/solace.*PubSubPlusClientError if the message could not be sent and all retry attempts failed.
// - solace/solace.*PublisherOverflowError if publishing messages faster than publisher's I/O
// capabilities allow. When publishing can be resumed, registered PublisherReadinessListeners
// will be called.
func (publisher *requestReplyMessagePublisherImpl) PublishString(str string, replyMessageHandler solace.ReplyMessageHandler, dest *resource.Topic, replyTimeout time.Duration, userContext interface{}) error {
	msg, err := publisher.messageBuilder.BuildWithStringPayload(str)
	if err != nil {
		return err
	}
	// we built the message so it is safe to cast
	outcomeHandler, err := publisher.publishAsync(msg.(*message.OutboundMessageImpl), replyMessageHandler, dest, replyTimeout, userContext)
	if err != nil {
		return err
	}
	go outcomeHandler()
	return nil
}

// Publish will publish the given message of type OutboundMessage
// with the given properties. These properties will override the properties on
// the OutboundMessage instance if present. Possible errors include:
// - solace/solace.*PubSubPlusClientError if the message could not be sent and all retry attempts failed.
// - solace/solace.*PublisherOverflowError if publishing messages faster than publisher's I/O
// capabilities allow. When publishing can be resumed, registered PublisherReadinessListeners
// will be called.
func (publisher *requestReplyMessagePublisherImpl) Publish(msg apimessage.OutboundMessage, replyMessageHandler solace.ReplyMessageHandler, dest *resource.Topic, replyTimeout time.Duration, properties config.MessagePropertiesConfigurationProvider, userContext interface{}) error {
	if err := publisher.checkStartedStateForPublish(); err != nil {
		return err
	}
	msgImpl, ok := msg.(*message.OutboundMessageImpl)
	if !ok {
		return solace.NewError(&solace.IllegalArgumentError{}, fmt.Sprintf(constants.InvalidOutboundMessageType, msg), nil)
	}
	msgDup, err := message.DuplicateOutboundMessage(msgImpl)
	if err != nil {
		return err
	}
	if properties != nil {
		err := message.SetProperties(msgDup, properties.GetConfiguration())
		if err != nil {
			msgDup.Dispose()
			return err
		}
	}
	outcomeHandler, err := publisher.publishAsync(msgDup, replyMessageHandler, dest, replyTimeout, userContext)
	if err != nil {
		return err
	}
	go outcomeHandler()
	return nil
}

func (publisher *requestReplyMessagePublisherImpl) PublishAwaitResponse(msg apimessage.OutboundMessage, dest *resource.Topic, replyTimeout time.Duration, properties config.MessagePropertiesConfigurationProvider) (apimessage.InboundMessage, error) {
	// stub for now
	// need to work out usage with call to publisher.publish
	if err := publisher.checkStartedStateForPublish(); err != nil {
		return nil, err
	}
	msgImpl, ok := msg.(*message.OutboundMessageImpl)
	if !ok {
		return nil, solace.NewError(&solace.IllegalArgumentError{}, fmt.Sprintf(constants.InvalidOutboundMessageType, msg), nil)
	}
	msgDup, err := message.DuplicateOutboundMessage(msgImpl)
	if err != nil {
		return nil, err
	}
	if properties != nil {
		err := message.SetProperties(msgDup, properties.GetConfiguration())
		if err != nil {
			msgDup.Dispose()
			return nil, err
		}
	}
	outcomeHandler, err := publisher.publish(msgDup, nil, dest, replyTimeout, nil)
	if err != nil {
		return nil, err
	}
	return outcomeHandler()
}

func (publisher *requestReplyMessagePublisherImpl) publishAsync(msg *message.OutboundMessageImpl, replyMessageHandler solace.ReplyMessageHandler, dest *resource.Topic, replyTimeout time.Duration, userContext interface{}) (retOutcome ReplyOutcome, ret error) {
	if replyMessageHandler == nil {
		err := solace.NewError(&solace.IllegalArgumentError{}, constants.MissingReplyMessageHandler, nil)
		return nil, err
	}
	return publisher.publish(msg, replyMessageHandler, dest, replyTimeout, userContext)
}

// publish impl taking a dup'd message, assuming state has been checked and we are running
func (publisher *requestReplyMessagePublisherImpl) publish(msg *message.OutboundMessageImpl, replyMessageHandler solace.ReplyMessageHandler, dest *resource.Topic, replyTimeout time.Duration, userContext interface{}) (retOutcome ReplyOutcome, ret error) {
	// There is a potential race condition in this function in buffered scenarios whereby a message is pushed into backpressure
	// after the publisher has moved from Started to Terminated if the routine is interrupted after the state check and not resumed
	// until much much later. Therefore, it may be possible for a message to get into the publisher buffers but not actually
	// be put out to the wire as the publisher's task buffer may shut down immediately after. This would result in an unpublished
	// message that was submitted to publish successfully. In reality, this condition's window is so rediculously tiny that it
	// can be considered a non-problem. Also (at the time of writing) this race condition is present in all other next-gen APIs.

	// Set the destination for the message which is assumed to be a dup'd message.
	err := message.SetDestination(msg, dest.GetName())
	if err != nil {
		msg.Dispose()
		return nil, err
	}

	// check the state once more before moving into the publish paths
	if err := publisher.checkStartedStateForPublish(); err != nil {
		return nil, err
	}

	// set the message replyTo destination
	if err := message.SetReplyToDestination(msg, publisher.replyToTopic); err != nil {
		msg.Dispose()
		return nil, err
	}

	// under lock generate correlation information and store in management struct
	correlationID, replyOutcome := publisher.createReplyCorrelation(userContext, replyTimeout, replyMessageHandler)
	defer func() {
		if ret != nil {
			publisher.closeReplyCorrelation(correlationID)
			retOutcome = nil
		}
	}()

	// set the message correlation id
	if err := message.SetCorrelationID(msg, correlationID); err != nil {
		msg.Dispose()
		return nil, err
	}

	// handle publish through back pressure
	if publisher.backpressureConfiguration == backpressureConfigurationDirect {
		defer msg.Dispose()
		// publish directly with CCSMP
		errorInfo := publisher.internalPublisher.Publish(message.GetOutboundMessagePointer(msg))
		if errorInfo != nil {
			publisher.signalRequestCorrelationSent(correlationID, core.ToNativeError(errorInfo, "encountered error while publishing message: "))
			if errorInfo.ReturnCode == ccsmp.SolClientReturnCodeWouldBlock {
				return nil, solace.NewError(&solace.PublisherOverflowError{}, constants.WouldBlock, nil)
			}
			return nil, core.ToNativeError(errorInfo)
		} else {
			publisher.signalRequestCorrelationSent(correlationID, nil)
		}
	} else {
		// buffered backpressure scenarios

		// this section is to handle the case where a publish proceeds after we have moved to terminating, specifically
		// in the ungraceful termination case, and we have decided that no more messages should be published, thus the
		// message queue is closed. The window for this race is very small, but it is still worth handling.
		channelWrite := false
		defer func() {
			if !channelWrite {
				// we have not written to the channel yet, we may or may not have received panic, so check
				if r := recover(); r != nil {
					// if we have a panic, and that panic is send on closed channel, we can return an error by setting "ret", otherwise repanic
					if err, ok := r.(error); ok && err.Error() == "send on closed channel" {
						publisher.logger.Debug("Caught a channel closed panic when trying to write to the message buffer, publisher must be terminated.")
						ret = solace.NewError(&solace.IllegalStateError{}, constants.UnableToPublishAlreadyTerminated, nil)
						retOutcome = nil
					} else {
						// this shouldn't ever happen, but panics are unpredictable. We want this message to make it into the logs
						publisher.logger.Error(fmt.Sprintf("Experienced panic while attempting to publish a message: %s", err))
						panic(r)
					}
				}
			}
		}()
		publisher.bufferPublishLock.Lock()
		defer publisher.bufferPublishLock.Unlock()
		pub := &publishable{msg, dest}
		if publisher.backpressureConfiguration == backpressureConfigurationReject {
			select {
			case publisher.buffer <- pub:
				channelWrite = true // we successfully wrote the message to the channel
			default:
				return nil, solace.NewError(&solace.PublisherOverflowError{}, constants.WouldBlock, nil)
			}
		} else {
			// wait forever
			select {
			case publisher.buffer <- pub:
				channelWrite = true // we successfully wrote the message to the channel
			case <-publisher.terminateWaitInterrupt:
				return nil, solace.NewError(&solace.IllegalStateError{}, constants.UnableToPublishAlreadyTerminated, nil)
			}
		}
		// if we successfully wrote to the channel (which should always be true at this point), submit the task and terminate.
		if !publisher.taskBuffer.Submit(publisher.sendTask(msg, dest, correlationID)) {
			// if we couldn't submit the task, log. This may happen on shutdown in a race between the task buffer shutting down
			// and the message buffer being drained, at which point we are terminating ungracefully.
			publisher.logger.Debug("Attempted to submit the message for publishing, but the task buffer rejected the task! Has the service been terminated?")
			// At this point, we have a message that made it into the buffer but the task did not get submitted.
			// This message will be counted as "not delivered" when terminate completes.
			// This is very unlikely as the message buffer is closed much earlier than the task buffer,
			// so this window is very small. It is best to handle this when we can though.
		}
	}
	return replyOutcome, nil
}

// sendTask represents the task that is submitted to the internal task buffer and ultimately the shared serialized publisher instance
// returned closure accepts a channel that will receive a notification when any waits should be interrupted
func (publisher *requestReplyMessagePublisherImpl) sendTask(msg *message.OutboundMessageImpl, dest resource.Destination, correlationID string) buffer.PublisherTask {
	return func(terminateChannel chan struct{}) {
		var errorInfo core.ErrorInfo
		// main publish loop
		for {
			// attempt a publish
			errorInfo = publisher.internalPublisher.Publish(message.GetOutboundMessagePointer(msg))
			if errorInfo != nil {
				// if we got a would block, wait for ready and retry
				if errorInfo.ReturnCode == ccsmp.SolClientReturnCodeWouldBlock {
					err := publisher.internalPublisher.AwaitWritable(terminateChannel)
					if err != nil {
						// if we encountered an error while waiting for writable, the publisher will shut down
						// and this task will not complete. The message queue will be drained by the caller of
						// terminate, so we should not deal with the message.
						publisher.signalRequestCorrelationSent(correlationID, err)
						return
					}
					continue
					// otherwise we got another error, should deal with it accordingly
				}
				publisher.signalRequestCorrelationSent(correlationID, core.ToNativeError(errorInfo, "encountered error while publishing message: "))
			} else {
				// if there is no errorInfo then the message was sent.
				publisher.signalRequestCorrelationSent(correlationID, nil)
			}
			// exit out of the loop if we succeeded or got an error
			// we will only continue on would_block + AwaitWritable
			break
		}
		isFull := len(publisher.buffer) == cap(publisher.buffer)
		// remove msg from buffer, should be guaranteed to be there, but we don't want to deadlock in case something went wonky.
		// shutdown is contingent on all active tasks completing.
		select {
		case pub, ok := <-publisher.buffer:
			if ok {
				// from the pub get the correlation id
				// extract the correlation management state from the publisher with the correlation id
				// This must be protected from access as the transport thread can update independently
				// Only if we were the ones to drain the message from the buffer should we call the replyhandler from the correlation management if there is a callback
				if errorInfo != nil /* && requestHandler != nil */ {
					// Only if we were the ones to drain the message from the buffer should we call the the request handler
					// if we call the replyhandler with an error, we should not dispose of the message
					if !publisher.eventExecutor.Submit(func() { /* call request specific reply handler */ }) &&
						publisher.logger.IsInfoEnabled() {
						publisher.logger.Info(fmt.Sprintf("Failed to submit publish reply handler callback for correlation %v. Is the publisher terminated?", correlationID))
					}
				} else {
					// clean up the message, we are finished with it in the direct messaging case
					// slightly more efficient to dispose of the message than let GC clean it up
					pub.message.Dispose()
				}
				// check if we should signal that the buffer has space
				// we only have to call the publisher notification of being ready when we
				// have successfully popped a message off the buffer
				if isFull && publisher.backpressureConfiguration == backpressureConfigurationReject {
					publisher.notifyReady()
				}

			}
			// We must have a closed buffer with no more messages. Since the buffer was closed, we can safely ignore the message.
		default:
			// should never happen as the message queue should always be drained after
			publisher.logger.Error("published a message after publisher buffer was drained, this is unexpected")
		}
	}
}

func (entry CorrelationEntry) construct(userContext interface{}, timeout time.Duration, handler solace.ReplyMessageHandler) {
	entry.userContext = userContext
	entry.timeout = timeout
	entry.handler = handler
	entry.received = false
	entry.result = make(chan core.Repliable, 1)
	entry.sentChan = make(chan error, 1)
}

func (publisher *requestReplyMessagePublisherImpl) closeReplyCorrelation(correlationID string) {
	publisher.rxLock.Lock()
	defer publisher.rxLock.Unlock()

	entry, ok := publisher.requestCorrelationMap[correlationID]
	if !ok {
		return
	}
	delete(publisher.requestCorrelationMap, correlationID)
	// handle signal for emptied CorrelationMap under lock

	// close channels
	select {
	case <-entry.result:
		// already closed
	default:
		close(entry.result)
	}
	select {
	case <-entry.sentChan:
		// already closed
	default:
		close(entry.sentChan)
	}

	// measure correlation table and signal 0 if needed
	select {
	case <-publisher.requestCorrelateComplete:
		if len(publisher.requestCorrelationMap) == 0 {
			select {
			case <-publisher.correlationComplete:
				// already closed
			default:
				// close the channel
				close(publisher.correlationComplete)
			}
		}
	default:
		// termination not called do nothing
	}
}

func (publisher *requestReplyMessagePublisherImpl) signalRequestCorrelationSent(correlationID string, sentErr error) {
	publisher.rxLock.Lock()
	defer publisher.rxLock.Unlock()
	entry, ok := publisher.requestCorrelationMap[correlationID]
	if !ok {
		return
	}
	entry.sentChan <- sentErr
}

func (publisher *requestReplyMessagePublisherImpl) createReplyCorrelation(userContext interface{}, timeout time.Duration, handler solace.ReplyMessageHandler) (string, func() (apimessage.InboundMessage, error)) {
	publisher.rxLock.Lock()
	defer publisher.rxLock.Unlock()
	// create correlation id
	_, correlationID := publisher.nextCorrelationID()

	// create entry for id
	entry := &correlationEntryImpl{}
	entry.construct(userContext, timeout, handler)

	publisher.requestCorrelationMap[correlationID] = entry

	// return closure function that blocks until result or timeout or pulbisher invalid state

	return correlationID, func() (retMsg apimessage.InboundMessage, retErr error) {
		retErr = nil
		var ok bool = true
		var sentErr error = nil
		// wait for request send
		select {
		case sentErr, ok = <-entry.sentChan:
			if !ok {
				sentErr = solace.NewError(&solace.IllegalStateError{}, constants.RequestReplyPublisherCannotReceiveReplyAlreadyTerminated, nil)
			}
		case <-publisher.correlationComplete:
			sentErr = solace.NewError(&solace.IllegalStateError{}, constants.RequestReplyPublisherCannotReceiveReplyAlreadyTerminated, nil)
		}

		// if not sent dispatch outcome of reply
		if sentErr != nil {
			retErr = sentErr
			goto DispatchOutcome
		}

		if entry.timeout >= 0 {
			timer := time.NewTimer(timeout)
			select {
			case msgP, ok := <-entry.result:
				timer.Stop()
				if ok {
					retMsg = message.NewInboundMessage(msgP, false)
				} else {
					retErr = solace.NewError(&solace.IllegalStateError{}, constants.RequestReplyPublisherCannotReceiveReplyAlreadyTerminated, nil)
				}
			case <-timer.C:
				retErr = solace.NewError(&solace.TimeoutError{}, constants.RequestReplyPublisherTimedOutWaitingForReply, nil)
			case <-publisher.correlationComplete:
				timer.Stop()
				retErr = solace.NewError(&solace.IllegalStateError{}, constants.RequestReplyPublisherCannotReceiveReplyAlreadyTerminated, nil)
			}
		} else {
			// timeout < 0 blocks forever
			select {
			case msgP, ok := <-entry.result:
				//success
				if ok {
					retMsg = message.NewInboundMessage(msgP, false)
				} else {
					retErr = solace.NewError(&solace.IllegalStateError{}, constants.RequestReplyPublisherCannotReceiveReplyAlreadyTerminated, nil)
				}
			case <-publisher.correlationComplete:
				retErr = solace.NewError(&solace.IllegalStateError{}, constants.RequestReplyPublisherCannotReceiveReplyAlreadyTerminated, nil)
			}
		}
		// only check correlation if there is no error
		if retErr == nil {
			// check if the correlation is tracked in publisher
			publisher.rxLock.Lock()
			_, ok := publisher.requestCorrelationMap[correlationID]
			if !ok {
				publisher.logger.Debug(fmt.Sprintf("Got request reply result with no correlation entry for correlation id '%s'.", correlationID))
				retErr = solace.NewError(&solace.IllegalStateError{}, constants.RequestReplyPublisherCannotReceiveReplyAlreadyTerminated, nil)
				if retMsg != nil {
					retMsg.Dispose()
					retMsg = nil
				}
			}
			publisher.rxLock.Unlock()
		}
	DispatchOutcome:
		if entry.handler != nil {
			// ReplyHanlder callback
			entry.handler(retMsg, entry.userContext, retErr)
		}
		// remove correlation
		publisher.closeReplyCorrelation(correlationID)
		return retMsg, retErr
	}
}

func (publisher *requestReplyMessagePublisherImpl) handleReplyMessage(msgP core.Repliable, correlationID string) (ret bool) {
	defer func() {
		if r := recover(); r != nil {
			// there can be a race between pushing the reply to result channel
			if err, rok := r.(error); rok && err.Error() == "send on closed channel" {
				publisher.logger.Debug("Caught a channel closed panic when trying to write to the result channel, request must have been cancelled or timed out")
			} else {
				// this shouldn't ever happen, but panics are unpredictable. We want this message to make it into the logs
				publisher.logger.Error(fmt.Sprintf("Caught panic in reply message callback! %s\n%s", err, string(debug.Stack())))
			}
			ret = false
		}
	}()
	publisher.rxLock.Lock()
	defer publisher.rxLock.Unlock()
	corEntry, ok := publisher.requestCorrelationMap[correlationID]
	if !ok {
		publisher.logger.Debug(fmt.Sprintf("Received reply message[0x%x] with correlationID[%s] without correlation entry for publisher", msgP, correlationID))
		return false
	}
	if corEntry.received {
		// return false to return the message
		publisher.logger.Debug(fmt.Sprintf("Received reply message[0x%x] with correlationID[%s] that already has response", msgP, correlationID))
		return false
	}
	corEntry.received = true
	corEntry.result <- msgP
	return true
}

func (publisher *requestReplyMessagePublisherImpl) String() string {
	return fmt.Sprintf("solace.RequestReplyMessagePublisher at %p", publisher)
}

type requestReplyMessagePublisherBuilderImpl struct {
	internalPublisher core.Publisher
	properties        map[config.PublisherProperty]interface{}
}

// NewRequestReplyMessagePublisherBuilderImpl function
func NewRequestReplyMessagePublisherBuilderImpl(internalPublisher core.Publisher) solace.RequestReplyMessagePublisherBuilder {
	return &requestReplyMessagePublisherBuilderImpl{
		internalPublisher: internalPublisher,
		// default properties
		// TODO change the default properties if necessary?
		properties: constants.DefaultDirectPublisherProperties.GetConfiguration(),
	}
}

// Build will build a new RequestReplyMessagePublisher instance based on the configured properties.
// Returns solace/solace.*InvalidConfigurationError if an invalid configuration is provided.
func (builder *requestReplyMessagePublisherBuilderImpl) Build() (messagePublisher solace.RequestReplyMessagePublisher, err error) {
	backpressureConfig, publisherBackpressureBufferSize, err := validateBackpressureConfig(builder.properties)
	if err != nil {
		return nil, err
	}
	publisher := &requestReplyMessagePublisherImpl{}
	publisher.construct(builder.internalPublisher, backpressureConfig, publisherBackpressureBufferSize)
	return publisher, nil
}

// OnBackPressureReject will set the publisher backpressure strategy to reject
// where publish attempts will be rejected once the bufferSize, in number of messages, is reached.
// If bufferSize is 0, an error will be thrown when the transport is full when publishing.
// Valid bufferSize is >= 0.
func (builder *requestReplyMessagePublisherBuilderImpl) OnBackPressureReject(bufferSize uint) solace.RequestReplyMessagePublisherBuilder {
	builder.properties[config.PublisherPropertyBackPressureStrategy] = config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull
	builder.properties[config.PublisherPropertyBackPressureBufferCapacity] = bufferSize
	return builder
}

// OnBackPressureWait will set the publisher backpressure strategy to wait where publish
// attempts will block until there is space in the buffer of size bufferSize in number of messages.
// Valid bufferSize is >= 1.
func (builder *requestReplyMessagePublisherBuilderImpl) OnBackPressureWait(bufferSize uint) solace.RequestReplyMessagePublisherBuilder {
	builder.properties[config.PublisherPropertyBackPressureStrategy] = config.PublisherPropertyBackPressureStrategyBufferWaitWhenFull
	builder.properties[config.PublisherPropertyBackPressureBufferCapacity] = bufferSize
	return builder
}

// FromConfigurationProvider will configure the direct publisher with the given properties.
// Built in PublisherPropertiesConfigurationProvider implementations include:
//
//	PublisherPropertyMap, a map of PublisherProperty keys to values
//	for loading of properties from a string configuration (files or other configuration source)
func (builder *requestReplyMessagePublisherBuilderImpl) FromConfigurationProvider(provider config.PublisherPropertiesConfigurationProvider) solace.RequestReplyMessagePublisherBuilder {
	if provider == nil {
		return builder
	}
	for key, value := range provider.GetConfiguration() {
		builder.properties[key] = value
	}
	return builder
}

func (builder *requestReplyMessagePublisherBuilderImpl) String() string {
	return fmt.Sprintf("solace.RequestReplyMessagePublisherBuilder at %p", builder)
}
