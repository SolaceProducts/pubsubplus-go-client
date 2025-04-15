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

package receiver

import (
	"fmt"
	"runtime"
	"time"

	"solace.dev/go/messaging/internal/ccsmp"
	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/message"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

type requestReplyMessageReceiverImpl struct {
	directReceiver *directMessageReceiverImpl
}

func (*requestReplyMessageReceiverImpl) construct() {
}

// common functions signature for MessageReceiver interface

// IsRunning checks if the process was successfully started and not yet stopped.
// Returns true if running, false otherwise.
func (receiver *requestReplyMessageReceiverImpl) IsRunning() bool {
	return receiver.directReceiver.IsRunning()
}

// IsTerminates checks if message delivery process is terminated.
// Returns true if terminated, false otherwise.
func (receiver *requestReplyMessageReceiverImpl) IsTerminated() bool {
	return receiver.directReceiver.IsTerminated()
}

// IsTerminating checks if the delivery process termination is ongoing.
// Returns true if the message delivery process is being terminated,
// but termination is not yet complete, otherwise false.
func (receiver *requestReplyMessageReceiverImpl) IsTerminating() bool {
	return receiver.directReceiver.IsTerminating()
}

// SetTerminationNotificationListener adds a callback to listen for
// non-recoverable interruption events.
func (receiver *requestReplyMessageReceiverImpl) SetTerminationNotificationListener(listener solace.TerminationNotificationListener) {
	receiver.directReceiver.SetTerminationNotificationListener(listener)
}

// Lifecycle and subscription functions that can use the original direct receiver

// Start will start the service synchronously.
// Before this function is called, the service is considered
// off-duty. To operate normally, this function must be called on
// a receiver or publisher instance. This function is idempotent.
// Returns an error if one occurred or nil if successful.
func (receiver *requestReplyMessageReceiverImpl) Start() (err error) {
	return receiver.directReceiver.Start()
}

// StartAsync will start the service asynchronously.
// Before this function is called, the service is considered
// off-duty. To operate normally, this function must be called on
// a receiver or publisher instance. This function is idempotent.
// Returns a channel that will receive an error if one occurred or
// nil if successful. Subsequent calls will return additional
// channels that can await an error, or nil if already started.
func (receiver *requestReplyMessageReceiverImpl) StartAsync() <-chan error {
	return receiver.directReceiver.StartAsync()
}

// StartAsyncCallback will start the DirectMessageReceiver asynchronously.
// Calls the callback when started with an error if one occurred or nil
// if successful.
func (receiver *requestReplyMessageReceiverImpl) StartAsyncCallback(callback func(solace.RequestReplyMessageReceiver, error)) {
	receiver.directReceiver.StartAsyncCallback(func(_ solace.DirectMessageReceiver, err error) {
		callback(receiver, err)
	})
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
func (receiver *requestReplyMessageReceiverImpl) Terminate(gracePeriod time.Duration) (err error) {
	return receiver.directReceiver.Terminate(gracePeriod)
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
func (receiver *requestReplyMessageReceiverImpl) TerminateAsync(gracePeriod time.Duration) <-chan error {
	return receiver.directReceiver.TerminateAsync(gracePeriod)
}

// TerminateAsyncCallback will terminate the RequestReplyMessageReceiver asynchronously.
// Calls the callback when terminated with nil if successful or an error if
// one occurred. If gracePeriod is less than 0, the function will wait indefinitely.
func (receiver *requestReplyMessageReceiverImpl) TerminateAsyncCallback(gracePeriod time.Duration, callback func(error)) {
	receiver.directReceiver.TerminateAsyncCallback(gracePeriod, callback)
}

// AddSubscription will subscribe to another message source on a PubSub+ Broker to receive messages from.
// Will block until subscription is added.
// Returns a solace/errors.*IllegalStateError if the service is not running.
// Returns a solace/errors.*IllegalArgumentError if unsupported Subscription type is passed.
// Returns nil if successful.
func (receiver *requestReplyMessageReceiverImpl) AddSubscription(subscription resource.Subscription) error {
	return receiver.directReceiver.AddSubscription(subscription)
}

// RemoveSubscription will unsubscribe from a previously subscribed message source on a broker
// such that no more messages will be received from it.
// Will block until subscription is removed.
// Returns an solace/errors.*IllegalStateError if the service is not running.
// Returns a solace/errors.*IllegalArgumentError if unsupported Subscription type is passed.
// Returns nil if successful.
func (receiver *requestReplyMessageReceiverImpl) RemoveSubscription(subscription resource.Subscription) error {
	return receiver.directReceiver.RemoveSubscription(subscription)
}

// AddSubscriptionAsync will subscribe to another message source on a PubSub+ Broker to receive messages from.
// Will block until subscription is added.
// Returns a solace/errors.*IllegalStateError if the service is not running.
// Returns a solace/errors.*IllegalArgumentError if unsupported Subscription type is passed.
// Returns nil if successful.
func (receiver *requestReplyMessageReceiverImpl) AddSubscriptionAsync(subscription resource.Subscription, listener solace.SubscriptionChangeListener) error {
	return receiver.directReceiver.AddSubscriptionAsync(subscription, listener)
}

// RemoveSubscriptionAsync will unsubscribe from a previously subscribed message source on a broker
// such that no more messages will be received from it. Will block until subscription is removed.
// Returns an solace/errors.*IllegalStateError if the service is not running.
// Returns a solace/errors.*IllegalArgumentError if unsupported Subscription type is passed.
// Returns nil if successful.
func (receiver *requestReplyMessageReceiverImpl) RemoveSubscriptionAsync(subscription resource.Subscription, listener solace.SubscriptionChangeListener) error {
	return receiver.directReceiver.RemoveSubscriptionAsync(subscription, listener)
}

func (receiver *requestReplyMessageReceiverImpl) ReceiveMessage(timeout time.Duration) (apimessage.InboundMessage, solace.Replier, error) {
	inboundMessage, err := receiver.directReceiver.ReceiveMessage(timeout)
	if err != nil {
		return nil, nil, err
	}
	// check inbound message for ReplyTo destination and correlation id and create replier
	if replier, hasReply := NewReplierImpl(inboundMessage, receiver.directReceiver.internalReceiver); hasReply {
		return inboundMessage, replier, nil
	}
	return inboundMessage, nil, nil
}

// ReceiveAsync will register a callback to be called when new messages
// are received. Returns an error one occurred while registering the callback.
// If a callback is already registered, it will be replaced by the given
// callback.
func (receiver *requestReplyMessageReceiverImpl) ReceiveAsync(callback solace.RequestMessageHandler) (err error) {
	if callback != nil {
		return receiver.directReceiver.ReceiveAsync(func(msg apimessage.InboundMessage) {
			// create Replier from Inbound Message
			if replier, hasReply := NewReplierImpl(msg, receiver.directReceiver.internalReceiver); hasReply {
				callback(msg, replier)
			} else {
				if receiver.directReceiver.logger.IsDebugEnabled() {
					receiver.directReceiver.logger.Debug("Received message without request fields [correlationId or reply destination]")
				}
				callback(msg, nil)
			}
		})
	}
	return receiver.directReceiver.ReceiveAsync(nil)
}

func (receiver *requestReplyMessageReceiverImpl) String() string {
	return fmt.Sprintf("solace.RequestReplyMessageReceiver at %p", receiver)
}

// Replier impl struct
type replierImpl struct {
	internalReplier    core.Replier
	correlationID      string
	replyToDestination string
}

// NewReplierImpl function
func NewReplierImpl(requestMsg apimessage.InboundMessage, internalReceiver core.Receiver) (solace.Replier, bool) {
	replyToDestination, ok := message.GetReplyToDestinationName(requestMsg.(*message.InboundMessageImpl))
	if !ok {
		return nil, ok
	}
	correlationID, ok := requestMsg.GetCorrelationID()
	if !ok {
		return nil, ok
	}
	replier := &replierImpl{}
	replier.construct(correlationID, replyToDestination, internalReceiver.Replier())
	return replier, true
}

func (replier *replierImpl) construct(correlationID string, replyToDestination string, coreReplier core.Replier) {
	replier.internalReplier = coreReplier
	replier.correlationID = correlationID
	replier.replyToDestination = replyToDestination
}

func (replier *replierImpl) Reply(msg apimessage.OutboundMessage) error {
	if msg == nil {
		return solace.NewError(&solace.IllegalArgumentError{}, "Replier must have OutboundMessage not nil", nil)
	}
	msgImpl, ok := msg.(*message.OutboundMessageImpl)
	if !ok {
		return solace.NewError(&solace.IllegalArgumentError{}, "Replier must have OutboundMessage from OutboundMessageBuilder", nil)
	}
	replyMsg, err := message.DuplicateOutboundMessage(msgImpl)
	if err != nil {
		return err
	}
	defer func() {
		replyMsg.Dispose()
	}()
	if err = message.SetAsReplyMessage(replyMsg, replier.replyToDestination, replier.correlationID); err != nil {
		return err
	}
	if errInfo := replier.internalReplier.SendReply(message.GetOutboundMessagePointer(replyMsg)); errInfo != nil {
		// error info would block return code for simple transport reject back pressure from replier
		if errInfo.ReturnCode == ccsmp.SolClientReturnCodeWouldBlock {
			err = solace.NewError(&solace.PublisherOverflowError{}, constants.WouldBlock, nil)
		} else {
			err = core.ToNativeError(errInfo, constants.ReplierFailureToPublishReply)
		}
	}

	runtime.KeepAlive(replyMsg)
	return err
}

// Builder impl struct
type requestReplyMessageReceiverBuilderImpl struct {
	directReceiverBuilder solace.DirectMessageReceiverBuilder
}

// NewRequestReplyMessageReceiverBuilderImpl function
func NewRequestReplyMessageReceiverBuilderImpl(internalReceiver core.Receiver) solace.RequestReplyMessageReceiverBuilder {
	return &requestReplyMessageReceiverBuilderImpl{
		directReceiverBuilder: NewDirectMessageReceiverBuilderImpl(internalReceiver),
	}
}

// Build will build a new RequestReplyMessageReceiver with the given properties.
// Returns solace/errors.*InvalidConfigurationError if an invalid configuration is provided.
func (builder *requestReplyMessageReceiverBuilderImpl) Build(requestTopicSubscription resource.Subscription) (messageReceiver solace.RequestReplyMessageReceiver, err error) {
	return builder.BuildWithSharedSubscription(requestTopicSubscription, nil)
}

func (builder *requestReplyMessageReceiverBuilderImpl) BuildWithSharedSubscription(requestTopicSubscription resource.Subscription, shareName *resource.ShareName) (messageReceiver solace.RequestReplyMessageReceiver, err error) {
	if requestTopicSubscription == nil {
		return nil, solace.NewError(&solace.InvalidConfigurationError{}, "RequestReplyReceiverBuilder must have a requestTopicSubscription subscription", nil)
	}
	builder.withSubscriptions(requestTopicSubscription)
	receiver, err := builder.directReceiverBuilder.BuildWithShareName(shareName)
	if err != nil {
		return nil, err
	}
	rrReceiver := &requestReplyMessageReceiverImpl{
		directReceiver: receiver.(*directMessageReceiverImpl),
	}
	rrReceiver.construct()
	return rrReceiver, nil
}

// FromConfigurationProvider will configure the request reply receiver with the given properties.
// Built in ReceiverPropertiesConfigurationProvider implementations include:
//
//	ReceiverPropertyMap, a map of ReceiverProperty keys to values
func (builder *requestReplyMessageReceiverBuilderImpl) FromConfigurationProvider(provider config.ReceiverPropertiesConfigurationProvider) solace.RequestReplyMessageReceiverBuilder {
	builder.directReceiverBuilder.FromConfigurationProvider(provider)
	return builder
}

// OnBackPressureDropLatest will configure the receiver with the given buffer size. If the buffer
// is full and a message arrives, the incoming message will be discarded.
// bufferCapacity must be >= 1
func (builder *requestReplyMessageReceiverBuilderImpl) OnBackPressureDropLatest(bufferCapacity uint) solace.RequestReplyMessageReceiverBuilder {
	builder.directReceiverBuilder.OnBackPressureDropLatest(bufferCapacity)
	return builder
}

// OnBackPressureDropOldest will configure the receiver with the given buffer size. If the buffer
// is full and a message arrives, the oldest message in the buffer will be discarded.
// bufferCapacity must be >= 1
func (builder *requestReplyMessageReceiverBuilderImpl) OnBackPressureDropOldest(bufferCapacity uint) solace.RequestReplyMessageReceiverBuilder {
	builder.directReceiverBuilder.OnBackPressureDropOldest(bufferCapacity)
	return builder
}

// withSubscriptions will set a list of TopicSubscriptions to subscribe
// to when starting the receiver.
// for internal use for now
func (builder *requestReplyMessageReceiverBuilderImpl) withSubscriptions(topics ...resource.Subscription) solace.RequestReplyMessageReceiverBuilder {
	builder.directReceiverBuilder.WithSubscriptions(topics[0])
	return builder
}

func (builder *requestReplyMessageReceiverBuilderImpl) String() string {
	return fmt.Sprintf("solace.RequestReplyMessageReceiverBuilder at %p", builder)
}
