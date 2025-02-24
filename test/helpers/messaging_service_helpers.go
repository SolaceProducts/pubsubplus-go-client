// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package helpers contains various test helpers for assertions, for example it contains a helper to connect and disconnect a messaging service
// based on a messaging service builder.
package helpers

import (
	"fmt"
	"net/url"
	"time"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/test/sempclient/action"
	"solace.dev/go/messaging/test/sempclient/monitor"
	"solace.dev/go/messaging/test/testcontext"

	//lint:ignore ST1001 dot import is fine for tests
	. "github.com/onsi/ginkgo/v2"
	//lint:ignore ST1001 dot import is fine for tests
	. "github.com/onsi/gomega"
)

// Helper functions for various connect/disconnect functionality, paramtereizing connects and disconnects

const defaultTimeout = 10 * time.Second

// ConnectFunction type defined
type ConnectFunction func(service solace.MessagingService) error

// SynchronousConnectFunction defined
var SynchronousConnectFunction = func(service solace.MessagingService) error {
	return service.Connect()
}

// AsynchronousConnectFunction defined
var AsynchronousConnectFunction = func(service solace.MessagingService) error {
	select {
	case err := <-service.ConnectAsync():
		return err
	case <-time.After(defaultTimeout):
		Fail("timed out waiting for async connect to return")
		return nil
	}
}

// CallbackConnectFunction defined
var CallbackConnectFunction = func(service solace.MessagingService) error {
	var passedService solace.MessagingService
	cErr := make(chan error)
	service.ConnectAsyncWithCallback(func(newService solace.MessagingService, err error) {
		passedService = newService
		cErr <- err
	})
	select {
	case err := <-cErr:
		ExpectWithOffset(1, passedService).To(Equal(service), "Messaging service passed to callback did not equal calling service")
		return err
	case <-time.After(defaultTimeout):
		Fail("timed out waiting for callback connect to return")
		return nil
	}
}

// ConnectFunctions defined
var ConnectFunctions = map[string]ConnectFunction{
	"synchronous connect":  SynchronousConnectFunction,
	"asynchronous connect": AsynchronousConnectFunction,
	"callback connect":     CallbackConnectFunction,
}

// DisconnectFunction defined
type DisconnectFunction func(service solace.MessagingService) error

// SynchronousDisconnectFunction defined
var SynchronousDisconnectFunction = func(service solace.MessagingService) error {
	return service.Disconnect()
}

// AsynchronousDisconnectFunction defined
var AsynchronousDisconnectFunction = func(service solace.MessagingService) error {
	select {
	case err := <-service.DisconnectAsync():
		return err
	case <-time.After(defaultTimeout):
		Fail("timed out waiting for async disconnect to return")
		return nil
	}
}

// CallbackDisconnectFunction defined
var CallbackDisconnectFunction = func(service solace.MessagingService) error {
	cErr := make(chan error)
	service.DisconnectAsyncWithCallback(func(err error) {
		cErr <- err
	})
	select {
	case err := <-cErr:
		return err
	case <-time.After(defaultTimeout):
		Fail("timed out waiting for callback disconnect to return")
		return nil
	}
}

// DisconnectFunctions defined
var DisconnectFunctions = map[string]DisconnectFunction{
	"synchronous disconnect":  SynchronousDisconnectFunction,
	"asynchronous disconnect": AsynchronousDisconnectFunction,
	"callback disconnect":     CallbackDisconnectFunction,
}

// DefaultConfiguration defined
func DefaultConfiguration() config.ServicePropertyMap {
	connectionDetails := testcontext.Messaging()
	url := fmt.Sprintf("%s:%d", connectionDetails.Host, connectionDetails.MessagingPorts.PlaintextPort)
	config := config.ServicePropertyMap{
		config.ServicePropertyVPNName:                     connectionDetails.VPN,
		config.TransportLayerPropertyHost:                 url,
		config.AuthenticationPropertySchemeBasicUserName:  connectionDetails.Authentication.BasicUsername,
		config.AuthenticationPropertySchemeBasicPassword:  connectionDetails.Authentication.BasicPassword,
		config.TransportLayerPropertyReconnectionAttempts: 0,
	}
	return config
}

// helper to connect and disconnect the messaging service

// TestConnectDisconnectMessagingService function
func TestConnectDisconnectMessagingService(builder solace.MessagingServiceBuilder) {
	testConnectDisconnectMessagingServiceWithFunctions(builder, SynchronousConnectFunction, SynchronousDisconnectFunction, 2)
}

// TestConnectDisconnectMessagingServiceWithFunctions function
func TestConnectDisconnectMessagingServiceWithFunctions(builder solace.MessagingServiceBuilder, connectFunction ConnectFunction, disconnectFunction DisconnectFunction) {
	testConnectDisconnectMessagingServiceWithFunctions(builder, connectFunction, disconnectFunction, 2)
}

// BuildMessagingService function
func BuildMessagingService(builder solace.MessagingServiceBuilder) solace.MessagingService {
	return buildMessagingService(builder, 2)
}

// ConnectMessagingService function
func ConnectMessagingService(messagingService solace.MessagingService) *monitor.MsgVpnClient {
	return connectMessagingServiceWithFunction(messagingService, SynchronousConnectFunction, 2)
}

// ConnectMessagingServiceWithFunction function
func ConnectMessagingServiceWithFunction(messagingService solace.MessagingService, connectFunction ConnectFunction) *monitor.MsgVpnClient {
	return connectMessagingServiceWithFunction(messagingService, connectFunction, 2)
}

// DisconnectMessagingService function
func DisconnectMessagingService(messagingService solace.MessagingService) {
	disconnectMessagingServiceWithFunction(messagingService, SynchronousDisconnectFunction, 2)
}

// DisconnectMessagingServiceWithFunction function
func DisconnectMessagingServiceWithFunction(messagingService solace.MessagingService, disconnectFunction DisconnectFunction) {
	disconnectMessagingServiceWithFunction(messagingService, disconnectFunction, 2)
}

// ForceDisconnectViaSEMPv2 function
func ForceDisconnectViaSEMPv2(messagingService solace.MessagingService) {
	_, _, err := testcontext.SEMP().Action().ClientApi.
		DoMsgVpnClientDisconnect(
			testcontext.SEMP().ActionCtx(),
			action.MsgVpnClientDisconnect{},
			testcontext.Messaging().VPN,
			url.QueryEscape(messagingService.GetApplicationID()),
		)
	ExpectWithOffset(1, err).ToNot(HaveOccurred())
}

// ValidateMetric function
func ValidateMetric(messagingService solace.MessagingService, metric metrics.Metric, expectedValue uint64) {
	EventuallyWithOffset(1, func() uint64 {
		return messagingService.Metrics().GetValue(metric)
	}, 5*time.Second).Should(BeNumerically("==", expectedValue), fmt.Sprintf("Expected metric %d to eventually equal %d", metric, expectedValue))
}

// ValidateMetricWithFunction function
func ValidateMetricWithFunction(messagingService solace.MessagingService, metric metrics.Metric, expectedValue uint64, comparator string) {
	EventuallyWithOffset(1, func() uint64 {
		return messagingService.Metrics().GetValue(metric)
	}, 5*time.Second).Should(BeNumerically(comparator, expectedValue), fmt.Sprintf("Expected metric %d to eventually %s %d", metric, comparator, expectedValue))
}

// TestFailedConnectMessagingService function
func TestFailedConnectMessagingService(builder solace.MessagingServiceBuilder, validation func(error)) {
	messagingService := buildMessagingService(builder, 2)
	defer SynchronousDisconnectFunction(messagingService)
	err := SynchronousConnectFunction(messagingService)
	ExpectWithOffset(1, err).To(HaveOccurred(), "Expected error to have occurred in messaging service connect but it was nil")
	validation(err)
	ExpectWithOffset(1, messagingService.IsConnected()).To(BeFalse(), "Expected messaging service to not be connected, IsConnected() returned true!")
}

// TestConnectDisconnectMessagingServiceClientValidation function
func TestConnectDisconnectMessagingServiceClientValidation(builder solace.MessagingServiceBuilder, validation func(client *monitor.MsgVpnClient)) {
	messagingService := buildMessagingService(builder, 2)
	defer disconnectMessagingServiceWithFunction(messagingService, SynchronousDisconnectFunction, 2)
	client := connectMessagingServiceWithFunction(messagingService, SynchronousConnectFunction, 2)
	validation(client)
}

// TestConnectPublishAndReceive function
func TestConnectPublishAndReceive(builder solace.MessagingServiceBuilder, topic string, clientValidation func(client *monitor.MsgVpnClient), msgValidation func(msg message.InboundMessage)) {
	messagingService := buildMessagingService(builder, 2)
	defer disconnectMessagingServiceWithFunction(messagingService, SynchronousDisconnectFunction, 2)
	client := connectMessagingServiceWithFunction(messagingService, SynchronousConnectFunction, 2)
	if clientValidation != nil {
		clientValidation(client)
	}
	publisher, err := messagingService.CreateDirectMessagePublisherBuilder().Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to build publisher")
	receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(resource.TopicSubscriptionOf(topic)).Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to build receiver")
	ExpectWithOffset(1, publisher.Start()).ToNot(HaveOccurred(), "Failed to start publisher")
	ExpectWithOffset(1, receiver.Start()).ToNot(HaveOccurred(), "Failed to start receiver")
	ExpectWithOffset(1, publisher.PublishString("hello world", resource.TopicOf(topic))).ToNot(HaveOccurred(), "Failed to publish message")
	ExpectWithOffset(1, publisher.Terminate(1*time.Second)).ToNot(HaveOccurred(), "Failed to terminate publisher")
	msg, err := receiver.ReceiveMessage(1 * time.Second)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to receive message")
	ExpectWithOffset(1, receiver.Terminate(1*time.Second)).ToNot(HaveOccurred(), "Failed to terminate receiver")
	if msgValidation != nil {
		msgValidation(msg)
	}
}

// TestConnectPublishAndReceivePersistent function
func TestConnectPublishAndReceivePersistent(builder solace.MessagingServiceBuilder, topic string, clientValidation func(client *monitor.MsgVpnClient), msgValidation func(msg message.InboundMessage)) {
	messagingService := buildMessagingService(builder, 2)
	defer disconnectMessagingServiceWithFunction(messagingService, SynchronousDisconnectFunction, 2)
	client := connectMessagingServiceWithFunction(messagingService, SynchronousConnectFunction, 2)
	if clientValidation != nil {
		clientValidation(client)
	}
	publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to build publisher")
	receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
		WithSubscriptions(resource.TopicSubscriptionOf(topic)).
		WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).
		Build(resource.QueueNonDurableExclusiveAnonymous())
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to build receiver")
	ExpectWithOffset(1, publisher.Start()).ToNot(HaveOccurred(), "Failed to start publisher")
	ExpectWithOffset(1, receiver.Start()).ToNot(HaveOccurred(), "Failed to start receiver")
	ExpectWithOffset(1, publisher.PublishString("hello world", resource.TopicOf(topic))).ToNot(HaveOccurred(), "Failed to publish message")
	ExpectWithOffset(1, publisher.Terminate(1*time.Second)).ToNot(HaveOccurred(), "Failed to terminate publisher")
	msg, err := receiver.ReceiveMessage(1 * time.Second)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to receive message")
	ExpectWithOffset(1, receiver.Terminate(1*time.Second)).ToNot(HaveOccurred(), "Failed to terminate receiver")
	if msgValidation != nil {
		msgValidation(msg)
	}
}

// PublishOneMessage will publish a message to the given topic using the given messaging service with an
// optional payload attached as a string payload. If no string payload is provided, "hello world" is used.
func PublishOneMessage(messagingService solace.MessagingService, topic string, payload ...string) {
	str := "hello world"
	if len(payload) > 0 {
		str = payload[0]
	}
	publisher, err := messagingService.CreateDirectMessagePublisherBuilder().OnBackPressureReject(0).Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected publisher to build without error")
	ExpectWithOffset(1, publisher.Start()).ToNot(HaveOccurred(), "Expected publisher to start without error")
	ExpectWithOffset(1, publisher.PublishString(str, resource.TopicOf(topic))).ToNot(HaveOccurred(), "Expected publish to be successful")
	ExpectWithOffset(1, publisher.Terminate(10*time.Second)).ToNot(HaveOccurred(), "Expected publisher to terminate gracefully")
}

// PublishOnePersistentMessage will publish a message persistently to the given topic using the given messaging service with an
// optional payload attached as a string payload. If no string payload is provided, "hello world" is used.
func PublishOnePersistentMessage(messagingService solace.MessagingService, topic string, payload ...string) {
	str := "hello world"
	if len(payload) > 0 {
		str = payload[0]
	}
	publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().OnBackPressureReject(0).Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected publisher to build without error")
	ExpectWithOffset(1, publisher.Start()).ToNot(HaveOccurred(), "Expected publisher to start without error")
	msg, err := messagingService.MessageBuilder().BuildWithStringPayload(str)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected message ot build without error")
	ExpectWithOffset(1, publisher.PublishAwaitAcknowledgement(msg, resource.TopicOf(topic), 10*time.Second, nil)).
		ToNot(HaveOccurred(), "Expected publish to be successful")
	ExpectWithOffset(1, publisher.Terminate(10*time.Second)).ToNot(HaveOccurred(), "Expected publisher to terminate gracefully")

}

// PublishNPersistentMessages function
func PublishNPersistentMessages(messagingService solace.MessagingService, topic string, n int, template ...string) {
	str := "hello world %d"
	if len(template) > 0 {
		str = template[0]
	}
	publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().OnBackPressureWait(uint(n)).Build()
	messageReceipts := make(chan solace.PublishReceipt, n)
	publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
		messageReceipts <- pr
	})
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected publisher to build without error")
	ExpectWithOffset(1, publisher.Start()).ToNot(HaveOccurred(), "Expected publisher to start without error")
	for i := 0; i < n; i++ {
		msg, err := messagingService.MessageBuilder().BuildWithStringPayload(fmt.Sprintf(str, i))
		ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected message ot build without error")
		ExpectWithOffset(1, publisher.Publish(msg, resource.TopicOf(topic), config.MessagePropertyMap{
			config.MessagePropertyCorrelationID: fmt.Sprint(i),
		}, nil)).
			ToNot(HaveOccurred(), "Expected publish to be successful")
	}
	for i := 0; i < n; i++ {
		EventuallyWithOffset(1, messageReceipts, 10*time.Second).Should(Receive())
	}
	ExpectWithOffset(1, publisher.Terminate(10*time.Second)).ToNot(HaveOccurred(), "Expected publisher to terminate gracefully")
}

// PublishNRequestReplyMessages will publish N request-reply messages to the given topic using the given messaging service with an
// optional template attached as a string template. If no string template is provided, "hello world %d" is used.
func PublishNRequestReplyMessages(messagingService solace.MessagingService, topic string, timeOut time.Duration, n int, template ...string) chan message.InboundMessage {
	// A handler for the request-reply publisher
	replyChannel := make(chan message.InboundMessage)
	replyHandler := func(inboundMessage message.InboundMessage, userContext interface{}, err error) {
		replyChannel <- inboundMessage
	}

	publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureReject(0).Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected request-reply publisher to build without error")
	ExpectWithOffset(1, publisher.Start()).ToNot(HaveOccurred(), "Expected request-reply publisher to start without error")

	builder := messagingService.MessageBuilder()

	for i := 0; i < n; i++ {

		msgPayload := fmt.Sprintf("hello world %d", i)
		if len(template) > 0 {
			msgPayload = template[0]
		}

		msg, err := builder.BuildWithStringPayload(msgPayload)
		ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected message to build without error")

		err = publisher.Publish(msg, replyHandler, resource.TopicOf(topic), timeOut, config.MessagePropertyMap{
			config.MessagePropertyCorrelationID: fmt.Sprint(i),
		}, nil /* usercontext */)
		ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected publish to be successful")
	}
	ExpectWithOffset(1, publisher.Terminate(10*time.Second)).ToNot(HaveOccurred(), "Expected request-reply publisher to terminate gracefully")
	return replyChannel
}

// PublishRequestReplyMessages will publish N request-reply messages to the given topic using the given messaging service with an
// optional template attached as a string template. If no string template is provided, "hello world %d" is used.
func PublishRequestReplyMessages(messagingService solace.MessagingService, topic string, timeOut time.Duration, bufferSize uint, publishedMessages *int, template ...string) (publisherReplies chan message.InboundMessage, publisherSaturated, publisherComplete chan struct{}) {
	isSaturated := false
	publisherReplies = make(chan message.InboundMessage)
	publisherSaturated = make(chan struct{})
	publisherComplete = make(chan struct{})
	testComplete := make(chan struct{})

	// A handler for the request-reply publisher
	publisherReplyHandler := func(message message.InboundMessage, userContext interface{}, err error) {
		if err == nil { // Good, a reply was received
			ExpectWithOffset(1, message).ToNot(BeNil())
			publisherReplies <- message
		} else {
			// message should be nil
			ExpectWithOffset(1, message).To(BeNil())
			ExpectWithOffset(1, err).ToNot(BeNil())
		}
	}

	publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureReject(0).Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected request-reply publisher to build without error")
	ExpectWithOffset(1, publisher.Start()).ToNot(HaveOccurred(), "Expected request-reply publisher to start without error")

	builder := messagingService.MessageBuilder()

	go func() {
		defer GinkgoRecover()
	loop:
		for {
			select {
			case <-testComplete:
				break loop
			default:
			}

			msgPayload := fmt.Sprintf("hello world %d", *publishedMessages)
			if len(template) > 0 {
				msgPayload = template[0]
			}

			msg, err := builder.BuildWithStringPayload(msgPayload)
			ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected message to build without error")

			err = publisher.Publish(msg, publisherReplyHandler, resource.TopicOf(topic), timeOut, config.MessagePropertyMap{
				config.MessagePropertyCorrelationID: fmt.Sprint(*publishedMessages),
			}, nil /* usercontext */)
			ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected publish to be successful")

			if err != nil {
				ExpectWithOffset(1, err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
				break loop
			} else {
				(*publishedMessages)++
			}

			// buffer is at least halfway filled
			if !isSaturated && (uint(*publishedMessages) > bufferSize/2) {
				close(publisherSaturated)
				isSaturated = true
			}

			// all message have been published now
			if uint(*publishedMessages) >= bufferSize {
				close(testComplete)
			}
		}
		close(publisherComplete)
		// terminate the publisher after closing the channel
		ExpectWithOffset(1, publisher.Terminate(10*time.Second)).ToNot(HaveOccurred(), "Expected request-reply publisher to terminate gracefully")

	}()

	return publisherReplies, publisherSaturated, publisherComplete
}

// ReceiveOneMessage function
func ReceiveOneMessage(messagingService solace.MessagingService, topic string) chan message.InboundMessage {
	receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(resource.TopicSubscriptionOf(topic)).Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Expected receiver to build successfully")
	ExpectWithOffset(1, receiver.Start()).ToNot(HaveOccurred(), "Expected receiver to start without error")
	ret := make(chan message.InboundMessage)
	receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
		go func() {
			receiver.Terminate(10 * time.Second)
			ret <- inboundMessage
		}()
	})
	return ret
}

// ReceiveOnePersistentMessage function
func ReceiveOnePersistentMessage(receiver solace.PersistentMessageReceiver, subscription resource.Subscription, messagingService solace.MessagingService, topic string, payload string) {
	receiver.AddSubscription(subscription)
	PublishOnePersistentMessage(messagingService, topic, payload)
	msg, err := receiver.ReceiveMessage(2 * time.Second)
	Expect(err).ToNot(HaveOccurred())
	msgString, _ := msg.GetPayloadAsString()
	Expect(msgString).Should(Equal(payload))
	Expect(receiver.Ack(msg)).ToNot(HaveOccurred())
}

// Implementations
func testConnectDisconnectMessagingServiceWithFunctions(builder solace.MessagingServiceBuilder, connectFunction ConnectFunction, disconnectFunction DisconnectFunction, callDepth int) {
	messagingService := buildMessagingService(builder, callDepth+1)

	connectMessagingServiceWithFunction(messagingService, connectFunction, callDepth+1)
	disconnectMessagingServiceWithFunction(messagingService, disconnectFunction, callDepth+1)
}

func buildMessagingService(builder solace.MessagingServiceBuilder, callDepth int) solace.MessagingService {
	messagingService, err := builder.Build()
	ExpectWithOffset(callDepth, err).To(BeNil(), "Got unexpected error while building messaging service")
	return messagingService
}

func connectMessagingServiceWithFunction(messagingService solace.MessagingService, connectFunction ConnectFunction, callDepth int) *monitor.MsgVpnClient {
	success := false
	// make sure that when we connect we defer the disconnect
	defer func() {
		if !success {
			// make sure that if we fail, we disconnect the messaging service
			messagingService.Disconnect()
		}
	}()
	err := connectFunction(messagingService)
	ExpectWithOffset(callDepth, err).To(BeNil(), "Got unexpected error when connecting messaging service")

	client := getClient(messagingService, callDepth+1)
	ExpectWithOffset(callDepth, client.ClientName).To(Equal(messagingService.GetApplicationID()), "Expected ClientName to equal application ID when checking for connected client")
	success = true
	return client
}

func disconnectMessagingServiceWithFunction(messagingService solace.MessagingService, disconnectFunction func(solace.MessagingService) error, callDepth int) {
	err := disconnectFunction(messagingService)
	ExpectWithOffset(callDepth, err).To(BeNil(), "Got unexpected error when disconnecting messaging service")

	resp, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
		GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, "notexist", nil)
	ExpectWithOffset(callDepth, err).To(HaveOccurred(), "Expected SEMP to reject call checking for connected client after disconnecting client")
	decodeMonitorSwaggerError(err, &resp, callDepth+1)
	ExpectWithOffset(callDepth, resp.Meta).ToNot(BeNil(), "Expected response from SEMP to contain meta")
	ExpectWithOffset(callDepth, resp.Meta.Error_).ToNot(BeNil(), "Expected response from SEMP to contain an error")
	ExpectWithOffset(callDepth, resp.Meta.Error_.Status).To(Equal("NOT_FOUND"), "Expected response from SEMP to have error status NOT_FOUND")
}

// GetClient function
func GetClient(messagingService solace.MessagingService) *monitor.MsgVpnClient {
	return getClient(messagingService, 2)
}

// GetClientSubscriptions function
func GetClientSubscriptions(messagingService solace.MessagingService) []monitor.MsgVpnClientSubscription {
	return getClientSubscriptions(messagingService, 2)
}

func getClient(messagingService solace.MessagingService, callDepth int) *monitor.MsgVpnClient {
	clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
		GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(messagingService.GetApplicationID()), nil)
	ExpectWithOffset(callDepth, err).To(BeNil(), "Got unexpected error checking SEMP for connected client")
	ExpectWithOffset(callDepth, clientResponse.Data).To(Not(BeNil()), "Expected SEMP response data to not be nil when checking for connected client")
	return clientResponse.Data
}

// GetClientFromVPN function
func GetClientFromVPN(messagingService solace.MessagingService, vpnName string) *monitor.MsgVpnClient {
	clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
		GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), vpnName, url.QueryEscape(messagingService.GetApplicationID()), nil)
	ExpectWithOffset(1, err).To(BeNil(), "Got unexpected error checking SEMP for connected client")
	ExpectWithOffset(1, clientResponse.Data).To(Not(BeNil()), "Expected SEMP response data to not be nil when checking for connected client")
	return clientResponse.Data
}

func getClientSubscriptions(messagingService solace.MessagingService, callDepth int) []monitor.MsgVpnClientSubscription {
	clientSubscriptionsResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
		GetMsgVpnClientSubscriptions(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(messagingService.GetApplicationID()), nil)
	ExpectWithOffset(callDepth, err).To(BeNil(), "Got unexpected error checking SEMP for client subscriptions")
	ExpectWithOffset(callDepth, clientSubscriptionsResponse.Data).To(Not(BeNil()), "Expected SEMP response data to not be nil when checking for client subscriptions")
	return clientSubscriptionsResponse.Data
}

// AsyncSubscribeHandler function
func AsyncSubscribeHandler(expectedTopic *resource.TopicSubscription, expectedOperation solace.SubscriptionOperation) (chan error, solace.SubscriptionChangeListener) {
	resultChan := make(chan error)
	subscriptionChangeListener := func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
		defer GinkgoRecover()
		if expectedTopic != nil {
			Expect(subscription).To(Equal(expectedTopic))
		}
		Expect(operation).To(Equal(expectedOperation))
		resultChan <- errOrNil
	}
	return resultChan, subscriptionChangeListener
}

// CheckResourceInfo function
func CheckResourceInfo(receiver solace.PersistentMessageReceiver, dur bool, name string) {
	receiverInfo, err := receiver.ReceiverInfo()
	Expect(err).ToNot(HaveOccurred())
	resourceInfo := receiverInfo.GetResourceInfo()
	Expect(resourceInfo.IsDurable()).To(Equal(dur))
	if dur {
		Expect(resourceInfo.GetName()).To(Equal(name))
	} else {
		if name == "" {
			Expect(resourceInfo.GetName()).Should(HavePrefix("#P2P/QTMP"))
		} else {
			Expect(resourceInfo.GetName()).Should(HavePrefix("#P2P/QTMP"))
			Expect(resourceInfo.GetName()).Should(HaveSuffix(name))
		}
	}
}
