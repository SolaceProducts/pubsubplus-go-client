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

package test

import (
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const gracePeriod = 10 * time.Second
const invalidTopicString = "//>"

var _ = Describe("DirectReceiver", func() {
	var messagingService solace.MessagingService
	BeforeEach(func() {
		var err error
		messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration()).Build()
		Expect(err).To(BeNil())
	})

	It("can print a subscription to a string", func() {
		const subscriptionString = "some-subscription"
		subscription := resource.TopicSubscriptionOf(subscriptionString)
		Expect(subscription.String()).To(ContainSubstring(subscriptionString))
		Expect(subscription.GetSubscriptionType()).To(ContainSubstring("TopicSubscription"))
	})

	It("fails to build when given an invalid backpressure type", func() {
		receiver, err := messagingService.CreateDirectMessageReceiverBuilder().FromConfigurationProvider(config.ReceiverPropertyMap{
			config.ReceiverPropertyDirectBackPressureStrategy: "not a strategy",
		}).Build()
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
		Expect(receiver).To(BeNil())
	})

	It("fails to build when given an invalid subscription", func() {
		badSubscription := &myCustomSubscription{}
		receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(badSubscription).Build()
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
		Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("%T", badSubscription)))
		Expect(receiver).To(BeNil())
	})

	It("fails to start on unstarted messaging service", func() {
		receiver, err := messagingService.CreateDirectMessageReceiverBuilder().Build()
		Expect(err).ToNot(HaveOccurred())

		err = receiver.Start()
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
	})

	It("fails to start on terminated messaging service", func() {
		receiver, err := messagingService.CreateDirectMessageReceiverBuilder().Build()
		Expect(err).ToNot(HaveOccurred())

		helpers.ConnectMessagingService(messagingService)
		helpers.DisconnectMessagingService(messagingService)

		err = receiver.Start()
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
	})

	It("fails to receive a message on an unstarted receiver", func() {
		receiver := helpers.NewDirectReceiver(messagingService)
		msg, err := receiver.ReceiveMessage(-1)
		Expect(msg).To(BeNil())
		helpers.ValidateError(err, &solace.IllegalStateError{})
	})

	Context("with a messaging service that will be disconnected", func() {
		const topicString = "terminate/me"
		const messagesPublished = 3

		var terminationChannel chan solace.TerminationEvent
		var receiver solace.DirectMessageReceiver

		BeforeEach(func() {
			helpers.ConnectMessagingService(messagingService)
			var err error
			receiver, err = messagingService.CreateDirectMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
			Expect(err).ToNot(HaveOccurred())
			err = receiver.Start()
			Expect(err).ToNot(HaveOccurred())

			terminationChannel = make(chan solace.TerminationEvent)
			startTime := time.Now()
			receiver.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
				defer GinkgoRecover()
				Expect(te.GetTimestamp()).To(BeTemporally(">=", startTime))
				Expect(te.GetTimestamp()).To(BeTemporally("<=", time.Now()))
				Expect(te.GetCause()).ToNot(BeNil())
				Expect(te.GetMessage()).ToNot(BeEmpty())
				terminationChannel <- te
			})
		})

		AfterEach(func() {
			if !receiver.IsTerminated() {
				receiver.Terminate(1 * time.Second)
			}
			if messagingService.IsConnected() {
				messagingService.Disconnect()
			}
		})

		forceDisconnectFunctions := map[string]func(messagingService solace.MessagingService){
			"messaging service disconnect": func(messagingService solace.MessagingService) {
				helpers.DisconnectMessagingService(messagingService)
			},
			"SEMPv2 disconnect": func(messagingService solace.MessagingService) {
				helpers.ForceDisconnectViaSEMPv2(messagingService)
			},
		}

		for testCase, disconnectFunctionRef := range forceDisconnectFunctions {
			disconnectFunction := disconnectFunctionRef
			It("terminates the receiver using async receive when disconnecting with "+testCase, func() {
				blocker := make(chan struct{})
				msgsReceived := make(chan message.InboundMessage, messagesPublished)

				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					<-blocker
					msgsReceived <- inboundMessage
				})

				for i := 0; i < messagesPublished; i++ {
					helpers.PublishOneMessage(messagingService, topicString, fmt.Sprint(i))
				}

				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, messagesPublished)

				disconnectFunction(messagingService)

				Eventually(receiver.IsTerminated, 10*time.Second).Should(BeTrue())

				// unblock the receiver callback after we are marked as terminated
				close(blocker)

				var expectedMsg message.InboundMessage
				Eventually(msgsReceived).Should(Receive(&expectedMsg))
				payload, ok := expectedMsg.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal(fmt.Sprint(0)))

				Consistently(msgsReceived).ShouldNot(Receive())
				Eventually(terminationChannel).Should(Receive())
				helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, messagesPublished-1)
			})
			It("terminates the receiver using sync receive when disconnecting with "+testCase, func() {
				for i := 0; i < messagesPublished; i++ {
					helpers.PublishOneMessage(messagingService, topicString, fmt.Sprint(i))
				}

				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, messagesPublished)

				discardOffset := 0
				disconnectFunction(messagingService)

				// Try and race with the unsolicited termination
				var err error = nil
				for err == nil {
					var racingMessage message.InboundMessage
					racingMessage, err = receiver.ReceiveMessage(-1)
					if racingMessage != nil {
						discardOffset++
					}
				}

				Eventually(receiver.IsTerminated, 10*time.Second).Should(BeTrue())
				Eventually(terminationChannel).Should(Receive())

				msg, err := receiver.ReceiveMessage(-1)
				Expect(msg).To(BeNil())
				helpers.ValidateError(err, &solace.IllegalStateError{})

				helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, messagesPublished-uint64(discardOffset))
			})
		}
	})

	Context("with a started messaging service", func() {
		var messageBuilder solace.OutboundMessageBuilder
		var publisher solace.DirectMessagePublisher
		var builder solace.DirectMessageReceiverBuilder

		BeforeEach(func() {
			var err error
			err = messagingService.Connect()
			Expect(err).To(BeNil())

			messageBuilder = messagingService.MessageBuilder()

			publisher, err = messagingService.CreateDirectMessagePublisherBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			err = publisher.Start()
			Expect(err).ToNot(HaveOccurred())

			builder = messagingService.CreateDirectMessageReceiverBuilder()
		})

		AfterEach(func() {
			var err error
			err = publisher.Terminate(30 * time.Second)
			Expect(err).To(BeNil())

			err = messagingService.Disconnect()
			Expect(err).To(BeNil())
		})

		startFunctions := map[string](func(solace.DirectMessageReceiver) error){
			"Start": func(dmr solace.DirectMessageReceiver) error {
				return dmr.Start()
			},
			"StartAsync": func(dmr solace.DirectMessageReceiver) error {
				return <-dmr.StartAsync()
			},
			"StartAsyncCallback": func(dmr solace.DirectMessageReceiver) error {
				startChan := make(chan error)
				dmr.StartAsyncCallback(func(passedDmr solace.DirectMessageReceiver, e error) {
					Expect(passedDmr).To(Equal(dmr))
					startChan <- e
				})
				return <-startChan
			},
		}
		for startFunctionName, startFunction := range startFunctions {
			start := startFunction
			It("should be able to receive a message successfully using start function "+startFunctionName, func() {
				topicString := "try-me"
				receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
				Expect(err).ToNot(HaveOccurred())

				msgChannel := make(chan message.InboundMessage)
				messageHandler := func(msg message.InboundMessage) {
					msgChannel <- msg
				}

				receiver.ReceiveAsync(messageHandler)

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = start(receiver)
				Expect(err).ToNot(HaveOccurred())

				// send message
				payload := "Hello World"
				msg, err := messageBuilder.BuildWithStringPayload(payload)
				Expect(err).To(BeNil())
				topic := resource.TopicOf(topicString)
				publisher.Publish(msg, topic)

				// check that the message was sent via semp
				client := helpers.GetClient(messagingService)
				Expect(client.DataTxMsgCount).To(Equal(int64(1)))

				select {
				case receivedMessage := <-msgChannel:
					Expect(receivedMessage).ToNot(BeNil())
					content, ok := receivedMessage.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(content).To(Equal(payload))
				case <-time.After(gracePeriod):
					Fail("Timed out waiting to receive message")
				}
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(1)))
			})
			It("should be able to synchronously receive a message successfully using start function "+startFunctionName, func() {
				topicString := "try-me"
				receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
				Expect(err).ToNot(HaveOccurred())

				msgChannel := make(chan message.InboundMessage)

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = start(receiver)
				Expect(err).ToNot(HaveOccurred())

				// send message
				payload := "Hello World"
				msg, err := messageBuilder.BuildWithStringPayload(payload)
				Expect(err).To(BeNil())
				topic := resource.TopicOf(topicString)
				publisher.Publish(msg, topic)

				// check that the message was sent via semp
				client := helpers.GetClient(messagingService)
				Expect(client.DataTxMsgCount).To(Equal(int64(1)))

				go func() {
					defer GinkgoRecover()
					msg, err := receiver.ReceiveMessage(-1)
					Expect(err).ToNot(HaveOccurred())
					msgChannel <- msg
				}()

				select {
				case receivedMessage := <-msgChannel:
					Expect(receivedMessage).ToNot(BeNil())
					content, ok := receivedMessage.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(content).To(Equal(payload))
				case <-time.After(gracePeriod):
					Fail("Timed out waiting to receive message")
				}
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(1)))
			})
		}

		terminateFunctions := map[string](func(solace.DirectMessageReceiver) error){
			"Terminate": func(dmr solace.DirectMessageReceiver) error {
				return dmr.Terminate(gracePeriod)
			},
			"TerminateAsync": func(dmr solace.DirectMessageReceiver) error {
				return <-dmr.TerminateAsync(gracePeriod)
			},
			"TerminateAsyncCallback": func(dmr solace.DirectMessageReceiver) error {
				errChan := make(chan error)
				dmr.TerminateAsyncCallback(gracePeriod, func(e error) {
					errChan <- e
				})
				return <-errChan
			},
		}

		for terminateFunctionName, terminateFunction := range terminateFunctions {
			terminate := terminateFunction
			It("can terminate using "+terminateFunctionName, func() {
				topicString := "try-me"
				receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
				Expect(err).ToNot(HaveOccurred())

				msgChannel := make(chan message.InboundMessage)
				messageHandler := func(msg message.InboundMessage) {
					msgChannel <- msg
				}

				Expect(receiver.ReceiveAsync(messageHandler)).ToNot(HaveOccurred())

				defer func() {
					// defer is okay since terminate is idempotent
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())

				Expect(receiver.IsRunning()).To(BeTrue())

				err = terminate(receiver)
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.IsTerminated()).To(BeTrue())
			})

		}

		It("gives correct state for terminating with blocked receiver", func() {
			topicString := "try-me"
			receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
			Expect(err).ToNot(HaveOccurred())

			blocker := make(chan struct{})
			received := make(chan struct{})
			messageHandler := func(msg message.InboundMessage) {
				close(received)
				<-blocker
			}
			receiver.ReceiveAsync(messageHandler)

			defer func() {
				// defer is okay since terminate is idempotent
				err = receiver.Terminate(gracePeriod)
				Expect(err).To(BeNil())
			}()

			helpers.ValidateState(receiver, false, false, false)
			err = receiver.Start()
			Expect(err).ToNot(HaveOccurred())
			helpers.ValidateState(receiver, true, false, false)

			// send message
			payload := "Hello World"
			msg, err := messageBuilder.BuildWithStringPayload(payload)
			Expect(err).To(BeNil())
			topic := resource.TopicOf(topicString)
			publisher.Publish(msg, topic)
			// we should now have messageHandler blocked
			select {
			case <-received:
				// success
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be received")
			}

			errChan := receiver.TerminateAsync(gracePeriod)
			// allow termination to start
			time.Sleep(100 * time.Millisecond)

			helpers.ValidateState(receiver, false, true, false)

			select {
			case <-errChan:
				Fail("did not expect to receive error when callback is still running")
			case <-time.After(100 * time.Millisecond):
				// success
			}

			close(blocker)
			select {
			case err = <-errChan:
				// success
				Expect(err).ToNot(HaveOccurred())
			case <-time.After(100 * time.Millisecond):
				Fail("timed out waiting for terminate channel to receive a result")
			}
			helpers.ValidateState(receiver, false, false, true)
		})

		It("should start idempotently", func() {
			topicString := "try-me"
			receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
			Expect(err).ToNot(HaveOccurred())

			msgChannel := make(chan message.InboundMessage)
			messageHandler := func(msg message.InboundMessage) {
				msgChannel <- msg
			}

			receiver.ReceiveAsync(messageHandler)

			defer func() {
				err = receiver.Terminate(gracePeriod)
				Expect(err).To(BeNil())
			}()
			errChan1 := receiver.StartAsync()
			errChan2 := receiver.StartAsync()
			select {
			case <-errChan1:
				// success
			case <-time.After(100 * time.Millisecond):
				Fail("timed out waiting for receiver to start asynchronously")
			}
			select {
			case <-errChan2:
				// success
			case <-time.After(1 * time.Millisecond):
				Fail("did not get second start signal")
			}
			Expect(receiver.IsRunning()).To(BeTrue())

			// send message
			payload := "Hello World"
			msg, err := messageBuilder.BuildWithStringPayload(payload)
			Expect(err).To(BeNil())
			topic := resource.TopicOf(topicString)
			publisher.Publish(msg, topic)

			// check that the message was sent via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(messagingService.GetApplicationID()), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data.DataTxMsgCount).To(Equal(int64(1)))

			select {
			case receivedMessage := <-msgChannel:
				Expect(receivedMessage).ToNot(BeNil())
				content, ok := receivedMessage.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(content).To(Equal(payload))
			case <-time.After(gracePeriod):
				Fail("Timed out waiting to receive message")
			}
			Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(1)))

			select {
			case <-msgChannel:
				Fail("did not expect to receive another message from msgChannel")
			case <-time.After(100 * time.Millisecond):
				// no more messages were received, subscription was only added once
			}
		})

		It("should terminate idempotently", func() {
			topicString := "try-me"
			receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
			Expect(err).ToNot(HaveOccurred())

			msgChannel := make(chan message.InboundMessage)
			messageHandler := func(msg message.InboundMessage) {
				msgChannel <- msg
			}

			receiver.ReceiveAsync(messageHandler)

			defer func() {
				err = receiver.Terminate(gracePeriod)
				Expect(err).To(BeNil())
			}()
			err = receiver.Start()
			Expect(err).ToNot(HaveOccurred())

			errChan1 := receiver.TerminateAsync(gracePeriod)
			errChan2 := receiver.TerminateAsync(gracePeriod)
			select {
			case <-errChan1:
				// success
			case <-time.After(100 * time.Millisecond):
				Fail("timed out waiting for receiver to start asynchronously")
			}
			select {
			case <-errChan2:
				// success
			case <-time.After(1 * time.Millisecond):
				Fail("did not get second start signal")
			}
			Expect(receiver.IsRunning()).To(BeFalse())
		})

		addSubscriptionCases := map[string](func(resource.Subscription, solace.DirectMessageReceiver) error){
			"synchronous subscribe": func(ts resource.Subscription, dmr solace.DirectMessageReceiver) error {
				return dmr.AddSubscription(ts)
			},
			"asynchronous subscribe": func(ts resource.Subscription, dmr solace.DirectMessageReceiver) error {
				type ret struct {
					subscription resource.Subscription
					operation    solace.SubscriptionOperation
					errOrNil     error
				}
				complete := make(chan ret)
				err := dmr.AddSubscriptionAsync(ts, func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
					complete <- ret{subscription, operation, errOrNil}
				})
				if err != nil {
					return err
				}
				val := <-complete
				Expect(val.subscription.GetName()).To(Equal(ts.GetName()))
				Expect(val.operation).To(Equal(solace.SubscriptionAdded))
				return val.errOrNil
			},
		}
		for caseName, caseFunction := range addSubscriptionCases {
			subscribe := caseFunction
			It("should be able to add subscriptions using "+caseName+" while running", func() {
				topicString := "try-me"
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				msgChannel := make(chan message.InboundMessage)
				messageHandler := func(msg message.InboundMessage) {
					msgChannel <- msg
				}
				receiver.ReceiveAsync(messageHandler)

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())

				msg, err := messageBuilder.BuildWithStringPayload("hello world")
				Expect(err).ToNot(HaveOccurred())

				topic := resource.TopicOf(topicString)
				publisher.Publish(msg, topic)

				clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(messagingService.GetApplicationID()), nil)
				Expect(err).To(BeNil())
				Expect(clientResponse.Data.DataTxMsgCount).To(Equal(int64(0)))
				Expect(clientResponse.Data.DataTxMsgCount).To(Equal(int64(0)))

				select {
				case <-msgChannel:
					Fail("did not expect to receive message")
				case <-time.After(100 * time.Millisecond):
					// success
				}

				err = subscribe(resource.TopicSubscriptionOf(topicString), receiver)
				Expect(err).ToNot(HaveOccurred())

				publisher.Publish(msg, topic)

				// check that the message was sent via semp
				clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(messagingService.GetApplicationID()), nil)
				Expect(err).To(BeNil())
				Expect(clientResponse.Data.DataTxMsgCount).To(Equal(int64(1)))

				select {
				case receivedMessage := <-msgChannel:
					Expect(receivedMessage).ToNot(BeNil())
				case <-time.After(gracePeriod):
					Fail("Timed out waiting to receive message")
				}
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(1)))
			})
			It("should fail to add an invalid subscription using "+caseName, func() {
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())

				err = subscribe(resource.TopicSubscriptionOf(invalidTopicString), receiver)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.NativeError{}))
			})
			It("should fail to add an invalid subscription type using "+caseName, func() {
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())

				err = subscribe(&myCustomSubscription{}, receiver)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
			})
			It("should fail to add a subscription on an unstarted receiver using "+caseName, func() {
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				err = subscribe(resource.TopicSubscriptionOf("some topic"), receiver)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			})
			It("should fail to add a subscription on a terminated receiver using "+caseName, func() {
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				receiver.Start()
				receiver.Terminate(60 * time.Second)

				err = subscribe(resource.TopicSubscriptionOf("some topic"), receiver)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			})
		}

		removeSubscriptionCases := map[string](func(resource.Subscription, solace.DirectMessageReceiver) error){
			"synchronous unsubscribe": func(ts resource.Subscription, dmr solace.DirectMessageReceiver) error {
				return dmr.RemoveSubscription(ts)
			},
			"asynchronous unsubscribe": func(ts resource.Subscription, dmr solace.DirectMessageReceiver) error {
				type ret struct {
					subscription resource.Subscription
					operation    solace.SubscriptionOperation
					errOrNil     error
				}
				complete := make(chan ret)
				err := dmr.RemoveSubscriptionAsync(ts, func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
					complete <- ret{subscription, operation, errOrNil}
				})
				if err != nil {
					return err
				}
				val := <-complete
				Expect(val.subscription.GetName()).To(Equal(ts.GetName()))
				Expect(val.operation).To(Equal(solace.SubscriptionRemoved))
				return val.errOrNil
			},
		}
		for caseName, caseFunction := range removeSubscriptionCases {
			unsubscribe := caseFunction
			It("should be able to remove subscriptions using "+caseName+" while running", func() {
				topicString := "try-me"
				receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
				Expect(err).ToNot(HaveOccurred())

				msgChannel := make(chan message.InboundMessage)
				messageHandler := func(msg message.InboundMessage) {
					msgChannel <- msg
				}
				receiver.ReceiveAsync(messageHandler)

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())

				msg, err := messageBuilder.BuildWithStringPayload("hello world")
				Expect(err).ToNot(HaveOccurred())

				topic := resource.TopicOf(topicString)
				publisher.Publish(msg, topic)

				// check that the message was sent via semp
				clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(messagingService.GetApplicationID()), nil)
				Expect(err).To(BeNil())
				Expect(clientResponse.Data.DataTxMsgCount).To(Equal(int64(1)))

				select {
				case receivedMessage := <-msgChannel:
					Expect(receivedMessage).ToNot(BeNil())
				case <-time.After(gracePeriod):
					Fail("Timed out waiting to receive message")
				}
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(1)))

				err = unsubscribe(resource.TopicSubscriptionOf(topicString), receiver)
				Expect(err).ToNot(HaveOccurred())

				publisher.Publish(msg, topic)

				select {
				case <-msgChannel:
					Fail("did not expect to receive message")
				case <-time.After(100 * time.Millisecond):
					// success
				}

				clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(messagingService.GetApplicationID()), nil)
				Expect(err).To(BeNil())
				Expect(clientResponse.Data.DataTxMsgCount).To(Equal(int64(1)))
				Expect(clientResponse.Data.DataTxMsgCount).To(Equal(int64(1)))
			})
			It("should fail to remove subscription using "+caseName, func() {
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())

				err = unsubscribe(resource.TopicSubscriptionOf(invalidTopicString), receiver)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.NativeError{}))
			})
			It("should fail to remove invalid subscription type using "+caseName, func() {
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())

				err = unsubscribe(&myCustomSubscription{}, receiver)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
			})
			It("should fail to remove a subscription on an unstarted receiver using "+caseName, func() {
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				err = unsubscribe(resource.TopicSubscriptionOf("some topic"), receiver)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			})
			It("should fail to remove a subscription on a terminated receiver using "+caseName, func() {
				receiver, err := builder.Build()
				Expect(err).ToNot(HaveOccurred())

				receiver.Start()
				receiver.Terminate(60 * time.Second)

				err = unsubscribe(resource.TopicSubscriptionOf("some topic"), receiver)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			})
		}

		It("should fail to add a subscription on start when provided via the builder", func() {
			receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(invalidTopicString)).Build()
			Expect(err).ToNot(HaveOccurred())

			err = receiver.Start()
			Expect(err).To(HaveOccurred())
			var nativeError *solace.NativeError
			Expect(errors.As(err, &nativeError)).To(BeTrue())
			Expect(nativeError.SubCode()).To(Equal(subcode.InvalidTopicSyntax))
		})

		It("should clean up orphaned subscriptions on failed start when provided via the builder", func() {
			validTopic := "hello/world"
			receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(validTopic),
				resource.TopicSubscriptionOf(invalidTopicString)).Build()
			Expect(err).ToNot(HaveOccurred())

			err = receiver.Start()
			Expect(err).To(HaveOccurred())
			helpers.ValidateNativeError(err, subcode.InvalidTopicSyntax)

			msg, err := messagingService.MessageBuilder().BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())
			publisher.Publish(msg, resource.TopicOf(validTopic))

			// give message a chance to be delivered
			<-time.After(100 * time.Millisecond)

			client := helpers.GetClient(messagingService)
			Expect(client.DataTxMsgCount).To(Equal(int64(0)))
		})

		Context("with an ACL deny exception", func() {
			subscribeOperations := []TableEntry{
				Entry("Synchronous Subscribe", func(receiver solace.DirectMessageReceiver,
					topic *resource.TopicSubscription) error {
					return receiver.AddSubscription(topic)
				}),
				Entry("Asynchronous Subscribe", func(receiver solace.DirectMessageReceiver,
					topic *resource.TopicSubscription) (err error) {
					resultChan, subscriptionChangeListener := helpers.AsyncSubscribeHandler(topic, solace.SubscriptionAdded)
					Expect(receiver.AddSubscriptionAsync(topic, subscriptionChangeListener)).ToNot(HaveOccurred())
					Eventually(resultChan).Should(Receive(&err))
					return err
				}),
			}

			Context("with a denied subscription", func() {
				const deniedTopic = "direct-receiver-acl-deny"
				BeforeEach(func() {
					helpers.AddSubscriptionTopicException(deniedTopic)
				})

				AfterEach(func() {
					helpers.RemoveSubscriptionTopicException(deniedTopic)
				})

				It("fails to subscribe when starting the receiver", func() {
					receiver, err := messagingService.CreateDirectMessageReceiverBuilder().
						WithSubscriptions(resource.TopicSubscriptionOf(deniedTopic)).
						Build()
					Expect(err).ToNot(HaveOccurred())
					err = receiver.Start()
					helpers.ValidateNativeError(err, subcode.SubscriptionAclDenied)
				})

				DescribeTable("runtime rubscription activities",
					func(subscribeFunc func(receiver solace.DirectMessageReceiver,
						topicSubscription *resource.TopicSubscription) error) {
						receiver, err := messagingService.CreateDirectMessageReceiverBuilder().
							Build()
						Expect(err).ToNot(HaveOccurred())
						err = receiver.Start()
						Expect(err).ToNot(HaveOccurred())
						err = subscribeFunc(receiver, resource.TopicSubscriptionOf(deniedTopic))
						helpers.ValidateNativeError(err, subcode.SubscriptionAclDenied)
						Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
					},
					subscribeOperations,
				)
			})

			Context("with a denied share name", func() {
				const deniedShareName = "directShareAclDenied"
				const deniedTopic = "any-arbitrary-topic"
				BeforeEach(func() {
					helpers.AddSharedSubscriptionTopicException(deniedShareName)
				})

				AfterEach(func() {
					helpers.RemoveSharedSubscriptionTopicException(deniedShareName)
				})

				It("fails to subscribe when starting the receiver", func() {
					receiver, err := messagingService.CreateDirectMessageReceiverBuilder().
						WithSubscriptions(resource.TopicSubscriptionOf(deniedTopic)).
						BuildWithShareName(resource.ShareNameOf(deniedShareName))
					Expect(err).ToNot(HaveOccurred())
					err = receiver.Start()
					helpers.ValidateNativeError(err, subcode.SubscriptionAclDenied)
				})

				DescribeTable("runtime rubscription activities",
					func(subscribeFunc func(receiver solace.DirectMessageReceiver,
						topicSubscription *resource.TopicSubscription) error) {
						receiver, err := messagingService.CreateDirectMessageReceiverBuilder().
							BuildWithShareName(resource.ShareNameOf(deniedShareName))
						Expect(err).ToNot(HaveOccurred())
						err = receiver.Start()
						Expect(err).ToNot(HaveOccurred())
						err = subscribeFunc(receiver, resource.TopicSubscriptionOf(deniedTopic))
						helpers.ValidateNativeError(err, subcode.SubscriptionAclDenied)
						Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
					},
					subscribeOperations,
				)
			})

		})

		It("should terminate with undelivered messages", func() {
			undeliveredCount := 10
			msgString := "hello message %d"
			topicString := "try-me"

			receiver, err := builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
			Expect(err).ToNot(HaveOccurred())
			defer func() {
				receiver.Terminate(gracePeriod)
			}()
			err = receiver.Start()
			Expect(err).ToNot(HaveOccurred())

			blocker := make(chan struct{})
			blockerClosed := false
			// we should clean up so that more tests can run
			defer func() {
				if !blockerClosed {
					close(blocker)
				}
			}()

			messageHandler := func(msg message.InboundMessage) {
				// we want to wait forever until the blocker is closed
				<-blocker
			}
			receiver.ReceiveAsync(messageHandler)

			// send one message to block the receiver
			firstMessage := "first message"
			firstMsg, err := messageBuilder.BuildWithStringPayload(firstMessage)
			Expect(err).ToNot(HaveOccurred())
			publisher.Publish(firstMsg, resource.TopicOf(topicString))

			for i := 0; i < undeliveredCount; i++ {
				message, err := messageBuilder.BuildWithStringPayload(fmt.Sprintf(msgString, i))
				Expect(err).ToNot(HaveOccurred())
				publisher.Publish(message, resource.TopicOf(topicString))
			}

			// wait for stats to indicate that the messages have arrived
			Eventually(func() uint64 {
				return messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)
			}).Should(BeNumerically(">=", undeliveredCount))

			terminateChannel := receiver.TerminateAsync(100 * time.Millisecond)

			select {
			case <-terminateChannel:
				Fail("did not expect receiver to terminate with blocking receive callback")
			case <-time.After(1 * time.Second):
				// we have expired the grace period
			}

			blockerClosed = true
			close(blocker)
			select {
			case err = <-terminateChannel:
				// success
			case <-time.After(100 * time.Millisecond):
				Fail("timed out waiting for terminate to complete")
			}
			Expect(err).To(HaveOccurred())
			Expect(err).To(BeAssignableToTypeOf(&solace.IncompleteMessageDeliveryError{}))

			Expect(messagingService.Metrics().GetValue(metrics.ReceivedMessagesTerminationDiscarded)).To(Equal(uint64(undeliveredCount)))
		})

		Context("with a started and subscribed receiver", func() {
			const topicString = "try-me"

			var receiver solace.DirectMessageReceiver

			BeforeEach(func() {
				var err error
				receiver, err = builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString)).Build()
				Expect(err).ToNot(HaveOccurred())
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				if !receiver.IsTerminated() {
					receiver.Terminate(1 * time.Second)
				}
			})

			It("should receive with out of scope receiver callback", func() {
				received := make(chan struct{})
				outOfScopeHandler := func() {
					handler := func(inboundMessage message.InboundMessage) {
						close(received)
					}
					receiver.ReceiveAsync(handler)
				}
				outOfScopeHandler()
				for i := 0; i < 10; i++ {
					runtime.GC()
				}
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(received).Should(BeClosed())
			})

			It("should not panic with nil receiver callback", func() {
				err := receiver.ReceiveAsync(nil)
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)
				}).Should(BeEquivalentTo(1))
			})

			It("should not panic with no receiver callback", func() {
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)
				}).Should(BeEquivalentTo(1))
			})

			It("should be able to override async callback", func() {
				r1 := make(chan message.InboundMessage, 2)
				r2 := make(chan message.InboundMessage, 2)
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					r1 <- inboundMessage
				})
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(r1).Should(Receive())
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					r2 <- inboundMessage
				})
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(r2).Should(Receive())
				Consistently(r1).ShouldNot(Receive())
			})

			It("should be able to receive messages synchronously on multiple threads", func() {
				r1 := make(chan message.InboundMessage, 2)
				r2 := make(chan message.InboundMessage, 2)
				go func() {
					defer GinkgoRecover()
					msg, err := receiver.ReceiveMessage(-1)
					Expect(err).ToNot(HaveOccurred())
					r1 <- msg
				}()
				go func() {
					defer GinkgoRecover()
					msg, err := receiver.ReceiveMessage(-1)
					Expect(err).ToNot(HaveOccurred())
					r2 <- msg
				}()
				helpers.PublishOneMessage(messagingService, topicString)
				helpers.PublishOneMessage(messagingService, topicString)
				// We don't know who will win the race so we publish first before checking
				Eventually(r1).Should(Receive())
				Eventually(r2).Should(Receive())
			})

			It("should be able to receive messages synchronously when an asynchronous callback is set and receive each message only once", func() {
				// make sure that we can receive all published messages and never receive twice with receive sync and receive async combineds
				// Note that this is not an officially supported use case, however we should make sure that it at minimum does not crash :)
				terminateLoop := make(chan struct{})
				defer close(terminateLoop)
				r1 := make(chan message.InboundMessage, 2)
				r2 := make(chan message.InboundMessage, 2)
				go func() {
					defer GinkgoRecover()
					for {
						// we need to check before calling ReceiveMessage AND after calling ReceiveMessage
						select {
						case <-terminateLoop:
							return
						default:
						}
						msg, err := receiver.ReceiveMessage(-1)
						select {
						case <-terminateLoop:
							return
						default:
						}
						Expect(err).ToNot(HaveOccurred())
						r1 <- msg
					}
				}()
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					r2 <- inboundMessage
				})
				nMessages := 10
				for i := 0; i < nMessages; i++ {
					helpers.PublishOneMessage(messagingService, topicString)
				}
				for i := 0; i < nMessages; i++ {
					select {
					case <-r1:
					case <-r2:
					case <-time.After(1 * time.Second):
						Fail("timed out waiting for messages to be received in either channel")
					}
				}
				Consistently(r1).ShouldNot(Receive())
				Consistently(r2).ShouldNot(Receive())
			})

			It("should be able to receive messages asynchronously after receiveing messages synchronously", func() {
				r1 := make(chan message.InboundMessage, 2)
				r2 := make(chan message.InboundMessage, 2)
				go func() {
					defer GinkgoRecover()
					msg, err := receiver.ReceiveMessage(-1)
					Expect(err).ToNot(HaveOccurred())
					r1 <- msg
				}()
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(r1).Should(Receive())
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					r2 <- inboundMessage
				})
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(r2).Should(Receive())
			})

			It("should receive all messages when a receive callback is specified later", func() {
				r1 := make(chan message.InboundMessage, 2)
				helpers.PublishOneMessage(messagingService, topicString)
				time.Sleep(1 * time.Second)
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					r1 <- inboundMessage
				})
				Eventually(r1).Should(Receive())
			})

			It("should terminate with undelivered messages without an async callback", func() {
				undeliveredCount := 10

				for i := 0; i < undeliveredCount; i++ {
					helpers.PublishOneMessage(messagingService, topicString)
				}

				// wait for stats to indicate that the messages have arrived
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)
				}).Should(BeNumerically(">=", undeliveredCount))

				terminateChannel := receiver.TerminateAsync(1 * time.Second)

				select {
				case <-terminateChannel:
					Fail("did not expect receiver to terminate before grace period")
				case <-time.After(500 * time.Millisecond):
					// we have expired the grace period
				}

				select {
				case err := <-terminateChannel:
					// success
					Expect(err).To(HaveOccurred())
					Expect(err).To(BeAssignableToTypeOf(&solace.IncompleteMessageDeliveryError{}))
				case <-time.After(1 * time.Second):
					Fail("timed out waiting for terminate to complete")
				}

				Expect(messagingService.Metrics().GetValue(metrics.ReceivedMessagesTerminationDiscarded)).To(Equal(uint64(undeliveredCount)))
			})

			It("should wait to termiante until all messages are processed with synchronous receive", func() {
				helpers.PublishOneMessage(messagingService, topicString)
				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 1)
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Consistently(terminateChannel).ShouldNot(Receive())
				msg, err := receiver.ReceiveMessage(-1)
				Expect(err).ToNot(HaveOccurred())
				Expect(msg).ToNot(BeNil())
				Eventually(terminateChannel).Should(Receive(BeNil()))
			})

			It("should wait to termiante until all messages are processed with async receive", func() {
				blocker := make(chan struct{})
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					<-blocker
				})
				helpers.PublishOneMessage(messagingService, topicString)
				helpers.PublishOneMessage(messagingService, topicString)
				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 2)
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Consistently(terminateChannel).ShouldNot(Receive())
				close(blocker)
				Eventually(terminateChannel).Should(Receive(BeNil()))
			})

			It("should reject asynchronous callback registration while terminating", func() {
				helpers.PublishOneMessage(messagingService, topicString)
				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 1)
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Consistently(terminateChannel).ShouldNot(Receive())

				err := receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {})
				helpers.ValidateError(err, &solace.IllegalStateError{})

				msg, err := receiver.ReceiveMessage(-1)
				Expect(err).ToNot(HaveOccurred())
				Expect(msg).ToNot(BeNil())
				Eventually(terminateChannel).Should(Receive(BeNil()))
			})

			It("should wait to terminate when all messages are processed but asynchronous callback is blocked", func() {
				blocker := make(chan struct{})
				blocking := make(chan struct{})
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					close(blocking)
					<-blocker
				})
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(blocking).Should(BeClosed())
				helpers.PublishOneMessage(messagingService, topicString)
				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 2)
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Consistently(terminateChannel).ShouldNot(Receive())
				msg, err := receiver.ReceiveMessage(500 * time.Millisecond)
				Expect(err).ToNot(HaveOccurred())
				Expect(msg).ToNot(BeNil())
				Consistently(terminateChannel).ShouldNot(Receive())
				close(blocker)
				Eventually(terminateChannel).Should(Receive(BeNil()))
			})

			It("should time out waiting for a message", func() {
				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					msg, err := receiver.ReceiveMessage(1 * time.Second)
					Expect(msg).To(BeNil())
					helpers.ValidateError(err, &solace.TimeoutError{})
					close(done)
				}()
				// We want to make sure that it does not close for at least 500ms
				Consistently(done, 500*time.Millisecond).ShouldNot(BeClosed())
				// we want it to be closed after 1 second though
				Eventually(done).Should(BeClosed())
			})

			It("should be able to continue message delivery when a receive async panics", func() {
				msgReceived := make(chan message.InboundMessage)
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					msgReceived <- inboundMessage
					panic("everybody stay calm, this should still pass")
				})
				const payloadOne = "one"
				const payloadTwo = "two"
				helpers.PublishOneMessage(messagingService, topicString, payloadOne)
				// we should receive a message
				var msg message.InboundMessage
				Eventually(msgReceived).Should(Receive(&msg))
				payload, ok := msg.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal(payloadOne))
				helpers.PublishOneMessage(messagingService, topicString, payloadTwo)
				// we should continue to receive messages
				Eventually(msgReceived).Should(Receive(&msg))
				payload, ok = msg.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal(payloadTwo))
				// we should be able to terminate
				Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("should wait indefinitely for a message", func() {
				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					msg, err := receiver.ReceiveMessage(-1)
					Expect(err).ToNot(HaveOccurred())
					Expect(msg).ToNot(BeNil())
					close(done)
				}()
				// 1 second is the same as forever right?
				Consistently(done, 1*time.Second).ShouldNot(BeClosed())
				helpers.PublishOneMessage(messagingService, topicString)
				Eventually(done).Should(BeClosed())
			})

			It("should be interrupted while waiting for a message", func() {
				done := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					msg, err := receiver.ReceiveMessage(-1)
					Expect(msg).To(BeNil())
					helpers.ValidateError(err, &solace.IllegalStateError{}, "terminated")
					close(done)
				}()
				Consistently(done, 1*time.Second).ShouldNot(BeClosed())
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Eventually(terminateChannel).Should(Receive(BeNil()))
				Eventually(done).Should(BeClosed())
			})
		})

		Describe("backpressure tests", func() {
			// Begin backpressure test setup
			const bufferSize = 10
			const msgStringTemplate = "hello message %d"
			const topicString = "receiver-backpressure"
			const backpressureTestBufferCapacity = 10
			// dropOldestConfigurationFunctions := map[string]func(builder solace.DirectMessageReceiverBuilder){
			// 	"builder function": func(builder solace.DirectMessageReceiverBuilder) {
			// 		builder.OnBackPressureDropOldest(backpressureTestBufferCapacity)
			// 	},
			// 	"configuration key": func(builder solace.DirectMessageReceiverBuilder) {
			// 		builder.FromConfigurationProvider(config.ReceiverPropertyMap{
			// 			config.ReceiverPropertyDirectBackPressureBufferCapacity: backpressureTestBufferCapacity,
			// 			config.ReceiverPropertyDirectBackPressureStrategy:       config.ReceiverBackPressureStrategyDropOldest,
			// 		})
			// 	},
			// }
			dropLatestConfigurationFunctions := map[string]func(builder solace.DirectMessageReceiverBuilder){
				"builder function": func(builder solace.DirectMessageReceiverBuilder) {
					builder.OnBackPressureDropLatest(backpressureTestBufferCapacity)
				},
				"configuration key": func(builder solace.DirectMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyDirectBackPressureBufferCapacity: backpressureTestBufferCapacity,
						config.ReceiverPropertyDirectBackPressureStrategy:       config.ReceiverBackPressureStrategyDropLatest,
					})
				},
			}
			type backpressureTest struct {
				configurations                                map[string]func(builder solace.DirectMessageReceiverBuilder)
				firstMessageHasDiscard, lastMessageHasDiscard bool
			}
			backpressureTests := map[string]*backpressureTest{
				// Disabling this test for now as there is a lot of instability around it
				// "drop oldest": {dropOldestConfigurationFunctions, true, false},
				"drop latest": {dropLatestConfigurationFunctions, false, true},
			}
			for backpressureTestString, backpressureTestCases := range backpressureTests {
				firstMessageHasDiscard := backpressureTestCases.firstMessageHasDiscard
				lastMessageHasDiscard := backpressureTestCases.lastMessageHasDiscard
				for strategyString, builderFuncRef := range backpressureTestCases.configurations {
					configureBackpressure := builderFuncRef
					Context("with backpressure "+backpressureTestString+" using "+strategyString, func() {
						var receiver solace.DirectMessageReceiver

						BeforeEach(func() {
							builder.WithSubscriptions(resource.TopicSubscriptionOf(topicString))
							configureBackpressure(builder)
							var err error
							receiver, err = builder.Build()
							Expect(err).ToNot(HaveOccurred())
							err = receiver.Start()
							Expect(err).ToNot(HaveOccurred())
						})

						AfterEach(func() {
							if !receiver.IsTerminated() {
								err := receiver.Terminate(10 * time.Second)
								Expect(err).ToNot(HaveOccurred())
							}
						})

						It("should handle backpressure using receive callback", func() {
							blocker := make(chan struct{})
							blockerClosed := false
							// we should clean up so that more tests can run
							defer func() {
								if !blockerClosed {
									close(blocker)
								}
							}()

							msgChannel := make(chan message.InboundMessage, bufferSize+1)
							messageHandler := func(msg message.InboundMessage) {
								// we want to wait forever until the blocker is closed
								<-blocker
								msgChannel <- msg
							}
							receiver.ReceiveAsync(messageHandler)

							// send one message to block the receiver
							firstMessage := "first message"
							firstMsg, err := messageBuilder.BuildWithStringPayload(firstMessage)
							Expect(err).ToNot(HaveOccurred())
							publisher.Publish(firstMsg, resource.TopicOf(topicString))

							for i := 0; i <= bufferSize; i++ {
								message, err := messageBuilder.BuildWithStringPayload(fmt.Sprintf(msgStringTemplate, i))
								Expect(err).ToNot(HaveOccurred())
								publisher.Publish(message, resource.TopicOf(topicString))
							}

							// wait for stats to indicate that the messages have arrived
							helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, bufferSize+2)

							// Just make sure the receiver has processed every message
							time.Sleep(1 * time.Second)
							blockerClosed = true
							close(blocker)
							// now messages should be popped off and added to the msgChannel
							var msg message.InboundMessage
							select {
							case msg = <-msgChannel:
							case <-time.After(10 * time.Millisecond):
								Fail("message channel did not have any messages!")
							}
							str, ok := msg.GetPayloadAsString()
							Expect(ok).To(BeTrue())
							Expect(str).To(Equal(firstMessage))

							receivedDiscard := false
							for i := 0; i < bufferSize; i++ {
								select {
								case msg = <-msgChannel:
									content, ok := msg.GetPayloadAsString()
									Expect(ok).To(BeTrue())
									index := i
									if firstMessageHasDiscard {
										index++
									}
									Expect(content).To(Equal(fmt.Sprintf(msgStringTemplate, index)))
									discardNotification := msg.GetMessageDiscardNotification()
									Expect(discardNotification).ToNot(BeNil())
									if !receivedDiscard && firstMessageHasDiscard && discardNotification.HasInternalDiscardIndication() {
										receivedDiscard = true
									} else {
										Expect(discardNotification.HasInternalDiscardIndication()).To(BeFalse())
									}
								case <-time.After(10 * time.Millisecond):
									Fail("timed out waiting for message to be added to channel")
								}
							}
							Expect(receivedDiscard).To(Equal(firstMessageHasDiscard))

							lastMessage := "last message"
							lastMsg, err := messageBuilder.BuildWithStringPayload(lastMessage)
							Expect(err).ToNot(HaveOccurred())
							publisher.Publish(lastMsg, resource.TopicOf(topicString))
							var received message.InboundMessage
							Eventually(msgChannel).Should(Receive(&received))
							content, ok := received.GetPayloadAsString()
							Expect(ok).To(BeTrue())
							Expect(content).To(Equal(lastMessage))
							discardNotification := received.GetMessageDiscardNotification()
							Expect(discardNotification).ToNot(BeNil())
							Expect(discardNotification.HasInternalDiscardIndication()).To(Equal(lastMessageHasDiscard))

							helpers.ValidateMetric(messagingService, metrics.InternalDiscardNotifications, 1)

							select {
							case msg = <-msgChannel:
								str, _ := msg.GetPayloadAsString()
								Fail("did not expect to get another message: " + str)
							case <-time.After(100 * time.Millisecond):
								// success
							}
						})

						It("should handle backpressure using synchronous receive", func() {
							// Fill the buffer + 1 extra
							for i := 0; i <= bufferSize; i++ {
								message, err := messageBuilder.BuildWithStringPayload(fmt.Sprintf(msgStringTemplate, i))
								Expect(err).ToNot(HaveOccurred())
								publisher.Publish(message, resource.TopicOf(topicString))
							}

							// wait for stats to indicate that the messages have arrived
							helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, bufferSize+1)

							// Just make sure the receiver has processed every message
							time.Sleep(50 * time.Microsecond)
							// now messages should be popped off and added to the msgChannel
							for i := 0; i < bufferSize; i++ {
								msg, err := receiver.ReceiveMessage(1 * time.Second)
								Expect(err).ToNot(HaveOccurred())
								content, ok := msg.GetPayloadAsString()
								Expect(ok).To(BeTrue())
								index := i
								if firstMessageHasDiscard {
									index++
								}
								Expect(content).To(Equal(fmt.Sprintf(msgStringTemplate, index)))
								discardNotification := msg.GetMessageDiscardNotification()
								Expect(discardNotification).ToNot(BeNil())
								if i == 0 && firstMessageHasDiscard {
									Expect(discardNotification.HasInternalDiscardIndication()).To(BeTrue())
								} else {
									Expect(discardNotification.HasInternalDiscardIndication()).To(BeFalse())
								}
							}

							// make sure we can still receive new messages
							lastMessage := "last message"
							lastMsg, err := messageBuilder.BuildWithStringPayload(lastMessage)
							Expect(err).ToNot(HaveOccurred())
							publisher.Publish(lastMsg, resource.TopicOf(topicString))
							var received message.InboundMessage
							received, err = receiver.ReceiveMessage(1 * time.Second)
							Expect(err).ToNot(HaveOccurred())
							content, ok := received.GetPayloadAsString()
							Expect(ok).To(BeTrue())
							Expect(content).To(Equal(lastMessage))
							discardNotification := received.GetMessageDiscardNotification()
							Expect(discardNotification).ToNot(BeNil())
							Expect(discardNotification.HasInternalDiscardIndication()).To(Equal(lastMessageHasDiscard))

							helpers.ValidateMetric(messagingService, metrics.InternalDiscardNotifications, 1)

							// make sure we don't receive any more messages
							_, err = receiver.ReceiveMessage(1 * time.Second)
							helpers.ValidateError(err, &solace.TimeoutError{})
						})

					})
				}
			}
		})

	})

	Describe("multiple receivers", func() {
		var publisher solace.DirectMessagePublisher
		var r1 solace.DirectMessageReceiver
		var r2 solace.DirectMessageReceiver

		BeforeEach(func() {
			var err error
			err = messagingService.Connect()
			Expect(err).ToNot(HaveOccurred())
			publisher, err = messagingService.CreateDirectMessagePublisherBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			err = publisher.Start()
			Expect(err).ToNot(HaveOccurred())

			r1, err = messagingService.CreateDirectMessageReceiverBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			r2, err = messagingService.CreateDirectMessageReceiverBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			err = r1.Start()
			Expect(err).ToNot(HaveOccurred())
			err = r2.Start()
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			err := publisher.Terminate(30 * time.Second)
			Expect(err).ToNot(HaveOccurred())
			err = r1.Terminate(30 * time.Second)
			Expect(err).ToNot(HaveOccurred())
			err = r2.Terminate(30 * time.Second)
			Expect(err).ToNot(HaveOccurred())
		})

		It("receives messages on only one receiver", func() {
			topic := "hello/world"
			r1Rcv := make(chan message.InboundMessage)
			r2Rcv := make(chan message.InboundMessage)
			err := r1.AddSubscription(resource.TopicSubscriptionOf(topic))
			Expect(err).ToNot(HaveOccurred())
			// do not add a subscription for r2
			r1.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				r1Rcv <- inboundMessage
			})
			r2.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				r2Rcv <- inboundMessage
			})
			msg, err := messagingService.MessageBuilder().BuildWithStringPayload("goodbye world")
			Expect(err).ToNot(HaveOccurred())
			err = publisher.Publish(msg, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			select {
			case <-r1Rcv:
				// success
			case <-time.After(100 * time.Millisecond):
				Fail("timed out waiting for receiver to receive message")
			}

			select {
			case <-r2Rcv:
				Fail("did not expect second receiver to receive message")
			case <-time.After(100 * time.Millisecond):
				// success
			}
		})

		It("receives messages on both receivers", func() {
			topic := "hello/world"
			r1Rcv := make(chan message.InboundMessage)
			r2Rcv := make(chan message.InboundMessage)
			err := r1.AddSubscription(resource.TopicSubscriptionOf(topic))
			Expect(err).ToNot(HaveOccurred())
			err = r2.AddSubscription(resource.TopicSubscriptionOf(topic))
			Expect(err).ToNot(HaveOccurred())
			// do not add a subscription for r2
			r1.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				r1Rcv <- inboundMessage
			})
			r2.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				r2Rcv <- inboundMessage
			})
			msg, err := messagingService.MessageBuilder().BuildWithStringPayload("goodbye world")
			Expect(err).ToNot(HaveOccurred())
			err = publisher.Publish(msg, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			var msg1, msg2 message.InboundMessage

			select {
			case msg1 = <-r1Rcv:
				// success
			case <-time.After(100 * time.Millisecond):
				Fail("timed out waiting for receiver to receive message")
			}

			select {
			case msg2 = <-r2Rcv:
				// success
			case <-time.After(100 * time.Millisecond):
				Fail("timed out waiting for second receiver to receive message")
			}
			Expect(msg1).ToNot(Equal(msg2))
		})
	})
	Describe("multiple messaging services", func() {
		var messagingService2 solace.MessagingService
		// var messageBuilder2 message.OutboundMessageBuilder
		var p1 solace.DirectMessagePublisher
		//		var p2 solace.DirectMessagePublisher
		var r1 solace.DirectMessageReceiver
		var r2 solace.DirectMessageReceiver

		BeforeEach(func() {
			builder := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration())
			var err error
			messagingService2, err = builder.Build()
			Expect(err).To(BeNil())

			err = messagingService.Connect()
			Expect(err).ToNot(HaveOccurred())
			err = messagingService2.Connect()
			Expect(err).To(BeNil())

			// messageBuilder2 = messagingService.MessageBuilder()

			p1, err = messagingService.CreateDirectMessagePublisherBuilder().Build()
			Expect(err).To(BeNil())
			// p2, err = messagingService2.CreateDirectMessagePublisherBuilder().Build()
			// Expect(err).To(BeNil())

			r1, err = messagingService.CreateDirectMessageReceiverBuilder().Build()
			Expect(err).To(BeNil())
			r2, err = messagingService2.CreateDirectMessageReceiverBuilder().Build()
			Expect(err).To(BeNil())

			err = r1.Start()
			Expect(err).To(BeNil())
			err = r2.Start()
			Expect(err).To(BeNil())
			err = p1.Start()
			Expect(err).To(BeNil())

		})

		AfterEach(func() {
			r1.Terminate(1 * time.Second)
			r2.Terminate(1 * time.Second)
			p1.Terminate(1 * time.Second)
			messagingService2.Disconnect()
		})

		It("receives messages on both receivers with separate messaging services", func() {
			topic := "hello world"
			topicSubscription := resource.TopicSubscriptionOf(topic)
			var err error
			err = r1.AddSubscription(topicSubscription)
			Expect(err).ToNot(HaveOccurred())
			err = r2.AddSubscription(topicSubscription)
			Expect(err).ToNot(HaveOccurred())

			r1Rx := make(chan message.InboundMessage)
			r2Rx := make(chan message.InboundMessage)

			r1.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				r1Rx <- inboundMessage
			})
			r2.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				r2Rx <- inboundMessage
			})

			msg, err := messagingService.MessageBuilder().BuildWithStringPayload("goodbye world")
			Expect(err).ToNot(HaveOccurred())
			err = p1.Publish(msg, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			select {
			case <-r1Rx:
				// success
			case <-time.After(5 * time.Second):
				Fail("did not receive message on first messaging service")
			}

			select {
			case <-r2Rx:
				// success
			case <-time.After(5 * time.Second):
				Fail("did not receive message on second messaging service")
			}
		})

		It("receives messages on only subscribed receivers", func() {
			topic := "hello world"
			topicSubscription := resource.TopicSubscriptionOf(topic)
			var err error
			err = r2.AddSubscription(topicSubscription)
			Expect(err).ToNot(HaveOccurred())

			r1Rx := make(chan message.InboundMessage)
			r2Rx := make(chan message.InboundMessage)

			r1.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				r1Rx <- inboundMessage
			})
			r2.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				r2Rx <- inboundMessage
			})

			msg, err := messagingService.MessageBuilder().BuildWithStringPayload("goodbye world")
			Expect(err).ToNot(HaveOccurred())
			err = p1.Publish(msg, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			select {
			case <-r1Rx:
				Fail("did not expect to receive message on first messaging service")
			case <-time.After(100 * time.Millisecond):
				// success
			}

			select {
			case <-r2Rx:
				// success
			case <-time.After(5 * time.Second):
				Fail("did not receive message on second messaging service")
			}
		})
	})

	Describe("Shared Subscriptions", func() {

		const shareName = "myshare"
		const topic = "my/complex/topic"
		const invalidShareName = "thisIsInvalid*"

		It("can print a share name to a string", func() {
			sn := resource.ShareNameOf(shareName)
			Expect(sn.String()).To(ContainSubstring(shareName))
		})

		It("should fail to build with an invalid share name", func() {
			receiver, err := messagingService.CreateDirectMessageReceiverBuilder().BuildWithShareName(resource.ShareNameOf(invalidShareName))
			helpers.ValidateError(err, &solace.IllegalArgumentError{}, "*")
			Expect(receiver).To(BeNil())
		})

		It("should fail to build with an empty share name", func() {
			receiver, err := messagingService.CreateDirectMessageReceiverBuilder().BuildWithShareName(resource.ShareNameOf(""))
			helpers.ValidateError(err, &solace.IllegalArgumentError{}, "empty")
			Expect(receiver).To(BeNil())
		})

		It("should successfully build with a nil share name", func() {
			receiver, err := messagingService.CreateDirectMessageReceiverBuilder().BuildWithShareName(nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(receiver).ToNot(BeNil())
		})

		Context("with a started messaging service", func() {
			getSubscriptions := func() []string {
				subscriptionsFromSEMP := helpers.GetClientSubscriptions(messagingService)
				subscriptions := []string{}
				for _, subscription := range subscriptionsFromSEMP {
					if !strings.HasPrefix(subscription.SubscriptionTopic, "#P2P") {
						subscriptions = append(subscriptions, subscription.SubscriptionTopic)
					}
				}
				return subscriptions
			}

			BeforeEach(func() {
				helpers.ConnectMessagingService(messagingService)
			})
			AfterEach(func() {
				helpers.DisconnectMessagingService(messagingService)
			})
			It("should apply the correct shared subscription when given one on build", func() {
				receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(
					resource.TopicSubscriptionOf(topic),
				).BuildWithShareName(
					resource.ShareNameOf(shareName),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())
				subscriptions := getSubscriptions()
				Expect(len(subscriptions)).To(Equal(1))
				Expect(subscriptions[0]).To(Equal("#share/" + shareName + "/" + topic))
			})
			It("should apply the correct shared subscription when given one with subscribe", func() {
				receiver, err := messagingService.CreateDirectMessageReceiverBuilder().BuildWithShareName(
					resource.ShareNameOf(shareName),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())
				Expect(receiver.AddSubscription(resource.TopicSubscriptionOf(topic))).ToNot(HaveOccurred())
				subscriptions := getSubscriptions()
				Expect(len(subscriptions)).To(Equal(1))
				Expect(subscriptions[0]).To(Equal("#share/" + shareName + "/" + topic))
			})
			It("should apply the correct shared subscription when given one with subscribe async", func() {
				receiver, err := messagingService.CreateDirectMessageReceiverBuilder().BuildWithShareName(
					resource.ShareNameOf(shareName),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())

				subscribeChan := make(chan struct{})
				Expect(receiver.AddSubscriptionAsync(resource.TopicSubscriptionOf(topic),
					func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
						defer GinkgoRecover()
						Expect(subscription.GetName()).To(Equal(topic))
						Expect(operation).To(Equal(solace.SubscriptionAdded))
						Expect(errOrNil).ToNot(HaveOccurred())
						close(subscribeChan)
					})).ToNot(HaveOccurred())
				Eventually(subscribeChan).Should(BeClosed())
				subscriptions := getSubscriptions()
				Expect(len(subscriptions)).To(Equal(1))
				Expect(subscriptions[0]).To(Equal("#share/" + shareName + "/" + topic))
			})
			It("should remove the correct shared subscription when given one with unsubscribe", func() {
				receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(
					resource.TopicSubscriptionOf(topic),
				).BuildWithShareName(
					resource.ShareNameOf(shareName),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())
				subscriptions := getSubscriptions()
				Expect(len(subscriptions)).To(Equal(1))
				Expect(subscriptions[0]).To(Equal("#share/" + shareName + "/" + topic))
				Expect(receiver.RemoveSubscription(resource.TopicSubscriptionOf(topic))).ToNot(HaveOccurred())
				subscriptions = getSubscriptions()
				Expect(len(subscriptions)).To(Equal(0))
			})
			It("should remove the correct shared subscription when given one with unsubscribe async", func() {
				receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(
					resource.TopicSubscriptionOf(topic),
				).BuildWithShareName(
					resource.ShareNameOf(shareName),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())
				subscriptions := getSubscriptions()
				Expect(len(subscriptions)).To(Equal(1))
				Expect(subscriptions[0]).To(Equal("#share/" + shareName + "/" + topic))
				unsubscribeChan := make(chan struct{})
				Expect(receiver.RemoveSubscriptionAsync(resource.TopicSubscriptionOf(topic),
					func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
						defer GinkgoRecover()
						Expect(subscription.GetName()).To(Equal(topic))
						Expect(operation).To(Equal(solace.SubscriptionRemoved))
						Expect(errOrNil).ToNot(HaveOccurred())
						close(unsubscribeChan)
					})).ToNot(HaveOccurred())
				Eventually(unsubscribeChan).Should(BeClosed())
				subscriptions = getSubscriptions()
				Expect(len(subscriptions)).To(Equal(0))
			})
			It("should remove the correct shared subscription when the receiver is terminated", func() {
				receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(
					resource.TopicSubscriptionOf(topic),
				).BuildWithShareName(
					resource.ShareNameOf(shareName),
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())
				subscriptions := getSubscriptions()
				Expect(len(subscriptions)).To(Equal(1))
				Expect(subscriptions[0]).To(Equal("#share/" + shareName + "/" + topic))
				receiver.Terminate(10 * time.Second)
				subscriptions = getSubscriptions()
				Expect(len(subscriptions)).To(Equal(0))
			})
		})

		Context("with multiple messaging services", func() {
			const messagingServiceCount = 3
			const receiverCount = 3
			var messagingServices []solace.MessagingService
			BeforeEach(func() {
				helpers.ConnectMessagingService(messagingService)
				messagingServices = make([]solace.MessagingService, messagingServiceCount)
				builder := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration())
				for i := 0; i < messagingServiceCount; i++ {
					messagingService := helpers.BuildMessagingService(builder)
					helpers.ConnectMessagingService(messagingService)
					messagingServices[i] = messagingService
				}
			})
			AfterEach(func() {
				helpers.DisconnectMessagingService(messagingService)
				for i := 0; i < messagingServiceCount; i++ {
					helpers.DisconnectMessagingService(messagingServices[i])
				}
			})
			It("should receive only one message with multiple shares", func() {
				channelCount := messagingServiceCount * receiverCount
				receiverChannels := make([]chan message.InboundMessage, channelCount)
				receivers := make([][]solace.DirectMessageReceiver, messagingServiceCount)
				for i := 0; i < messagingServiceCount; i++ {
					receivers[i] = make([]solace.DirectMessageReceiver, receiverCount)
					for j := 0; j < receiverCount; j++ {
						receiver, err := messagingServices[i].CreateDirectMessageReceiverBuilder().WithSubscriptions(
							resource.TopicSubscriptionOf(topic),
						).BuildWithShareName(
							resource.ShareNameOf(shareName),
						)
						Expect(err).ToNot(HaveOccurred())
						receiverChan := make(chan message.InboundMessage, 1)
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							receiverChan <- inboundMessage
						})
						Expect(receiver.Start()).ToNot(HaveOccurred())
						defer func() {
							Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
						}()

						receivers[i][j] = receiver
						receiverChannels[3*i+j] = receiverChan
					}
				}
				// Publish message from an external messaging service
				payload := "Hello World"
				helpers.PublishOneMessage(messagingService, topic, payload)
				selectCases := make([]reflect.SelectCase, channelCount)
				for i := 0; i < channelCount; i++ {
					selectCases[i] = reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(receiverChannels[i]),
					}
				}
				selectComplete := make(chan reflect.Value, 1)
				go func() {
					defer GinkgoRecover()
					_, value, recvOK := reflect.Select(selectCases)
					Expect(recvOK).To(BeTrue())
					selectComplete <- value
				}()
				var val reflect.Value
				Eventually(selectComplete).Should(Receive(&val))
				iface := val.Interface()
				inboundMessage, ok := iface.(message.InboundMessage)
				Expect(ok).To(BeTrue())
				receivedPayload, ok := inboundMessage.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(receivedPayload).To(Equal(payload))
				for i := 0; i < channelCount; i++ {
					Consistently(receiverChannels[i]).ShouldNot(Receive())
				}
			})
		})

	})

})

type myCustomSubscription struct {
}

func (sub *myCustomSubscription) GetName() string {
	return "some string"
}

func (sub *myCustomSubscription) GetSubscriptionType() string {
	return "some type"
}
