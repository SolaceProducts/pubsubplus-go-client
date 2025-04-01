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
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const subscriptionString = "some-subscription"

var subscription = resource.TopicSubscriptionOf(subscriptionString)

var _ = Describe("RequestReplyReceiver", func() {

	var messagingService solace.MessagingService

	BeforeEach(func() {
		var err error
		messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration()).Build()
		Expect(err).To(BeNil())
	})

	Describe("Builder verification", func() {

		It("fails to build when given an invalid backpressure type", func() {
			receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().FromConfigurationProvider(config.ReceiverPropertyMap{
				config.ReceiverPropertyDirectBackPressureStrategy: "not a strategy",
			}).Build(subscription)
			Expect(err).To(HaveOccurred())
			Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
			Expect(receiver).To(BeNil())
		})

		It("fails to build when given an invalid subscription", func() {
			badSubscription := &myCustomRequestReplySubscription{}
			receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(badSubscription)
			Expect(err).To(HaveOccurred())
			Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
			Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("%T", badSubscription)))
			Expect(receiver).To(BeNil())
		})

		It("fails to start on unconnected messaging service", func() {
			receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(subscription)
			Expect(err).ToNot(HaveOccurred())

			err = receiver.Start()
			Expect(err).To(HaveOccurred())
			Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
		})

		It("fails to start on disconnected messaging service", func() {
			receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(subscription)
			Expect(err).ToNot(HaveOccurred())

			helpers.ConnectMessagingService(messagingService)
			helpers.DisconnectMessagingService(messagingService)

			err = receiver.Start()
			Expect(err).To(HaveOccurred())
			Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
		})

		It("fails to receive a message on an unconnected receiver", func() {
			receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(subscription)
			Expect(err).ToNot(HaveOccurred())

			msg, _, err := receiver.ReceiveMessage(-1) // blocking call to receive messages
			Expect(msg).To(BeNil())
			helpers.ValidateError(err, &solace.IllegalStateError{})
		})

		validConfigurations := map[string]config.ReceiverPropertyMap{
			"valid backpressure configuration": {
				config.ReceiverPropertyDirectBackPressureStrategy: config.ReceiverBackPressureStrategyDropLatest,
			},
			"valid backpressure configuration type": {
				config.ReceiverPropertyDirectBackPressureStrategy: config.ReceiverBackPressureStrategyDropOldest,
			},
		}
		for key, val := range validConfigurations {
			validConfiguration := val
			It("succeeds to build with "+key, func() {
				receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().FromConfigurationProvider(validConfiguration).Build(subscription)
				Expect(err).ToNot(HaveOccurred(), "Expected error to not have occurred")
				Expect(receiver).ToNot(BeNil())
				Expect(receiver.IsRunning()).To(BeFalse()) // running state should be false

				expected := &solace.IllegalArgumentError{}
				Expect(err).ToNot(BeAssignableToTypeOf(expected), fmt.Sprintf("Expected error of type %T to not be assignable of type %T", err, expected))
			})
		}

	})

	Context("with a messaging service that will be disconnected", func() {
		const topicString = "terminate/me"
		const messagesPublished = 3
		const publishTimeOut = 3 * time.Second
		var terminationChannel chan solace.TerminationEvent
		var receiver solace.RequestReplyMessageReceiver

		BeforeEach(func() {
			helpers.ConnectMessagingService(messagingService)
			var err error
			receiver, err = messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(resource.TopicSubscriptionOf(topicString))
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
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage, replier solace.Replier) {
					<-blocker
					msgsReceived <- inboundMessage

					payload, _ := inboundMessage.GetPayloadAsString()
					if replier != nil {
						err := replier.Reply(helpers.NewMessage(messagingService, "Reply for: "+payload))
						Expect(err).ToNot(BeNil()) // because the messaging service is diconnected
					}
				})

				publishReplyMsgChan := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, messagesPublished)
				Eventually(publishReplyMsgChan).Should(Receive()) // something was published

				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, messagesPublished)

				disconnectFunction(messagingService)

				Eventually(receiver.IsTerminated(), 10*time.Second).Should(BeTrue())
				Expect(receiver.IsRunning()).To(BeFalse()) // should not be in running state now

				// unblock the receiver callback after we are marked as terminated
				close(blocker)

				var expectedMsg message.InboundMessage
				Eventually(msgsReceived).Should(Receive(&expectedMsg))
				payload, ok := expectedMsg.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal("hello world 0")) // check that only first message was received

				Consistently(msgsReceived).ShouldNot(Receive()) // no more messages since the receiver has been terminated
				Eventually(terminationChannel).Should(Receive())
				helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, messagesPublished-1) // less the one successfully received
			})
			It("terminates the receiver using sync receive when disconnecting with "+testCase, func() {
				publishReplyMsgChan := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, messagesPublished)
				Eventually(publishReplyMsgChan).Should(Receive()) // something was published

				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, messagesPublished)

				discardOffset := 0
				disconnectFunction(messagingService)

				// Try and race with the unsolicited termination
				var err error = nil
				var replier solace.Replier = nil
				for err == nil {
					var racingMessage message.InboundMessage
					racingMessage, replier, err = receiver.ReceiveMessage(-1) // blocking receive call
					if racingMessage != nil {
						discardOffset++
					}
					if replier != nil {
						payload, _ := racingMessage.GetPayloadAsString()
						err = replier.Reply(helpers.NewMessage(messagingService, "Reply for: "+payload))
					}
				}

				Eventually(receiver.IsTerminated(), 10*time.Second).Should(BeTrue())
				Expect(receiver.IsRunning()).To(BeFalse()) // should not be in running state now
				Eventually(terminationChannel).Should(Receive())

				msg, replier, err := receiver.ReceiveMessage(-1)
				Expect(msg).To(BeNil())
				Expect(replier).To(BeNil()) // no more messages to reply to
				Expect(err).ToNot(BeNil())
				helpers.ValidateError(err, &solace.IllegalStateError{})

				helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, messagesPublished-uint64(discardOffset))
			})
		}
	})

	Context("with a connected messaging service", func() {
		var messageBuilder solace.OutboundMessageBuilder
		var publisher solace.RequestReplyMessagePublisher
		var builder solace.RequestReplyMessageReceiverBuilder
		const publishTimeOut = 3 * time.Second
		const gracePeriod = 5 * time.Second

		BeforeEach(func() {
			var err error
			err = messagingService.Connect()
			Expect(err).To(BeNil())

			messageBuilder = messagingService.MessageBuilder()

			publisher, err = messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			err = publisher.Start()
			Expect(err).ToNot(HaveOccurred())

			builder = messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder()
		})

		AfterEach(func() {
			var err error
			err = publisher.Terminate(10 * time.Second) // 30 second
			Expect(err).To(BeNil())

			err = messagingService.Disconnect()
			Expect(err).To(BeNil())
		})

		startFunctions := map[string](func(solace.RequestReplyMessageReceiver) error){
			"Start": func(rrmr solace.RequestReplyMessageReceiver) error {
				return rrmr.Start()
			},
			"StartAsync": func(rrmr solace.RequestReplyMessageReceiver) error {
				return <-rrmr.StartAsync()
			},
			"StartAsyncCallback": func(rrmr solace.RequestReplyMessageReceiver) error {
				startChan := make(chan error)
				rrmr.StartAsyncCallback(func(passedRrmr solace.RequestReplyMessageReceiver, e error) {
					Expect(passedRrmr).To(Equal(rrmr))
					startChan <- e
				})
				return <-startChan
			},
		}
		for startFunctionName, startFunction := range startFunctions {
			start := startFunction
			It("should be able to receive a message successfully using start function "+startFunctionName, func() {
				topicString := "try-me"
				receiver, err := builder.Build(resource.TopicSubscriptionOf(topicString))
				Expect(err).ToNot(HaveOccurred())

				msgChannel := make(chan message.InboundMessage)
				messageHandler := func(msg message.InboundMessage, replier solace.Replier) {
					reply, err := messageBuilder.BuildWithStringPayload("Pong") // send reply back
					Expect(err).ToNot(HaveOccurred())
					err = replier.Reply(reply)
					Expect(err).ToNot(HaveOccurred())

					msgChannel <- msg // put into channel
				}
				receiver.ReceiveAsync(messageHandler)

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()

				err = start(receiver)
				Expect(err).ToNot(HaveOccurred())

				// get the reply message
				replyChannel := make(chan message.InboundMessage, 1)
				replyMessageHandler := func(msg message.InboundMessage, userContext interface{}, err error) {
					replyChannel <- msg
				}

				// send message
				payload := "Ping"
				msg, err := messageBuilder.BuildWithStringPayload(payload)
				Expect(err).To(BeNil())
				topic := resource.TopicOf(topicString)
				publisher.Publish(msg, replyMessageHandler, topic, publishTimeOut, nil, nil)

				// check that the message & reply was sent via semp
				client := helpers.GetClient(messagingService)
				Expect(client.DataTxMsgCount).To(Equal(int64(2)))

				select {
				case receivedMessage := <-msgChannel:
					Expect(receivedMessage).ToNot(BeNil())
					content, ok := receivedMessage.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(content).To(Equal(payload))

					// for the reply message
					var replyMessage message.InboundMessage
					Eventually(replyChannel).Should(Receive(&replyMessage))
					Expect(replyMessage).ToNot(BeNil())
					content, ok = replyMessage.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(content).To(Equal("Pong"))
				case <-time.After(gracePeriod):
					Fail("Timed out waiting to receive message")
				}
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(2))) // send  & reply
			})
			It("should be able to synchronously receive a message successfully using start function "+startFunctionName, func() {
				topicString := "try-me"
				receiver, err := builder.Build(resource.TopicSubscriptionOf(topicString))
				Expect(err).ToNot(HaveOccurred())

				msgChannel := make(chan message.InboundMessage, 1)

				defer func() {
					err = receiver.Terminate(gracePeriod)
					Expect(err).To(BeNil())
				}()
				err = start(receiver)
				Expect(err).ToNot(HaveOccurred())

				// send message
				payload := "Ping"
				msg, err := messageBuilder.BuildWithStringPayload(payload)
				Expect(err).To(BeNil())
				topic := resource.TopicOf(topicString)
				replyChannel := make(chan message.InboundMessage, 1)
				replyMessageHandler := func(msg message.InboundMessage, userContext interface{}, err error) {
					replyChannel <- msg
				}
				publisher.Publish(msg, replyMessageHandler, topic, publishTimeOut, nil, nil)

				// check that the message was sent via semp
				client := helpers.GetClient(messagingService)
				Expect(client.DataTxMsgCount).To(Equal(int64(1)))

				go func() {
					defer GinkgoRecover()
					msg, replier, err := receiver.ReceiveMessage(-1)
					Expect(err).ToNot(HaveOccurred())
					Expect(replier).ToNot(BeNil())

					reply, err := messageBuilder.BuildWithStringPayload("Pong") // send reply back
					Expect(err).ToNot(HaveOccurred())
					err = replier.Reply(reply)
					Expect(err).ToNot(HaveOccurred())
					msgChannel <- msg
				}()

				select {
				case receivedMessage := <-msgChannel:
					Expect(receivedMessage).ToNot(BeNil())
					content, ok := receivedMessage.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(content).To(Equal(payload))

					// for the reply message
					var replyMessage message.InboundMessage
					Eventually(replyChannel).Should(Receive(&replyMessage))
					Expect(replyMessage).ToNot(BeNil())
					content, ok = replyMessage.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(content).To(Equal("Pong"))
				case <-time.After(gracePeriod):
					Fail("Timed out waiting to receive message")
				}
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(2))) // message & reply
			})
		}

		It("should start idempotently", func() {
			topicString := "try-me"
			receiver, err := builder.Build(resource.TopicSubscriptionOf(topicString))
			Expect(err).ToNot(HaveOccurred())

			msgChannel := make(chan message.InboundMessage)
			messageHandler := func(msg message.InboundMessage, replier solace.Replier) {
				reply, err := messageBuilder.BuildWithStringPayload("Pong") // send reply back
				Expect(err).ToNot(HaveOccurred())
				err = replier.Reply(reply)
				Expect(err).ToNot(HaveOccurred())

				msgChannel <- msg // put into channel
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
			payload := "Ping"
			msg, err := messageBuilder.BuildWithStringPayload(payload)
			Expect(err).To(BeNil())
			topic := resource.TopicOf(topicString)
			replyChannel := make(chan message.InboundMessage, 1)
			replyMessageHandler := func(msg message.InboundMessage, userContext interface{}, err error) {
				replyChannel <- msg
			}
			publisher.Publish(msg, replyMessageHandler, topic, publishTimeOut, nil, nil)

			// check that the message was sent via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnClient(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(messagingService.GetApplicationID()), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data.DataTxMsgCount).To(Equal(int64(2))) // for message & reply

			select {
			case receivedMessage := <-msgChannel:
				Expect(receivedMessage).ToNot(BeNil())
				content, ok := receivedMessage.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(content).To(Equal(payload))
			case <-time.After(gracePeriod):
				Fail("Timed out waiting to receive message")
			}
			Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(2))) // message & reply

			select {
			case <-msgChannel:
				Fail("did not expect to receive another message from msgChannel")
			case <-time.After(100 * time.Millisecond):
				// no more messages were received
			}
		})

		terminateFunctions := map[string](func(solace.RequestReplyMessageReceiver) error){
			"Terminate": func(rrmr solace.RequestReplyMessageReceiver) error {
				return rrmr.Terminate(gracePeriod)
			},
			"TerminateAsync": func(rrmr solace.RequestReplyMessageReceiver) error {
				return <-rrmr.TerminateAsync(gracePeriod)
			},
			"TerminateAsyncCallback": func(rrmr solace.RequestReplyMessageReceiver) error {
				errChan := make(chan error)
				rrmr.TerminateAsyncCallback(gracePeriod, func(e error) {
					errChan <- e
				})
				return <-errChan
			},
		}

		for terminateFunctionName, terminateFunction := range terminateFunctions {
			terminate := terminateFunction
			It("can terminate using "+terminateFunctionName, func() {
				topicString := "try-me"
				receiver, err := builder.Build(resource.TopicSubscriptionOf(topicString))
				Expect(err).ToNot(HaveOccurred())

				messageHandler := func(msg message.InboundMessage, replier solace.Replier) {
					// we aren't doing anything in the handler
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
			receiver, err := builder.Build(resource.TopicSubscriptionOf(topicString))
			Expect(err).ToNot(HaveOccurred())

			blocker := make(chan struct{})
			received := make(chan struct{})
			messageHandler := func(msg message.InboundMessage, replier solace.Replier) {
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
			handler := func(msg message.InboundMessage, userContext interface{}, err error) {} // empty handler
			publisher.Publish(msg, handler, topic, publishTimeOut, nil, nil)
			// we should now have messageHandler blocked
			select {
			case <-received:
				// success
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be received")
			}

			errChan := receiver.TerminateAsync(gracePeriod)
			// allow termination to start
			select {
			case <-errChan:
				Fail("did not expect to receive error when callback is still running")
			case <-time.After(20 * time.Millisecond):
				// allow termination to start
				helpers.ValidateState(receiver, false, true, false)
			}

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

		It("should terminate idempotently", func() {
			topicString := "try-me"
			receiver, err := builder.Build(resource.TopicSubscriptionOf(topicString))
			Expect(err).ToNot(HaveOccurred())

			msgChannel := make(chan message.InboundMessage)
			messageHandler := func(msg message.InboundMessage, replier solace.Replier) {
				msgChannel <- msg // put into channel
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

		Context("with a started and subscribed receiver", func() {
			const topicString = "try-me"
			var receiver solace.RequestReplyMessageReceiver
			var publishTimeOut = 3 * time.Second

			BeforeEach(func() {
				var err error
				receiver, err = builder.Build(resource.TopicSubscriptionOf(topicString))
				Expect(err).ToNot(HaveOccurred())
				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				if !receiver.IsTerminated() {
					receiver.Terminate(1 * time.Second)
				}
			})

			It("should terminate with undelivered messages without an async callback", func() {
				undeliveredCount := 10

				publishReplyChan := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, undeliveredCount)
				Eventually(publishReplyChan).Should(Receive()) // something was published

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

			It("should wait to terminate until all messages are processed with synchronous receive", func() {
				publishChan := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1)
				Eventually(publishChan).Should(Receive()) // something was published

				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 1) // reply not yet sent
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Consistently(terminateChannel).ShouldNot(Receive())
				msg, replier, err := receiver.ReceiveMessage(-1)
				Expect(err).ToNot(HaveOccurred())
				Expect(msg).ToNot(BeNil())
				Expect(replier).ToNot(BeNil())
				err = replier.Reply(helpers.NewMessage(messagingService, "Pong"))
				Expect(err).ToNot(HaveOccurred())
				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 2) // the reply has been sent now
				Eventually(terminateChannel).Should(Receive(BeNil()))
			})

			It("should wait to terminate until all messages are processed with async receive", func() {
				blocker := make(chan struct{})
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage, replier solace.Replier) {
					<-blocker
				})
				publishChan1 := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1)
				publishChan2 := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1)
				Eventually(publishChan1).Should(Receive()) // something was published
				Eventually(publishChan2).Should(Receive()) // something was published
				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 2)
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Consistently(terminateChannel).ShouldNot(Receive())
				close(blocker)
				Eventually(terminateChannel).Should(Receive(BeNil()))
			})

			It("should reject asynchronous callback registration while terminating", func() {
				publishChan := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1)
				Eventually(publishChan).Should(Receive()) // something was published
				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 1)
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Consistently(terminateChannel).ShouldNot(Receive())

				err := receiver.ReceiveAsync(func(inboundMessage message.InboundMessage, replier solace.Replier) {})
				helpers.ValidateError(err, &solace.IllegalStateError{})

				msg, replier, err := receiver.ReceiveMessage(-1)
				Expect(err).ToNot(HaveOccurred())
				Expect(msg).ToNot(BeNil())
				Expect(replier).ToNot(BeNil())
				Eventually(terminateChannel).Should(Receive(BeNil()))
			})

			It("should wait to terminate when all messages are processed but asynchronous callback is blocked", func() {
				blocker := make(chan struct{})
				blocking := make(chan struct{})
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage, replier solace.Replier) {
					close(blocking)
					<-blocker
				})
				publishChan := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1)
				Eventually(publishChan).Should(Receive()) // something was published
				Eventually(blocking).Should(BeClosed())
				publishChan = helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1)
				Eventually(publishChan).Should(Receive()) // something was published
				helpers.ValidateMetric(messagingService, metrics.DirectMessagesReceived, 2)
				terminateChannel := receiver.TerminateAsync(10 * time.Second)
				Consistently(terminateChannel).ShouldNot(Receive())
				msg, replier, err := receiver.ReceiveMessage(500 * time.Millisecond)
				Expect(err).ToNot(HaveOccurred())
				Expect(msg).ToNot(BeNil())
				Expect(replier).ToNot(BeNil())
				Consistently(terminateChannel).ShouldNot(Receive())
				close(blocker)
				Eventually(terminateChannel).Should(Receive(BeNil()))
			})

			// SOL-112355 - the tests to cover request/reply processing using RequestReply Publisher/Receiver
			Describe("for request/reply processing", func() {

				It("should time out waiting for a message", func() {
					done := make(chan struct{})
					go func() {
						defer GinkgoRecover()
						msg, replier, err := receiver.ReceiveMessage(1 * time.Second)
						Expect(msg).To(BeNil())
						Expect(replier).To(BeNil())
						helpers.ValidateError(err, &solace.TimeoutError{})
						close(done)
					}()
					// We want to make sure that it does not close for at least 500ms
					Consistently(done, 500*time.Millisecond).ShouldNot(BeClosed())
					// we want it to be closed after 1 second
					Eventually(done).Should(BeClosed())
				})

				It("should be able to continue message delivery when a receive async panics", func() {
					msgReceived := make(chan message.InboundMessage)
					receiver.ReceiveAsync(func(inboundMessage message.InboundMessage, replier solace.Replier) {
						msgReceived <- inboundMessage
						panic("this should still pass even though this Panic occurred")
					})
					const payloadOne = "one"
					const payloadTwo = "two"
					helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1, payloadOne)

					// we should receive a message
					var msg message.InboundMessage
					Eventually(msgReceived).Should(Receive(&msg))
					payload, ok := msg.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(payload).To(Equal(payloadOne))
					helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1, payloadTwo)
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
						msg, replier, err := receiver.ReceiveMessage(-1)
						Expect(err).ToNot(HaveOccurred())
						Expect(msg).ToNot(BeNil())
						Expect(replier).ToNot(BeNil())
						time.Sleep(3 * time.Second)
						close(done)
					}()
					Consistently(done, 2*time.Second).ShouldNot(BeClosed()) // less than receive function's sleep
					helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1)
					Eventually(done).Should(BeClosed())
				})

				It("should be interrupted while waiting for a message", func() {
					done := make(chan struct{})
					go func() {
						defer GinkgoRecover()
						msg, replier, err := receiver.ReceiveMessage(-1)
						Expect(msg).To(BeNil())
						Expect(replier).To(BeNil())
						helpers.ValidateError(err, &solace.IllegalStateError{}, "terminated")
						close(done)
					}()
					Consistently(done, 1*time.Second).ShouldNot(BeClosed())
					terminateChannel := receiver.TerminateAsync(10 * time.Second)
					Eventually(terminateChannel).Should(Receive(BeNil()))
					Eventually(done).Should(BeClosed())
				})

				receiverFunctions := map[string](func(solace.RequestReplyMessageReceiver, int) chan message.InboundMessage){
					"Receive": func(rrmr solace.RequestReplyMessageReceiver, count int) chan message.InboundMessage {
						msgChannel := make(chan message.InboundMessage, count)
						for i := 0; i < count; i++ {
							go func() {
								defer GinkgoRecover()
								msg, replier, err := rrmr.ReceiveMessage(-1)
								Expect(err).ToNot(HaveOccurred())
								Expect(msg).ToNot(BeNil())
								Expect(replier).ToNot(BeNil())
								err = replier.Reply(helpers.NewMessage(messagingService, "Pong")) // send reply back
								Expect(err).ToNot(HaveOccurred())
								msgChannel <- msg // put into channel
							}()
						}
						return msgChannel
					},
					"ReceiveAsync": func(rrmr solace.RequestReplyMessageReceiver, count int) chan message.InboundMessage {
						msgChannel := make(chan message.InboundMessage, count)
						rrmr.ReceiveAsync(func(msg message.InboundMessage, replier solace.Replier) {
							go func() { // send reply in a new go routine
								err := replier.Reply(helpers.NewMessage(messagingService, "Pong")) // send reply back
								Expect(err).ToNot(HaveOccurred())
								msgChannel <- msg // put into channel
							}()
						})
						return msgChannel
					},
				}

				for receiverFunctionName, receiverFunction := range receiverFunctions {
					receiverFunc := receiverFunction

					It("should be able to receive a message successfully with "+receiverFunctionName+"() function", func() {
						// publish the request messages
						publishedMessages := 0
						publisherReplies, _, publisherComplete := helpers.PublishRequestReplyMessages(messagingService, topicString, publishTimeOut, uint(1), &publishedMessages, "Ping")

						// allow the goroutine above to saturate the publisher
						select {
						case <-publisherComplete:
							// allow the publishing to complete before proceeding
						case <-time.After(100 * time.Millisecond):
							// should not timeout before publishing is complete
							Fail("Not expected to timeout before publishing is complete; Should not get here")
						}

						// receive the request message and send back a reply
						receivedMsgChannel := receiverFunc(receiver, 1)

						// check that the message & reply was sent via semp
						client := helpers.GetClient(messagingService)
						Expect(client.DataTxMsgCount).To(Equal(int64(2)))

						select {
						case receivedMessage := <-receivedMsgChannel:
							Expect(receivedMessage).ToNot(BeNil())
							content, ok := receivedMessage.GetPayloadAsString()
							Expect(ok).To(BeTrue())
							Expect(content).To(Equal("Ping"))

							// for the reply message
							var replyMessage message.InboundMessage
							Eventually(publisherReplies).Should(Receive(&replyMessage)) // something was published
							Expect(replyMessage).ToNot(BeNil())
							content, ok = replyMessage.GetPayloadAsString()
							Expect(ok).To(BeTrue())
							Expect(content).To(Equal("Pong"))
						case <-time.After(gracePeriod):
							Fail("Timed out waiting to receive message")
						}

						Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(2))) // send  & reply
					})

					It("should be able to receive multiple messages successfully with "+receiverFunctionName+"() function", func() {
						// send messages
						sentMessage := 500
						const publishTimeOut = 3 * time.Second

						var err error
						// we need to properly configure the receiver's buffer to handle > 50 published messages
						receiver, err = builder.OnBackPressureDropLatest(uint(sentMessage)).Build(resource.TopicSubscriptionOf(topicString))
						Expect(err).ToNot(HaveOccurred())
						err = receiver.Start()
						Expect(err).ToNot(HaveOccurred())

						// publish the request messages
						publishedMessages := 0
						publisherReplies, publisherSaturated, publisherComplete := helpers.PublishRequestReplyMessages(messagingService, topicString, publishTimeOut, uint(sentMessage), &publishedMessages, "Ping")

						// allow the goroutine above to saturate the publisher
						select {
						case <-publisherComplete:
							// block until publish complete
							Fail("Expected publisher to not be complete")
						case <-publisherSaturated:
							// allow the goroutine above to saturate the publisher (at least halfway filled)
						case <-time.After(1 * time.Second):
							// should not timeout while saturating the publisher
							Fail("Not expected to timeout while saturating publisher; Should not get here")
						}

						receivedMsgChannel := receiverFunc(receiver, sentMessage)
						Eventually(receivedMsgChannel).Should(HaveLen(sentMessage)) // replies should be sent back
						// message in the channel should be a request message
						receivedMessage := <-receivedMsgChannel
						content, ok := receivedMessage.GetPayloadAsString()
						Expect(ok).To(BeTrue())
						Expect(content).To(Equal("Ping"))

						select {
						// message in the reply channel should be a reply message
						case replyMessage := <-publisherReplies:
							Expect(replyMessage).ToNot(BeNil())
							content, ok := replyMessage.GetPayloadAsString()
							Expect(ok).To(BeTrue())
							Expect(content).To(Equal("Pong"))
						case <-time.After(20 * time.Second):
							Fail("Timed out waiting to receive reply message")
						}

						// check that the message & reply was sent via semp
						client := helpers.GetClient(messagingService)
						Expect(client.DataTxMsgCount).To(Equal(int64(sentMessage * 2)))                                                     // requests & replies
						Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically(">", uint64(sentMessage))) // send  & reply
					})
				}

				It("should properly handle direct massages published to request-reply topic with the Receive() function", func() {
					helpers.PublishOneMessage(messagingService, topicString, "Ping") // publish direct message
					// block and wait for direct message
					msg, replier, err := receiver.ReceiveMessage(-1)
					Expect(err).ToNot(HaveOccurred()) // not expecting an error
					Expect(msg).ToNot(BeNil())        // we should receive a message
					Expect(replier).To(BeNil())       // but no replier since this should be a direct message

					// publish the request messages on another thread to same topic
					helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, 1, "Ping")

					// block and wait for request message
					msg, replier, err = receiver.ReceiveMessage(-1)
					Expect(err).ToNot(HaveOccurred())
					Expect(msg).ToNot(BeNil())
					Expect(replier).ToNot(BeNil())
					err = replier.Reply(helpers.NewMessage(messagingService, "Pong")) // send reply back
					Expect(err).ToNot(HaveOccurred())

					// check that the message & reply was sent via semp
					client := helpers.GetClient(messagingService)
					Expect(client.DataTxMsgCount).To(Equal(int64(3)))                                                // 1 direct, 1 request and 1 reply message                                            // requests & replies
					Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(Equal(uint64(3))) // 3 messages
				})

				It("should properly handle direct messages published to request-reply topic with the ReceiveAsync() function", func() {
					rRMessagesCount := uint64(0)
					directMessagesCount := uint64(0)

					publishedRRMessages := 250
					publishedDirectMessages := 250
					receiverBackPressureBufferLength := publishedRRMessages + publishedDirectMessages

					// create a receiver with an adequate buffer size
					var err error
					// we need to properly configure the receiver's buffer to handle > 50 published messages
					receiver, err = builder.OnBackPressureDropLatest(uint(receiverBackPressureBufferLength)).Build(resource.TopicSubscriptionOf(topicString))
					Expect(err).ToNot(HaveOccurred())
					err = receiver.Start()
					Expect(err).ToNot(HaveOccurred())

					receiver.ReceiveAsync(func(inboundMessage message.InboundMessage, replier solace.Replier) {
						Expect(inboundMessage).ToNot(BeNil()) // we should receive a message
						if replier != nil {
							// request-reply message received
							err := replier.Reply(helpers.NewMessage(messagingService, "Pong")) // send reply back
							Expect(err).ToNot(HaveOccurred())
							atomic.AddUint64(&rRMessagesCount, 1) // increment
						} else {
							// direct message received
							atomic.AddUint64(&directMessagesCount, 1) // increment
						}
					})

					for i := 0; i < publishedDirectMessages; i++ {
						helpers.PublishOneMessage(messagingService, topicString, "Ping") // publish direct message
					}

					// publish the request messages on another thread to same topic
					publishChan := helpers.PublishNRequestReplyMessages(messagingService, topicString, publishTimeOut, publishedRRMessages, "Ping")

					select {
					// last message (any message) in the reply channel should be a reply message
					case replyMessage := <-publishChan:
						Expect(replyMessage).ToNot(BeNil())
						content, ok := replyMessage.GetPayloadAsString()
						Expect(ok).To(BeTrue())
						Expect(content).To(Equal("Pong"))
					case <-time.After(20 * time.Second):
						Fail("Timed out waiting to receive reply message")
					}

					// check that the message & reply was sent via semp
					Eventually(func() int64 {
						return helpers.GetClient(messagingService).DataTxMsgCount
					}).Should(BeNumerically("==", int64(publishedRRMessages*2)+int64(publishedDirectMessages)))

					Eventually(func() uint64 {
						return messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)
					}).Should(BeNumerically("==", uint64(publishedRRMessages*2)+uint64(publishedDirectMessages))) // the messages

					Expect(atomic.LoadUint64(&rRMessagesCount)).To(BeNumerically("==", publishedRRMessages))
					Expect(atomic.LoadUint64(&directMessagesCount)).To(BeNumerically("==", publishedDirectMessages))
				})
			})
		})

	})

})

type myCustomRequestReplySubscription struct {
}

func (sub *myCustomRequestReplySubscription) GetName() string {
	return "some string"
}

func (sub *myCustomRequestReplySubscription) GetSubscriptionType() string {
	return "some type"
}
