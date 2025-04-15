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
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/message/rgmid"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/sempclient/action"
	sempconfig "solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/sempclient/monitor"
	"solace.dev/go/messaging/test/testcontext"
)

var persistentReceiverStartFunctions = map[string](func(receiver solace.PersistentMessageReceiver) <-chan error){
	"sync": func(receiver solace.PersistentMessageReceiver) <-chan error {
		c := make(chan error)
		go func() {
			c <- receiver.Start()
		}()
		return c
	},
	"async": func(receiver solace.PersistentMessageReceiver) <-chan error {
		return receiver.StartAsync()
	},
	"callback": func(receiver solace.PersistentMessageReceiver) <-chan error {
		c := make(chan error)
		receiver.StartAsyncCallback(func(dmp solace.PersistentMessageReceiver, e error) {
			defer GinkgoRecover()
			Expect(dmp).To(Equal(receiver))
			c <- e
		})
		return c
	},
}
var persistentReceiverTerminateFunctions = map[string](func(receiver solace.PersistentMessageReceiver) <-chan error){
	"sync": func(receiver solace.PersistentMessageReceiver) <-chan error {
		c := make(chan error)
		go func() {
			c <- receiver.Terminate(gracePeriod)
		}()
		return c
	},
	"async": func(receiver solace.PersistentMessageReceiver) <-chan error {
		return receiver.TerminateAsync(gracePeriod)
	},
	"callback": func(receiver solace.PersistentMessageReceiver) <-chan error {
		c := make(chan error)
		receiver.TerminateAsyncCallback(gracePeriod, func(e error) {
			c <- e
		})
		return c
	},
}

var _ = Describe("PersistentReceiver", func() {
	const queueName = "testqueue"
	const topicString = "persistent/receiver"

	Context("with a default messaging service", func() {
		var messagingService solace.MessagingService
		BeforeEach(func() {
			messagingService = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration()))
		})

		Describe("builder verification", func() {
			It("can print a builder to a string", func() {
				builder := messagingService.CreatePersistentMessageReceiverBuilder()
				Expect(fmt.Sprint(builder)).To(ContainSubstring(fmt.Sprintf("%p", builder)))
			})

			invalidConfiugrationOptions := []config.ReceiverPropertyMap{
				{config.ReceiverPropertyPersistentMissingResourceCreationStrategy: "notaresourcestrategy"},
				{config.ReceiverPropertyPersistentMissingResourceCreationStrategy: 15},
				{config.ReceiverPropertyPersistentMissingResourceCreationStrategy: config.MissingResourcesCreationStrategy("notastrategy")},
				{config.ReceiverPropertyPersistentMessageSelectorQuery: 3},
				{config.ReceiverPropertyPersistentStateChangeListener: "this isn't a state change listener is it..."},
				{config.ReceiverPropertyPersistentMessageAckStrategy: 1234},
				{config.ReceiverPropertyPersistentMessageReplayStrategy: 1234},
				{config.ReceiverPropertyPersistentMessageReplayStrategy: config.PersistentReplayIDBased},
				{config.ReceiverPropertyPersistentMessageReplayStrategy: config.PersistentReplayIDBased,
					config.ReceiverPropertyPersistentMessageReplayStrategyIDBasedReplicationGroupMessageID: ""},
				{config.ReceiverPropertyPersistentMessageReplayStrategy: config.PersistentReplayIDBased,
					config.ReceiverPropertyPersistentMessageReplayStrategyIDBasedReplicationGroupMessageID: 1234},
				{config.ReceiverPropertyPersistentMessageReplayStrategy: config.PersistentReplayTimeBased},
			}
			for _, invalidConfigurationOptionRef := range invalidConfiugrationOptions {
				invalidConfigurationOption := invalidConfigurationOptionRef
				It("fails to build a new persistent receiver with property map "+fmt.Sprint(invalidConfigurationOption), func() {
					receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
						FromConfigurationProvider(invalidConfigurationOption).Build(resource.QueueNonDurableExclusiveAnonymous())
					Expect(receiver).To(BeNil())
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
			}

			It("fails to build with invalid subscription types", func() {
				receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
					WithSubscriptions(&myCustomSubscription{}).Build(resource.QueueNonDurableExclusiveAnonymous())
				Expect(receiver).To(BeNil())
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
			})

			It("fails to build with a nil queue", func() {
				receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().Build(nil)
				Expect(receiver).To(BeNil())
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
			})

			It("can build with a nil configuration provider", func() {
				receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
					FromConfigurationProvider(nil).
					Build(resource.QueueNonDurableExclusiveAnonymous())
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver).ToNot(BeNil())
			})
		})

		It("can print a receiver to a string", func() {
			receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().Build(resource.QueueNonDurableExclusiveAnonymous())
			Expect(err).ToNot(HaveOccurred())
			Expect(fmt.Sprint(receiver)).To(ContainSubstring(fmt.Sprintf("%p", receiver)))
		})
		Context("with a connected messaging service and specially formatted queue", func() {
			const basicQueueName = "test_invalid_durability_on_bind_raises_exception"
			var modifiedQueueName string

			BeforeEach(func() {
				helpers.ConnectMessagingService(messagingService)
				foundHostName, _ := helpers.GetHostName(messagingService, basicQueueName)
				modifiedQueueName = "#P2P/QTMP/v:" + foundHostName + "/" + basicQueueName
				helpers.CreateQueue(modifiedQueueName)
			})

			AfterEach(func() {
				foundHostName, _ := helpers.GetHostName(nil, basicQueueName)
				modifiedQueueName = "#P2P/QTMP/v:" + foundHostName + "/" + basicQueueName
				if messagingService.IsConnected() {
					helpers.DisconnectMessagingService(messagingService)
				}
				helpers.DeleteQueue(url.QueryEscape(modifiedQueueName))
			})

			Describe("Invalid Queue or Topic Endpoint", func() {
				It("can return an error when configured to bind to a non-durable queue but attempts to bind to a durable queue.", func() {
					receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueNonDurableExclusive(basicQueueName))
					err := receiver.Start()
					Expect(err).ShouldNot(BeNil())
					helpers.ValidateNativeError(err, subcode.InvalidDurability)
				})
			})
		})

		Context("with a connected messaging service and queue", func() {

			BeforeEach(func() {
				helpers.ConnectMessagingService(messagingService)
				helpers.CreateQueue(queueName, topicString)
			})

			AfterEach(func() {
				if messagingService.IsConnected() {
					helpers.DisconnectMessagingService(messagingService)
				}
				helpers.DeleteQueue(queueName)
			})

			Describe("Lifecycle", func() {
				for startName, fn := range persistentReceiverStartFunctions {
					for terminateName, fn2 := range persistentReceiverTerminateFunctions {
						start := fn
						terminate := fn2

						var publishAndReceiveMessage = func(receiver solace.PersistentMessageReceiver) {
							helpers.PublishOnePersistentMessage(messagingService, topicString)
							msg, err := receiver.ReceiveMessage(10 * time.Second)
							Expect(err).ToNot(HaveOccurred())
							Expect(msg).ToNot(BeNil())
							Eventually(func() int64 {
								return helpers.GetClient(messagingService).DataTxMsgCount
							}).Should(BeNumerically("==", 1))
						}

						It("can start and terminate with start "+startName+" and terminate "+terminateName, func() {
							receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
							// check that not started
							helpers.ValidateState(receiver, false, false, false)
							// start and check state
							Eventually(start(receiver)).Should(Receive(Not(HaveOccurred())))
							helpers.ValidateState(receiver, true, false, false)
							// try using receiver
							publishAndReceiveMessage(receiver)
							// terminate and check state
							Eventually(terminate(receiver)).Should(Receive(Not(HaveOccurred())))
							helpers.ValidateState(receiver, false, false, true)
						})
						It("can start and terminate idempotently with start "+startName+" and terminate "+terminateName, func() {
							receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
							// check that not started
							helpers.ValidateState(receiver, false, false, false)
							// start and check state
							c1 := start(receiver)
							c2 := start(receiver)
							Eventually(c1).Should(Receive(Not(HaveOccurred())))
							Eventually(c2).Should(Receive(Not(HaveOccurred())))
							helpers.ValidateState(receiver, true, false, false)
							// try using receiver
							publishAndReceiveMessage(receiver)
							// terminate and check state
							c1 = terminate(receiver)
							c2 = terminate(receiver)
							Eventually(c1).Should(Receive(Not(HaveOccurred())))
							Eventually(c2).Should(Receive(Not(HaveOccurred())))
							helpers.ValidateState(receiver, false, false, true)
							c1 = start(receiver)
							c2 = start(receiver)
							Eventually(c1).Should(Receive(BeAssignableToTypeOf(&solace.IllegalStateError{})))
							Eventually(c2).Should(Receive(BeAssignableToTypeOf(&solace.IllegalStateError{})))
						})
					}
				}
				for startName, fn := range persistentReceiverStartFunctions {
					start := fn
					It("should fail to start when messaging service is disconnected using start "+startName, func() {
						receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
						messagingService.Disconnect()
						helpers.ValidateChannelError(start(receiver), &solace.IllegalStateError{})
					})
					It("should fail to start when messaging service is down using start "+startName, func() {
						receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))

						helpers.ForceDisconnectViaSEMPv2(messagingService)
						Eventually(messagingService.IsConnected).Should(BeFalse())
						Expect(messagingService.Disconnect()).ToNot(HaveOccurred())

						helpers.ValidateChannelError(start(receiver), &solace.IllegalStateError{})
					})
				}
				for terminateName, fn := range persistentReceiverTerminateFunctions {
					terminate := fn
					It("should fail to terminate when not started using terminate "+terminateName, func() {
						receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
						Eventually(terminate(receiver)).Should(Receive(BeNil()))
						Expect(receiver.IsTerminated()).To(BeTrue())
						helpers.ValidateError(receiver.Start(), &solace.IllegalStateError{})
					})
				}
				Context("with an unstarted receiver", func() {
					var receiver solace.PersistentMessageReceiver
					BeforeEach(func() {
						receiver = helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
					})

					It("should fail to receive", func() {
						msg, err := receiver.ReceiveMessage(10 * time.Second)
						Expect(msg).To(BeNil())
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
					It("should fail to retrieve receiver info", func() {
						receiverInfo, err := receiver.ReceiverInfo()
						helpers.ValidateError(err, &solace.IllegalStateError{})
						Expect(receiverInfo).To(BeNil())
					})
					It("should fail to pause receiver", func() {
						err := receiver.Pause()
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
					It("should fail to resume receiver", func() {
						err := receiver.Resume()
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
					It("should fail to acknowledge a message", func() {
						err := receiver.Ack(nil)
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
					DescribeTable("should fail to settle a nil message as ",
						func(outcome config.MessageSettlementOutcome) {
							err := receiver.Settle(nil, outcome)
							helpers.ValidateError(err, &solace.IllegalStateError{})
						},
						Entry("accepted", config.PersistentReceiverAcceptedOutcome),
						Entry("rejected", config.PersistentReceiverRejectedOutcome),
						Entry("failed", config.PersistentReceiverFailedOutcome),
					)
					It("should fail to settle a nil message with garbage", func() {
						err := receiver.Settle(nil, "garbage")
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
				})

				Context("with a terminated receiver", func() {
					var receiver solace.PersistentMessageReceiver
					BeforeEach(func() {
						receiver = helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
						receiver.Start()
						receiver.Terminate(gracePeriod)
					})

					It("should fail to receive", func() {
						msg, err := receiver.ReceiveMessage(10 * time.Second)
						Expect(msg).To(BeNil())
						helpers.ValidateError(err, &solace.IllegalStateError{})
						helpers.ValidateError(receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {}), &solace.IllegalStateError{})
					})
					It("should fail to retrieve receiver info", func() {
						receiverInfo, err := receiver.ReceiverInfo()
						helpers.ValidateError(err, &solace.IllegalStateError{})
						Expect(receiverInfo).To(BeNil())
					})
					It("should fail to resume receiver", func() {
						err := receiver.Resume()
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
					It("should fail to pause receiver", func() {
						err := receiver.Pause()
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
					It("should fail to acknowledge a nil message", func() {
						err := receiver.Ack(nil)
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
					DescribeTable("should fail to settle a nil message as ",
						func(outcome config.MessageSettlementOutcome) {
							err := receiver.Settle(nil, outcome)
							helpers.ValidateError(err, &solace.IllegalStateError{})
						},
						Entry("accepted", config.PersistentReceiverAcceptedOutcome),
						Entry("rejected", config.PersistentReceiverRejectedOutcome),
						Entry("failed", config.PersistentReceiverFailedOutcome),
					)
					It("should fail to settle a nil message with garbage", func() {
						err := receiver.Settle(nil, "garbage")
						helpers.ValidateError(err, &solace.IllegalStateError{})
					})
				})

				It("should fail to start with an invalid queue name", func() {
					receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().Build(resource.QueueDurableExclusive("notexpectedqueue"))
					Expect(receiver).ToNot(BeNil())
					Expect(err).ToNot(HaveOccurred())
					err = receiver.Start()
					helpers.ValidateNativeError(err, subcode.UnknownQueueName)
					Expect(receiver.IsTerminated()).To(BeTrue())
					Expect(receiver.Terminate(0)).ToNot(HaveOccurred())
				})
				It("should fail to start with an invalid subscription", func() {
					receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
						WithSubscriptions(resource.TopicSubscriptionOf(invalidTopicString)).
						Build(resource.QueueDurableExclusive(queueName))
					Expect(receiver).ToNot(BeNil())
					Expect(err).ToNot(HaveOccurred())
					err = receiver.Start()
					helpers.ValidateNativeError(err, subcode.InvalidTopicSyntax)
					Expect(receiver.IsTerminated()).To(BeTrue())
					Expect(receiver.Terminate(0)).ToNot(HaveOccurred())
				})
			})

			It("can remove subscriptions added by another message receiver", func() {
				topic := "subscriptionForReceiverTerminated"
				receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableNonExclusive(queueName))
				Expect(receiver.Start()).ToNot(HaveOccurred())
				Expect(receiver.AddSubscription(resource.TopicSubscriptionOf(topic))).ToNot(HaveOccurred())
				subscriptions := helpers.GetQueueSubscriptions(queueName)
				Expect(subscriptions).To(ContainElement(topic))
				Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
				newReceiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableNonExclusive(queueName))
				Expect(newReceiver.Start()).ToNot(HaveOccurred())
				Expect(newReceiver.RemoveSubscription(resource.TopicSubscriptionOf(topic))).ToNot(HaveOccurred())
				subscriptions = helpers.GetQueueSubscriptions(queueName)
				Expect(subscriptions).ToNot(ContainElement(topic))
				Expect(newReceiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			Context("with a started persistent message receiver", func() {
				var receiver solace.PersistentMessageReceiver

				BeforeEach(func() {
					var err error
					receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
						WithSubscriptions(resource.TopicSubscriptionOf(topicString)).
						Build(resource.QueueDurableNonExclusive(queueName))
					Expect(err).ToNot(HaveOccurred())
					err = receiver.Start()
					Expect(err).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					if !receiver.IsTerminated() {
						receiver.Terminate(10 * time.Second)
					}
				})

				// Basic receive tests
				receiveFunctions := map[string]func() chan message.InboundMessage{
					"receive sync": func() chan message.InboundMessage {
						msgChan := make(chan message.InboundMessage)
						go func() {
							defer GinkgoRecover()
							msg, err := receiver.ReceiveMessage(-1)
							Expect(err).ToNot(HaveOccurred())
							msgChan <- msg
						}()
						return msgChan
					},
					"receive async": func() chan message.InboundMessage {
						msgChan := make(chan message.InboundMessage)
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							msgChan <- inboundMessage
						})
						return msgChan
					},
				}
				for testCase, receiveFunctionRef := range receiveFunctions {
					receiveFunction := receiveFunctionRef
					It("can receive messages via "+testCase, func() {
						payload := "hello persistent receiver"
						helpers.PublishOnePersistentMessage(messagingService, topicString, payload)
						helpers.ValidateMetric(messagingService, metrics.PersistentMessagesReceived, 1)
						msgChan := receiveFunction()
						var msg message.InboundMessage
						Eventually(msgChan).Should(Receive(&msg))
						Expect(msg).ToNot(BeNil())
						actualPayload, ok := msg.GetPayloadAsString()
						Expect(ok).To(BeTrue())
						Expect(actualPayload).To(Equal(payload))

						helpers.ValidateMetric(messagingService, metrics.PersistentMessagesAccepted, 0)
						err := receiver.Ack(msg)
						Expect(err).ToNot(HaveOccurred())
						helpers.ValidateMetric(messagingService, metrics.PersistentMessagesAccepted, 1)
					})
				}

				// Add subscription tests
				const newTopicString = "receiver/subscribe/tests"
				addSubscriptionCases := map[string](func(*resource.TopicSubscription, solace.PersistentMessageReceiver) error){
					"synchronous subscribe": func(ts *resource.TopicSubscription, dmr solace.PersistentMessageReceiver) error {
						return dmr.AddSubscription(ts)
					},
					"asynchronous subscribe": func(ts *resource.TopicSubscription, dmr solace.PersistentMessageReceiver) error {
						complete := make(chan error)
						err := dmr.AddSubscriptionAsync(ts, func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
							defer GinkgoRecover()
							Expect(subscription.GetName()).To(Equal(ts.GetName()))
							Expect(operation).To(Equal(solace.SubscriptionAdded))
							complete <- errOrNil
						})
						if err != nil {
							return err
						}
						return <-complete
					},
				}
				// Remove subscription tests
				removeSubscriptionCases := map[string](func(*resource.TopicSubscription, solace.PersistentMessageReceiver) error){
					"synchronous unsubscribe": func(ts *resource.TopicSubscription, dmr solace.PersistentMessageReceiver) error {
						return dmr.RemoveSubscription(ts)
					},
					"asynchronous unsubscribe": func(ts *resource.TopicSubscription, dmr solace.PersistentMessageReceiver) error {
						complete := make(chan error)
						err := dmr.RemoveSubscriptionAsync(ts, func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
							defer GinkgoRecover()
							Expect(subscription.GetName()).To(Equal(ts.GetName()))
							Expect(operation).To(Equal(solace.SubscriptionRemoved))
							complete <- errOrNil
						})
						if err != nil {
							return err
						}
						return <-complete
					},
				}
				for subscribeCaseName, subscribeCaseFunction := range addSubscriptionCases {
					for unsubscribeCaseName, unsubscribeCaseFunction := range removeSubscriptionCases {
						subscribe := subscribeCaseFunction
						unsubscribe := unsubscribeCaseFunction
						It("should be able to add and remove subscriptions using "+subscribeCaseName+" and "+unsubscribeCaseName+" while running", func() {
							Expect(
								subscribe(resource.TopicSubscriptionOf(newTopicString), receiver),
							).ToNot(HaveOccurred())
							helpers.PublishOnePersistentMessage(messagingService, newTopicString)
							msg, err := receiver.ReceiveMessage(1 * time.Second)
							Expect(err).ToNot(HaveOccurred())
							Expect(msg).ToNot(BeNil())
							Expect(msg.GetDestinationName()).To(Equal(newTopicString))
							Expect(
								unsubscribe(resource.TopicSubscriptionOf(newTopicString), receiver),
							).ToNot(HaveOccurred())
							helpers.PublishOnePersistentMessage(messagingService, newTopicString)
							msg, err = receiver.ReceiveMessage(1 * time.Second)
							Expect(msg).To(BeNil())
							helpers.ValidateError(err, &solace.TimeoutError{})
						})
					}
				}
				for caseName, caseFunction := range addSubscriptionCases {
					subscribe := caseFunction
					It("should fail to add an invalid subscription using "+caseName, func() {
						helpers.ValidateError(
							subscribe(resource.TopicSubscriptionOf(invalidTopicString), receiver),
							&solace.NativeError{},
						)
					})
					It("should fail to add a subscription on an unstarted receiver using "+caseName, func() {
						unstartedReceiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
						helpers.ValidateError(
							subscribe(resource.TopicSubscriptionOf("some topic"), unstartedReceiver),
							&solace.IllegalStateError{},
						)
					})
					It("should fail to add a subscription on a terminated receiver using "+caseName, func() {
						receiver.Terminate(60 * time.Second)
						helpers.ValidateError(
							subscribe(resource.TopicSubscriptionOf("some topic"), receiver),
							&solace.IllegalStateError{},
						)
					})
				}
				for caseName, caseFunction := range removeSubscriptionCases {
					unsubscribe := caseFunction
					It("should fail to remove subscription using "+caseName, func() {
						helpers.ValidateError(
							unsubscribe(resource.TopicSubscriptionOf(invalidTopicString), receiver),
							&solace.NativeError{},
						)
					})
					It("should fail to remove a subscription on an unstarted receiver using "+caseName, func() {
						unstartedReceiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
						helpers.ValidateError(
							unsubscribe(resource.TopicSubscriptionOf("some topic"), unstartedReceiver),
							&solace.IllegalStateError{},
						)
					})
					It("should fail to remove a subscription on a terminated receiver using "+caseName, func() {
						receiver.Terminate(60 * time.Second)
						helpers.ValidateError(
							unsubscribe(resource.TopicSubscriptionOf("some topic"), receiver),
							&solace.IllegalStateError{},
						)
					})
				}

				It("waits for message delivery to complete on graceful termiantion", func() {
					helpers.PublishNPersistentMessages(messagingService, topicString, 2)
					terminateChan := receiver.TerminateAsync(10 * time.Second)
					Consistently(terminateChan).ShouldNot(Receive())
					msg, err := receiver.ReceiveMessage(5 * time.Second)
					Expect(err).ToNot(HaveOccurred())
					Expect(msg).ToNot(BeNil())
					// even after receiving one message, we should not be terminated
					Consistently(terminateChan).ShouldNot(Receive())
					msg, err = receiver.ReceiveMessage(5 * time.Second)
					Expect(err).ToNot(HaveOccurred())
					Expect(msg).ToNot(BeNil())
					Eventually(terminateChan).Should(Receive(BeNil()))
				})

				It("waits for message delivery to complete on graceful termiantion using receive async", func() {
					msgChan := make(chan message.InboundMessage)
					Expect(
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							msgChan <- inboundMessage
						}),
					).ToNot(HaveOccurred())
					helpers.PublishNPersistentMessages(messagingService, topicString, 2)
					terminateChan := receiver.TerminateAsync(10 * time.Second)
					Consistently(terminateChan).ShouldNot(Receive())
					Eventually(msgChan).Should(Receive())
					// even after receiving one message, we should not be terminated
					Consistently(terminateChan).ShouldNot(Receive())
					Eventually(msgChan).Should(Receive())
					Eventually(terminateChan).Should(Receive(BeNil()))
				})

				It("can successfully pause message receiption", func() {
					msgChan := make(chan message.InboundMessage)
					Expect(
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							msgChan <- inboundMessage
						}),
					).ToNot(HaveOccurred())
					Expect(receiver.Pause()).ToNot(HaveOccurred())
					helpers.PublishOnePersistentMessage(messagingService, topicString)
					Consistently(msgChan).ShouldNot(Receive())
					Expect(receiver.Resume()).ToNot(HaveOccurred())
					Eventually(msgChan).Should(Receive(Not(BeNil())))
				})

				It("can pause message receiption while currently processing a message", func() {
					blocker := make(chan struct{})
					msgChan := make(chan message.InboundMessage)
					Expect(
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							msgChan <- inboundMessage
							<-blocker
						}),
					).ToNot(HaveOccurred())
					helpers.PublishOnePersistentMessage(messagingService, topicString)
					Eventually(msgChan).Should(Receive())
					Expect(receiver.Pause()).ToNot(HaveOccurred())
					helpers.PublishOnePersistentMessage(messagingService, topicString)
					close(blocker)
					Consistently(msgChan).ShouldNot(Receive())
					Expect(receiver.Resume()).ToNot(HaveOccurred())
					Eventually(msgChan).Should(Receive())
				})

				It("can pause message reception idempotently", func() {
					msgChan := make(chan message.InboundMessage)
					Expect(
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							msgChan <- inboundMessage
						}),
					).ToNot(HaveOccurred())
					Expect(receiver.Pause()).ToNot(HaveOccurred())
					helpers.PublishOnePersistentMessage(messagingService, topicString)
					// pause again, should have no effect
					Expect(receiver.Pause()).ToNot(HaveOccurred())
					Consistently(msgChan).ShouldNot(Receive())
					Expect(receiver.Resume()).ToNot(HaveOccurred())
					Eventually(msgChan).Should(Receive(Not(BeNil())))
				})

				Context("pause and resume tests", func() {
					It("can pause and resume messaging repeatedly", func() {
						numMessages := 1000
						msgChan := make(chan message.InboundMessage, numMessages)
						Expect(
							receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
								msgChan <- inboundMessage
							}),
						).ToNot(HaveOccurred())
						pauseDone := make(chan struct{})
						resumeDone := make(chan struct{})
						publishDone := make(chan struct{})
						go func() {
							defer GinkgoRecover()
							helpers.PublishNPersistentMessages(messagingService, topicString, numMessages)
							close(publishDone)
						}()
						go func() {
							defer GinkgoRecover()
						pauseLoop:
							for {
								select {
								case <-publishDone:
									break pauseLoop
								default:
									Expect(receiver.Pause()).ToNot(HaveOccurred())
								}
							}
							close(pauseDone)
						}()
						go func() {
							defer GinkgoRecover()
						resumeLoop:
							for {
								select {
								case <-publishDone:
									break resumeLoop
								default:
									Expect(receiver.Resume()).ToNot(HaveOccurred())
								}
							}
							close(resumeDone)
						}()

						Eventually(publishDone, 10*time.Second).Should(BeClosed())
						Eventually(pauseDone).Should(BeClosed())
						Eventually(resumeDone).Should(BeClosed())

						Expect(receiver.Resume()).ToNot(HaveOccurred())

						// Make sure we receive all messages
						for i := 0; i < numMessages; i++ {
							Eventually(msgChan).Should(Receive())
						}
					})

				})

				It("does not receive multiple messages with overlapping subscriptions", func() {
					msgChan := make(chan message.InboundMessage)
					Expect(
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							msgChan <- inboundMessage
						}),
					).ToNot(HaveOccurred())
					topicOne := "persistent/overlapping/message/test"
					topicTwo := "persistent/overlapping/>"
					Expect(receiver.AddSubscription(resource.TopicSubscriptionOf(topicOne))).ToNot(HaveOccurred())
					Expect(receiver.AddSubscription(resource.TopicSubscriptionOf(topicTwo))).ToNot(HaveOccurred())
					helpers.PublishOnePersistentMessage(messagingService, topicOne)
					Eventually(msgChan).Should(Receive(Not(BeNil())))
					Consistently(msgChan).ShouldNot(Receive(Not(BeNil())))
				})
				It("can check the receiver info", func() {
					receiverInfo, err := receiver.ReceiverInfo()
					Expect(err).ToNot(HaveOccurred())
					Expect(receiverInfo).ToNot(BeNil())
					resourceInfo := receiverInfo.GetResourceInfo()
					Expect(resourceInfo).ToNot(BeNil())
					Expect(resourceInfo.GetName()).To(Equal(queueName))
					Expect(resourceInfo.IsDurable()).To(Equal(true))
				})
				It("fails to remove management subscriptions on an existing queue", func() {
					err := receiver.RemoveSubscription(resource.TopicSubscriptionOf(topicString))
					helpers.ValidateNativeError(err, subcode.PermissionNotAllowed)
					subscriptions := helpers.GetQueueSubscriptions(queueName)
					Expect(len(subscriptions)).To(Equal(1))
					Expect(subscriptions).To(ContainElement(topicString))
				})
				It("fails to add the queue topic subscription to the queue", func() {
					err := receiver.AddSubscription(resource.TopicSubscriptionOf("#P2P/QUE/v:solbroker/" + queueName))
					helpers.ValidateNativeError(err, subcode.PermissionNotAllowed)
				})
				It("fails to remove the queue topic subscription from the queue", func() {
					err := receiver.RemoveSubscription(resource.TopicSubscriptionOf("#P2P/QUE/v:solbroker/" + queueName))
					helpers.ValidateNativeError(err, subcode.PermissionNotAllowed)
				})
				It("fails to add an invalid subscription", func() {
					err := receiver.AddSubscription(&myCustomSubscription{})
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
				It("fails to add an invalid subscription asynchronously", func() {
					callbackCalled := make(chan struct{})
					err := receiver.AddSubscriptionAsync(&myCustomSubscription{},
						func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
							close(callbackCalled)
						})
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
					Consistently(callbackCalled).ShouldNot(BeClosed())
				})
				It("fails to remove an invalid subscription", func() {
					err := receiver.RemoveSubscription(&myCustomSubscription{})
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
				It("fails to remove an invalid subscription asynchronously", func() {
					callbackCalled := make(chan struct{})
					err := receiver.RemoveSubscriptionAsync(&myCustomSubscription{},
						func(subscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
							close(callbackCalled)
						})
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
					Consistently(callbackCalled).ShouldNot(BeClosed())
				})
				It("fails to register a nil callback", func() {
					err := receiver.ReceiveAsync(nil)
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
				It("fails to swap the callback after terminated", func() {
					helpers.PublishOneMessage(messagingService, topicString)
					blocker := make(chan struct{})
					Expect(receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
						<-blocker
					})).ToNot(HaveOccurred())
					terminateChannel := receiver.TerminateAsync(10 * time.Second)
					// Allow the terminate call to start processing
					time.Sleep(500 * time.Millisecond)
					err := receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {})
					helpers.ValidateError(err, &solace.IllegalStateError{})
					close(blocker)
					Eventually(terminateChannel).Should(Receive(BeNil()))
				})
				It("can swap the callback after already being set", func() {
					const (
						one = "one"
						two = "two"
					)
					helpers.PublishOneMessage(messagingService, topicString, one)
					firstMsgChan := make(chan message.InboundMessage, 1)
					secondMsgChan := make(chan message.InboundMessage, 1)
					Expect(receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
						firstMsgChan <- inboundMessage
					})).ToNot(HaveOccurred())

					var msg message.InboundMessage
					Eventually(firstMsgChan).Should(Receive(&msg))
					payload, ok := msg.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(payload).To(Equal(one))

					helpers.PublishOneMessage(messagingService, topicString, two)
					Expect(receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
						secondMsgChan <- inboundMessage
					})).ToNot(HaveOccurred())

					Eventually(secondMsgChan).Should(Receive(&msg))
					payload, ok = msg.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(payload).To(Equal(two))

					Consistently(firstMsgChan).ShouldNot(Receive())
				})
				It("fails to acknowledge a nil message", func() {
					err := receiver.Ack(nil)
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
				It("fails to acknowledge a direct message", func() {
					const topic = "direct-message-ack"
					directMsgChan := helpers.ReceiveOneMessage(messagingService, topic)
					helpers.PublishOneMessage(messagingService, topic)
					var directMsg message.InboundMessage
					Eventually(directMsgChan).Should(Receive(&directMsg))
					err := receiver.Ack(directMsg)
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
				DescribeTable("fails to settle a direct message as ", func(outcome config.MessageSettlementOutcome) {
					const topic = "direct-message-ack"
					directMsgChan := helpers.ReceiveOneMessage(messagingService, topic)
					helpers.PublishOneMessage(messagingService, topic)
					var directMsg message.InboundMessage
					Eventually(directMsgChan).Should(Receive(&directMsg))
					err := receiver.Settle(directMsg, outcome) // should fail to settle message
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				},
					Entry("accepted", config.PersistentReceiverAcceptedOutcome),
					Entry("rejected", config.PersistentReceiverRejectedOutcome),
					Entry("failed", config.PersistentReceiverFailedOutcome),
				)
				It("fails to settle a direct message as garbage", func() {
					const topic = "direct-message-ack"
					directMsgChan := helpers.ReceiveOneMessage(messagingService, topic)
					helpers.PublishOneMessage(messagingService, topic)
					var directMsg message.InboundMessage
					Eventually(directMsgChan).Should(Receive(&directMsg))
					// I don't know why this compiles, but the Entry version doesn't.
					err := receiver.Settle(directMsg, "garbage")
					helpers.ValidateError(err, &solace.IllegalArgumentError{})
				})
			})

			const numQueuedMessages = 5000
			Context(fmt.Sprintf("with %d queued messages", numQueuedMessages), func() {
				BeforeEach(func() {
					helpers.PublishNPersistentMessages(messagingService, topicString, numQueuedMessages)
					Eventually(func() int {
						resp, _, err := testcontext.SEMP().Monitor().QueueApi.GetMsgVpnQueueMsgs(testcontext.SEMP().MonitorCtx(),
							testcontext.Messaging().VPN, queueName, nil)
						Expect(err).ToNot(HaveOccurred())
						return int(resp.Meta.Count)
					}).Should(Equal(numQueuedMessages))
				})

				It("receives all messages when connecting to a queue with spooled messages", func() {
					receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
					Expect(receiver.Start()).ToNot(HaveOccurred())
					for i := 0; i < numQueuedMessages; i++ {
						msg, err := receiver.ReceiveMessage(1 * time.Second)
						Expect(err).ToNot(HaveOccurred())
						Expect(msg).ToNot(BeNil())
						corrID, ok := msg.GetCorrelationID()
						Expect(ok).To(BeTrue())
						Expect(corrID).To(Equal(fmt.Sprint(i)))
						Expect(receiver.Ack(msg)).ToNot(HaveOccurred())
					}
				})

				It("receives all messages when connecting to a queue with spooled messages with receive async added later", func() {
					receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
					Expect(receiver.Start()).ToNot(HaveOccurred())
					// We want to assert that we get at least the buffer size of messages received
					Eventually(func() uint64 {
						return messagingService.Metrics().GetValue(metrics.PersistentMessagesReceived)
					}).Should(BeNumerically(">=", 50))
					// and less than the buffer size plus the max window size, assuming we have paused message receiption
					Consistently(func() uint64 {
						return messagingService.Metrics().GetValue(metrics.PersistentMessagesReceived)
					}).Should(BeNumerically("<=", 50+255))
					messagesReceived := make(chan message.InboundMessage, numQueuedMessages)
					Expect(receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
						messagesReceived <- inboundMessage
					}))
					for i := 0; i < numQueuedMessages; i++ {
						var inboundMessage message.InboundMessage
						Eventually(messagesReceived).Should(Receive(&inboundMessage))
						corrID, ok := inboundMessage.GetCorrelationID()
						Expect(ok).To(BeTrue())
						Expect(corrID).To(Equal(fmt.Sprint(i)))
						Expect(receiver.Ack(inboundMessage)).ToNot(HaveOccurred())
					}
				})
			})

			DescribeTable("Activation Passivation",
				func(configFunc func(builder solace.PersistentMessageReceiverBuilder, listener solace.ReceiverStateChangeListener)) {
					receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
					var stateChangeListener solace.ReceiverStateChangeListener
					configFunc(receiverBuilder, func(oldState, newState solace.ReceiverState, timestamp time.Time) {
						stateChangeListener(oldState, newState, timestamp)
					})

					// First event should be an activation event
					startupCalled := make(chan struct{})
					var startupTime time.Time
					stateChangeListener = func(oldState, newState solace.ReceiverState, timestamp time.Time) {
						defer GinkgoRecover()
						Expect(oldState).To(Equal(solace.ReceiverPassive))
						Expect(newState).To(Equal(solace.ReceiverActive))
						Expect(timestamp).To(BeTemporally(">=", startupTime))
						Expect(timestamp).To(BeTemporally("<=", time.Now()))
						close(startupCalled)
					}

					receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
					Expect(err).ToNot(HaveOccurred())

					startupTime = time.Now()
					Expect(receiver.Start()).ToNot(HaveOccurred())
					Eventually(startupCalled).Should(BeClosed())

					// Once we disable egress, we should get a passivation event
					passiveCalled := make(chan struct{})
					var passiveTimestamp time.Time
					stateChangeListener = func(oldState, newState solace.ReceiverState, timestamp time.Time) {
						defer GinkgoRecover()
						Expect(oldState).To(Equal(solace.ReceiverActive))
						Expect(newState).To(Equal(solace.ReceiverPassive))
						Expect(timestamp).To(BeTemporally(">=", passiveTimestamp))
						Expect(timestamp).To(BeTemporally("<=", time.Now()))
						close(passiveCalled)
					}

					passiveTimestamp = time.Now()
					testcontext.SEMP().Config().QueueApi.UpdateMsgVpnQueue(
						testcontext.SEMP().ConfigCtx(),
						sempconfig.MsgVpnQueue{
							EgressEnabled: helpers.False,
						},
						testcontext.Messaging().VPN,
						queueName,
						nil,
					)
					Eventually(passiveCalled).Should(BeClosed())

					// Once we enable egress, we should get an activation event
					activeCalled := make(chan struct{})
					var activeTimestamp time.Time
					stateChangeListener = func(oldState, newState solace.ReceiverState, timestamp time.Time) {
						defer GinkgoRecover()
						Expect(oldState).To(Equal(solace.ReceiverPassive))
						Expect(newState).To(Equal(solace.ReceiverActive))
						Expect(timestamp).To(BeTemporally(">=", activeTimestamp))
						Expect(timestamp).To(BeTemporally("<=", time.Now()))
						close(activeCalled)
					}

					activeTimestamp = time.Now()
					testcontext.SEMP().Config().QueueApi.UpdateMsgVpnQueue(
						testcontext.SEMP().ConfigCtx(),
						sempconfig.MsgVpnQueue{
							EgressEnabled: helpers.True,
						},
						testcontext.Messaging().VPN,
						queueName,
						nil,
					)
					Eventually(activeCalled, 5*time.Second).Should(BeClosed())

					Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
				},
				Entry("from builder function", func(builder solace.PersistentMessageReceiverBuilder, listener solace.ReceiverStateChangeListener) {
					builder.WithActivationPassivationSupport(listener)
				}),
				Entry("from config entry", func(builder solace.PersistentMessageReceiverBuilder, listener solace.ReceiverStateChangeListener) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentStateChangeListener: listener,
					})
				}),
			)

			Describe("Selectors", func() {
				var publisher solace.PersistentMessagePublisher
				BeforeEach(func() {
					publisher = helpers.NewPersistentPublisher(messagingService)
					Expect(publisher.Start()).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					if !publisher.IsTerminated() {
						Expect(publisher.Terminate(0)).ToNot(HaveOccurred())
					}
				})

				DescribeTable("Empty Selector",
					func(configFunc func(solace.PersistentMessageReceiverBuilder)) {
						receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
						configFunc(receiverBuilder)
						receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
						Expect(err).ToNot(HaveOccurred())
						Expect(receiver.Start()).ToNot(HaveOccurred())
						helpers.PublishOnePersistentMessage(messagingService, topicString)
						msg, err := receiver.ReceiveMessage(10 * time.Second)
						Expect(err).ToNot(HaveOccurred())
						Expect(msg).ToNot(BeNil())
					},
					Entry("with no configured selector", func(solace.PersistentMessageReceiverBuilder) {}),
					Entry("nil selector in config", func(builder solace.PersistentMessageReceiverBuilder) {
						builder.FromConfigurationProvider(config.ReceiverPropertyMap{
							config.ReceiverPropertyPersistentMessageSelectorQuery: nil,
						})
					}),
					Entry("empty selector in config", func(builder solace.PersistentMessageReceiverBuilder) {
						builder.FromConfigurationProvider(config.ReceiverPropertyMap{
							config.ReceiverPropertyPersistentMessageSelectorQuery: "",
						})
					}),
					Entry("empty selector in builder func", func(builder solace.PersistentMessageReceiverBuilder) {
						builder.WithMessageSelector("")
					}),
				)

				selectorFunctions := map[string]func(solace.PersistentMessageReceiverBuilder, string){
					"WithMessageSelector": func(pmrb solace.PersistentMessageReceiverBuilder, s string) {
						pmrb.WithMessageSelector(s)
					},
					"FromConfigurationProvider": func(pmrb solace.PersistentMessageReceiverBuilder, s string) {
						pmrb.FromConfigurationProvider(config.ReceiverPropertyMap{
							config.ReceiverPropertyPersistentMessageSelectorQuery: s,
						})
					},
				}

				for selectorFunctionName, selectorFunctionRef := range selectorFunctions {
					selectorFunction := selectorFunctionRef
					Context("with selector configuration using "+selectorFunctionName, func() {
						DescribeTable("Valid selector",
							func(selector string, validConfigs, invalidConfigs []config.MessagePropertyMap) {
								receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
								selectorFunction(receiverBuilder, selector)
								receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
								Expect(err).ToNot(HaveOccurred())
								Expect(receiver.Start()).ToNot(HaveOccurred())
								for i, config := range validConfigs {
									payloadString := fmt.Sprintf("msg %d", i)
									Expect(
										publisher.PublishAwaitAcknowledgement(helpers.NewMessage(messagingService, payloadString),
											resource.TopicOf(topicString), 10*time.Second, config),
									).ToNot(HaveOccurred())
									inboundMsg, err := receiver.ReceiveMessage(500 * time.Millisecond)
									Expect(err).ToNot(HaveOccurred())
									payload, ok := inboundMsg.GetPayloadAsString()
									Expect(ok).To(BeTrue())
									Expect(payload).To(Equal(payloadString))
								}
								for i, config := range invalidConfigs {
									payloadString := fmt.Sprintf("msg %d", i)
									Expect(
										publisher.PublishAwaitAcknowledgement(helpers.NewMessage(messagingService, payloadString),
											resource.TopicOf(topicString), 10*time.Second, config),
									).ToNot(HaveOccurred())
									inboundMsg, err := receiver.ReceiveMessage(500 * time.Millisecond)
									Expect(inboundMsg).To(BeNil())
									helpers.ValidateError(err, &solace.TimeoutError{})
								}
							},
							func(selector string, validConfigs, invalidConfigs []config.MessagePropertyMap) string {
								return "With selector '" + selector + "'"
							},
							Entry(nil, "JMSCorrelationID = '1'",
								[]config.MessagePropertyMap{{config.MessagePropertyCorrelationID: "1"}},
								[]config.MessagePropertyMap{{config.MessagePropertyCorrelationID: "2"}},
							),
							Entry(nil, "JMSCorrelationID = '1' and JMSPriority = 1",
								[]config.MessagePropertyMap{{config.MessagePropertyCorrelationID: "1", config.MessagePropertyPriority: 1}},
								[]config.MessagePropertyMap{{config.MessagePropertyCorrelationID: "1", config.MessagePropertyPriority: 2},
									{config.MessagePropertyCorrelationID: "2", config.MessagePropertyPriority: 1}},
							),
							Entry(nil, "JMSCorrelationID = '1' or JMSCorrelationID = '2'",
								[]config.MessagePropertyMap{{config.MessagePropertyCorrelationID: "1"}, {config.MessagePropertyCorrelationID: "2"}},
								[]config.MessagePropertyMap{{config.MessagePropertyCorrelationID: "3"}},
							),
							Entry(nil, "JMSCorrelationID = '1' and JMSPriority = 1 or JMSCorrelationID = '2' and JMSPriority = 1",
								[]config.MessagePropertyMap{{config.MessagePropertyCorrelationID: "1", config.MessagePropertyPriority: 1}, {config.MessagePropertyCorrelationID: "2", config.MessagePropertyPriority: 1}},
								[]config.MessagePropertyMap{{config.MessagePropertyCorrelationID: "1", config.MessagePropertyPriority: 2}, {config.MessagePropertyCorrelationID: "2", config.MessagePropertyPriority: 2}, {config.MessagePropertyCorrelationID: "3", config.MessagePropertyPriority: 1}},
							),
						)

						DescribeTable("invalid selectors",
							func(selector string) {
								receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
								selectorFunction(receiverBuilder, selector)
								receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
								Expect(err).ToNot(HaveOccurred())
								helpers.ValidateNativeError(receiver.Start(), subcode.InvalidSelector)
							},
							Entry(nil, "this isn't a valid selector is it?"),
						)
					})

				}

			})

			DescribeTable("Auto Acknowledgement",
				func(configFunc func(solace.PersistentMessageReceiverBuilder), expectAutoAck bool, useReceiveAsync bool) {
					receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
					configFunc(receiverBuilder)
					receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
					Expect(err).ToNot(HaveOccurred())
					Expect(receiver.Start()).ToNot(HaveOccurred())

					Expect(helpers.GetQueueMessages(queueName)).To(HaveLen(0))
					messagingService.Metrics().Reset()

					var msg message.InboundMessage
					if useReceiveAsync {
						msgChan := make(chan message.InboundMessage, 1)
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							defer GinkgoRecover()
							Expect(helpers.GetQueueMessages(queueName)).To(HaveLen(1))
							msgChan <- inboundMessage
						})
						helpers.PublishOnePersistentMessage(messagingService, topicString)
						Eventually(msgChan).Should(Receive(&msg))
					} else {
						helpers.PublishOnePersistentMessage(messagingService, topicString)
						inboundMessage, err := receiver.ReceiveMessage(10 * time.Second)
						Expect(err).ToNot(HaveOccurred())
						msg = inboundMessage
					}

					if expectAutoAck {
						Eventually(func() []monitor.MsgVpnQueueMsg {
							return helpers.GetQueueMessages(queueName)
						}, 2*time.Second).Should(HaveLen(0))
					} else {
						Consistently(func() []monitor.MsgVpnQueueMsg {
							return helpers.GetQueueMessages(queueName)
						}, 2*time.Second).Should(HaveLen(1))
					}
					// We expect Ack to act idempotently, not further increasing the ack count on auto ack, but
					// acknowledging the message on ack
					err = receiver.Ack(msg)
					Expect(err).ToNot(HaveOccurred())
					Eventually(func() []monitor.MsgVpnQueueMsg {
						return helpers.GetQueueMessages(queueName)
					}, 2*time.Second).Should(HaveLen(0))
				},
				// No ack strategy, expect default to client ack
				Entry("No Ack Strategy with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {}, false, false),
				Entry("No Ack Strategy with async receive", func(builder solace.PersistentMessageReceiverBuilder) {}, false, true),
				// Explicit client ack through builder
				Entry("Builder Client Ack with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithMessageClientAcknowledgement()
				}, false, false),
				Entry("Builder Client Ack with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithMessageClientAcknowledgement()
				}, false, true),
				// Explicit client ack through property
				Entry("Property Client Ack with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageAckStrategy: config.PersistentReceiverClientAck,
					})
				}, false, false),
				Entry("Property Client Ack with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageAckStrategy: config.PersistentReceiverClientAck,
					})
				}, false, true),
				// Nil ack through property
				Entry("Property Client Ack with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageAckStrategy: nil,
					})
				}, false, false),
				Entry("Property Client Ack with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageAckStrategy: nil,
					})
				}, false, true),
				// Explicit auto ack through builder
				Entry("Builder Auto Ack with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithMessageAutoAcknowledgement()
				}, true, false),
				Entry("Builder Auto Ack with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithMessageAutoAcknowledgement()
				}, true, true),
				// Explicit auto ack through property
				Entry("Property Auto Ack with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageAckStrategy: config.PersistentReceiverAutoAck,
					})
				}, true, false),
				Entry("Property Auto Ack with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageAckStrategy: config.PersistentReceiverAutoAck,
					})
				}, true, true),
			)

			It("does not auto acknowledge a message with a panicing callback", func() {
				receiver, err := messagingService.
					CreatePersistentMessageReceiverBuilder().
					WithMessageAutoAcknowledgement().
					Build(resource.QueueDurableExclusive(queueName))
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())

				Expect(helpers.GetQueueMessages(queueName)).To(HaveLen(0))

				formatString := "msg %d"
				helpers.PublishNPersistentMessages(messagingService, topicString, 2, formatString)

				msgChan := make(chan message.InboundMessage, 1)
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					msgChan <- inboundMessage
					panic("uh oh I'm panicing!!!")
				})
				var msg message.InboundMessage
				Eventually(msgChan).Should(Receive(&msg))
				payload, ok := msg.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				// expect the payload to be the first message
				Expect(payload).To(Equal(fmt.Sprintf(formatString, 0)))
				// should be able to receive more messages
				Eventually(msgChan).Should(Receive(&msg))
				payload, ok = msg.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				// Still expect the messages to not be dequeued
				Consistently(func() []monitor.MsgVpnQueueMsg {
					return helpers.GetQueueMessages(queueName)
				}, 2*time.Second).Should(HaveLen(2))
				// expect the payload to be the second message
				Expect(payload).To(Equal(fmt.Sprintf(formatString, 1)))
				// should also be able to shut down properly
				Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			for _, autoAck := range []bool{true, false} {
				autoAckText := ""
				// ??? I don't speak go, so I'm just rolling with it.
				shouldAutoAck := autoAck
				if autoAck {
					autoAckText = " withAutoAck"
				}
				DescribeTable("Happy cases for outcome configuration on ReceiverBuilder"+autoAckText,
					func(configFunc func(solace.PersistentMessageReceiverBuilder), outcome config.MessageSettlementOutcome, autoAckConfig bool) {
						receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
						configFunc(receiverBuilder)
						if autoAckConfig {
							receiverBuilder.WithMessageAutoAcknowledgement()
						}
						receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
						Expect(err).ToNot(HaveOccurred())
						Expect(receiver.Start()).ToNot(HaveOccurred())
						Expect(helpers.GetQueueMessages(queueName)).To(HaveLen(0))
						messagingService.Metrics().Reset()
						Expect(err).ToNot(HaveOccurred())

						helpers.PublishOnePersistentMessage(messagingService, topicString)
						msg, err := receiver.ReceiveMessage(10 * time.Second)
						Expect(err).ToNot(HaveOccurred())

						err = receiver.Settle(msg, outcome)
						Expect(err).ToNot(HaveOccurred())
						/* if (autoAckConfig) {
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesAccepted, 0)
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesRejected, 0)
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesFailed, 0)
						} else */if outcome == config.PersistentReceiverAcceptedOutcome {
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesAccepted, 1)
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesRejected, 0)
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesFailed, 0)
						} else if outcome == config.PersistentReceiverRejectedOutcome {
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesAccepted, 0)
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesRejected, 1)
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesFailed, 0)
						} else if outcome == config.PersistentReceiverFailedOutcome {
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesAccepted, 0)
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesRejected, 0)
							helpers.ValidateMetric(messagingService, metrics.PersistentMessagesFailed, 1)
						}
						// I guess this drains the queue?
						if !autoAckConfig && outcome == config.PersistentReceiverFailedOutcome {
							Consistently(
								func() []monitor.MsgVpnQueueMsg {
									return helpers.GetQueueMessages(queueName)
								},
								2*time.Second).Should(HaveLen(1))
						}
					},
					Entry(
						"Default can Accept", func(builder solace.PersistentMessageReceiverBuilder) {},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Accept via setter", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverAcceptedOutcome)
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Accept & Fail via setter can accept", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverAcceptedOutcome)
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Accept & Fail via setter can fail", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverAcceptedOutcome)
						},
						config.PersistentReceiverFailedOutcome, shouldAutoAck),
					Entry(
						"Fail via setter can accept", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome)
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Fail via setter can fail", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome)
						},
						config.PersistentReceiverFailedOutcome, shouldAutoAck),
					Entry(
						"Accept via property", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s", config.PersistentReceiverAcceptedOutcome),
							})
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Fail via property can accept", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s", config.PersistentReceiverFailedOutcome),
							})
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Fail via property can fail", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s", config.PersistentReceiverFailedOutcome),
							})
						},
						config.PersistentReceiverFailedOutcome, shouldAutoAck),
					Entry(
						"Accept & Fail via property can accept", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverAcceptedOutcome),
							})
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Accept & Fail via property can fail", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverAcceptedOutcome),
							})
						},
						config.PersistentReceiverFailedOutcome, shouldAutoAck),

					Entry(
						"Accept & Reject via setter can accept", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverRejectedOutcome, config.PersistentReceiverAcceptedOutcome)
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Accept & Reject via setter can reject", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverRejectedOutcome, config.PersistentReceiverAcceptedOutcome)
						},
						config.PersistentReceiverRejectedOutcome, shouldAutoAck),
					Entry(
						"Reject via setter can accept", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverRejectedOutcome)
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Reject via setter can reject", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverRejectedOutcome)
						},
						config.PersistentReceiverRejectedOutcome, shouldAutoAck),
					Entry(
						"Reject via property can accept", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s", config.PersistentReceiverRejectedOutcome),
							})
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Reject via property can reject", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s", config.PersistentReceiverRejectedOutcome),
							})
						},
						config.PersistentReceiverRejectedOutcome, shouldAutoAck),
					Entry(
						"Accept & Reject via property can accept", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverRejectedOutcome, config.PersistentReceiverAcceptedOutcome),
							})
						},
						config.PersistentReceiverAcceptedOutcome, shouldAutoAck),
					Entry(
						"Accept & Reject via property can reject", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverRejectedOutcome, config.PersistentReceiverAcceptedOutcome),
							})
						},
						config.PersistentReceiverRejectedOutcome, shouldAutoAck),
				) // describe
			} // for

			// Config time fail cases

			// I don't think the setter should accept garbage.
			It("Garbage to setter", func() {
				receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
				receiverBuilder.WithRequiredMessageOutcomeSupport("garbage")
				receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				Expect(receiver).To(BeNil())
			})
			It("Legit outcome mixed with garbage to setter", func() {
				receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
				receiverBuilder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, "garbage")
				receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				Expect(receiver).To(BeNil())
			})
			It("Legit outcome mixed with garbage to setter backwards", func() {
				receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
				receiverBuilder.WithRequiredMessageOutcomeSupport("garbage", config.PersistentReceiverFailedOutcome)
				receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				Expect(receiver).To(BeNil())
			})
			It("Garbage as property", func() {
				receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
				receiverBuilder.FromConfigurationProvider(config.ReceiverPropertyMap{
					config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: "garbage",
				})
				receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				Expect(receiver).To(BeNil())
			})
			It("Garbage mixed in as property", func() {
				receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
				receiverBuilder.FromConfigurationProvider(config.ReceiverPropertyMap{
					config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,garbage", config.PersistentReceiverRejectedOutcome),
				})
				receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				Expect(receiver).To(BeNil())
			})
			It("Poor punctuation in property", func() {
				receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
				receiverBuilder.FromConfigurationProvider(config.ReceiverPropertyMap{
					config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s %s", config.PersistentReceiverRejectedOutcome, config.PersistentReceiverAcceptedOutcome),
				})
				receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				Expect(receiver).To(BeNil())
			})

			// Settle() time fail cases

			DescribeTable("Message Settlement Outcome with Client Ack Configured",
				func(configFunc func(solace.PersistentMessageReceiverBuilder), outcome config.MessageSettlementOutcome, expectNackSupport bool, useReceiveAsync bool) {
					receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
					receiverBuilder.WithMessageClientAcknowledgement() // with Client- Ack configured (the default)
					configFunc(receiverBuilder)
					receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
					Expect(err).ToNot(HaveOccurred())
					Expect(receiver.Start()).ToNot(HaveOccurred())

					Expect(helpers.GetQueueMessages(queueName)).To(HaveLen(0))
					messagingService.Metrics().Reset()

					var msg message.InboundMessage
					if useReceiveAsync {
						msgChan := make(chan message.InboundMessage, 1)
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							defer GinkgoRecover()
							Expect(helpers.GetQueueMessages(queueName)).To(HaveLen(1))
							msgChan <- inboundMessage
						})
						helpers.PublishOnePersistentMessage(messagingService, topicString)
						Eventually(msgChan).Should(Receive(&msg))
					} else {
						helpers.PublishOnePersistentMessage(messagingService, topicString)
						inboundMessage, err := receiver.ReceiveMessage(10 * time.Second)
						Expect(err).ToNot(HaveOccurred())
						msg = inboundMessage
					}

					err = receiver.Settle(msg, outcome)
					// message should still be in the queue - for FAILED settlement outcome
					if expectNackSupport && outcome == config.PersistentReceiverFailedOutcome {
						Expect(err).ToNot(HaveOccurred())
						Consistently(func() []monitor.MsgVpnQueueMsg {
							return helpers.GetQueueMessages(queueName)
						}, 2*time.Second).Should(HaveLen(1))
					} else if expectNackSupport {
						// should be removed from the queue - for ACCEPTED and REJECTED settlement outcomes
						Expect(err).ToNot(HaveOccurred())
						Eventually(func() []monitor.MsgVpnQueueMsg {
							return helpers.GetQueueMessages(queueName)
						}, 2*time.Second).Should(HaveLen(0))
					} else {
						Expect(err).To(HaveOccurred())
					}
				},
				// Message settlement not enabled on flow, expect error while caling settle() with Nack
				Entry(
					"RejectedOutcome - NACK not supported on flow sync receive", func(builder solace.PersistentMessageReceiverBuilder) {},
					config.PersistentReceiverRejectedOutcome,
					false, false),
				Entry(
					"FailedOutcome - NACK not supported on flow sync receive", func(builder solace.PersistentMessageReceiverBuilder) {},
					config.PersistentReceiverFailedOutcome,
					false, false),
				Entry(
					"RejectedOutcome - NACK not supported on flow with async receive", func(builder solace.PersistentMessageReceiverBuilder) {},
					config.PersistentReceiverRejectedOutcome,
					false, true),
				Entry(
					"FailedOutcome - NACK not supported on flow with async receive", func(builder solace.PersistentMessageReceiverBuilder) {},
					config.PersistentReceiverFailedOutcome,
					false, true),

				// Nil Nack support through property - expected Nack to not be supported on flow
				Entry("Property NACK not supported with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: nil,
					})
				}, config.PersistentReceiverRejectedOutcome, false, false),
				Entry("Property NACK not supported with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: nil,
					})
				}, config.PersistentReceiverRejectedOutcome, false, true),

				// With configured Nack support through builder
				Entry("RejectedOutcome - Builder Nack support with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome)
				}, config.PersistentReceiverRejectedOutcome, true, false),
				Entry("FailedOutcome - Builder Nack support with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome)
				}, config.PersistentReceiverFailedOutcome, true, false),
				Entry("RejectedOutcome - Builder Nack support with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome)
				}, config.PersistentReceiverRejectedOutcome, true, true),
				Entry("FailedOutcome - Builder Nack support with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome)
				}, config.PersistentReceiverFailedOutcome, true, true),

				// With configured Nack support through property
				Entry("RejectedOutcome - Property Nack support with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome),
					})
				}, config.PersistentReceiverRejectedOutcome, true, false),
				Entry("FailedOutcome - Property Nack support with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome),
					})
				}, config.PersistentReceiverFailedOutcome, true, false),
				Entry("RejectedOutcome - Property Nack support with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome),
					})
				}, config.PersistentReceiverRejectedOutcome, true, true),
				Entry("FailedOutcome - Property Nack support with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome),
					})
				}, config.PersistentReceiverFailedOutcome, true, true),
			)

			DescribeTable("Message Settlement Outcome with Auto Ack Configured",
				func(configFunc func(solace.PersistentMessageReceiverBuilder), outcome config.MessageSettlementOutcome, useReceiveAsync bool) {
					receiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
					receiverBuilder.WithMessageAutoAcknowledgement() // with Auto- Ack configured
					configFunc(receiverBuilder)
					receiver, err := receiverBuilder.Build(resource.QueueDurableExclusive(queueName))
					Expect(err).ToNot(HaveOccurred())
					Expect(receiver.Start()).ToNot(HaveOccurred())

					Expect(helpers.GetQueueMessages(queueName)).To(HaveLen(0))
					messagingService.Metrics().Reset()

					var msg message.InboundMessage
					if useReceiveAsync {
						msgChan := make(chan message.InboundMessage, 1)
						receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							defer GinkgoRecover()
							Expect(helpers.GetQueueMessages(queueName)).To(HaveLen(1))
							msgChan <- inboundMessage
						})
						helpers.PublishOnePersistentMessage(messagingService, topicString)
						Eventually(msgChan).Should(Receive(&msg))
					} else {
						helpers.PublishOnePersistentMessage(messagingService, topicString)
						inboundMessage, err := receiver.ReceiveMessage(10 * time.Second)
						Expect(err).ToNot(HaveOccurred())
						msg = inboundMessage
					}

					// no message should be in the queue since already Auto-Acked
					err = receiver.Settle(msg, outcome)
					// we don't expect any errors when calling settle() since message not founf
					Expect(err).ToNot(HaveOccurred())
					Eventually(func() []monitor.MsgVpnQueueMsg {
						return helpers.GetQueueMessages(queueName)
					}, 2*time.Second).Should(HaveLen(0))
				},
				// With configured Nack support through builder
				Entry("RejectedOutcome - Builder Nack support with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome)
				}, config.PersistentReceiverRejectedOutcome, false),
				Entry("FailedOutcome - Builder Nack support with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome)
				}, config.PersistentReceiverFailedOutcome, false),
				Entry("RejectedOutcome - Builder Nack support with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome)
				}, config.PersistentReceiverRejectedOutcome, true),
				Entry("FailedOutcome - Builder Nack support with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.WithRequiredMessageOutcomeSupport(config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome)
				}, config.PersistentReceiverFailedOutcome, true),

				// With configured Nack support through property
				Entry("RejectedOutcome - Property Nack support with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome),
					})
				}, config.PersistentReceiverRejectedOutcome, false),
				Entry("FailedOutcome - Property Nack support with sync receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome),
					})
				}, config.PersistentReceiverFailedOutcome, false),
				Entry("RejectedOutcome - Property Nack support with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome),
					})
				}, config.PersistentReceiverRejectedOutcome, true),
				Entry("FailedOutcome - Property Nack support with async receive", func(builder solace.PersistentMessageReceiverBuilder) {
					builder.FromConfigurationProvider(config.ReceiverPropertyMap{
						config.ReceiverPropertyPersistentMessageRequiredOutcomeSupport: fmt.Sprintf("%s,%s", config.PersistentReceiverFailedOutcome, config.PersistentReceiverRejectedOutcome),
					})
				}, config.PersistentReceiverFailedOutcome, true),
			)

			Context("with an ACL deny exception", func() {
				const deniedTopic = "persistent-receiver-acl-deny"
				BeforeEach(func() {
					helpers.AddSubscriptionTopicException(deniedTopic)
				})

				AfterEach(func() {
					helpers.RemoveSubscriptionTopicException(deniedTopic)
				})

				It("fails to subscribe when starting the receiver", func() {
					persistentReceiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
						WithSubscriptions(resource.TopicSubscriptionOf(deniedTopic)).
						Build(resource.QueueDurableExclusive(queueName))
					Expect(err).ToNot(HaveOccurred())
					err = persistentReceiver.Start()
					helpers.ValidateNativeError(err, subcode.SubscriptionAclDenied)
				})

				DescribeTable("runtime rubscription activities",
					func(subscribeFunc func(receiver solace.PersistentMessageReceiver,
						topicSubscription *resource.TopicSubscription) error) {
						persistentReceiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
							Build(resource.QueueDurableExclusive(queueName))
						Expect(err).ToNot(HaveOccurred())
						err = persistentReceiver.Start()
						Expect(err).ToNot(HaveOccurred())
						err = subscribeFunc(persistentReceiver, resource.TopicSubscriptionOf(deniedTopic))
						helpers.ValidateNativeError(err, subcode.SubscriptionAclDenied)
						Expect(persistentReceiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
					},
					Entry("Synchronous Subscribe", func(receiver solace.PersistentMessageReceiver,
						topic *resource.TopicSubscription) error {
						return receiver.AddSubscription(topic)
					}),
					Entry("Synchronous Unsubscribe", func(receiver solace.PersistentMessageReceiver,
						topic *resource.TopicSubscription) error {
						return receiver.RemoveSubscription(topic)
					}),
					Entry("Asynchronous Subscribe", func(receiver solace.PersistentMessageReceiver,
						topic *resource.TopicSubscription) (err error) {
						resultChan, subscriptionChangeListener := helpers.AsyncSubscribeHandler(topic, solace.SubscriptionAdded)
						Expect(receiver.AddSubscriptionAsync(topic, subscriptionChangeListener)).ToNot(HaveOccurred())
						Eventually(resultChan).Should(Receive(&err))
						return err
					}),
					Entry("Asynchronous Unsubscribe", func(receiver solace.PersistentMessageReceiver,
						topic *resource.TopicSubscription) (err error) {
						resultChan, subscriptionChangeListener := helpers.AsyncSubscribeHandler(topic, solace.SubscriptionRemoved)
						Expect(receiver.RemoveSubscriptionAsync(topic, subscriptionChangeListener)).ToNot(HaveOccurred())
						Eventually(resultChan).Should(Receive(&err))
						return err
					}),
				)
			})

			Describe("Replay", func() {
				Context("with a replay log created", func() {
					const replayLogName = "persistentMessagingReplayTests"
					const replaySubscription = "persistentMessagingReplayTest"
					BeforeEach(func() {
						// Setup Replay Log
						testcontext.SEMP().Config().ReplayLogApi.CreateMsgVpnReplayLog(
							testcontext.SEMP().ConfigCtx(),
							sempconfig.MsgVpnReplayLog{
								EgressEnabled:  helpers.True,
								IngressEnabled: helpers.True,
								MaxSpoolUsage:  1000,
								MsgVpnName:     testcontext.Messaging().VPN,
								ReplayLogName:  replayLogName,
							},
							testcontext.Messaging().VPN,
							nil,
						)
						helpers.CreateQueueSubscription(queueName, replaySubscription)
					})
					AfterEach(func() {
						// Delete replay log
						testcontext.SEMP().Config().ReplayLogApi.DeleteMsgVpnReplayLog(
							testcontext.SEMP().ConfigCtx(),
							testcontext.Messaging().VPN,
							replayLogName,
						)
					})

					drainQueue := func(n int) []message.InboundMessage {
						receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().WithMessageAutoAcknowledgement().Build(resource.QueueDurableExclusive(queueName))
						Expect(err).ToNot(HaveOccurred())
						Expect(receiver.Start()).ToNot(HaveOccurred())
						msgs := make([]message.InboundMessage, n)
						for i := 0; i < n; i++ {
							msg, err := receiver.ReceiveMessage(500 * time.Millisecond)
							Expect(err).ToNot(HaveOccurred())
							msgs[i] = msg
						}
						Expect(receiver.Terminate(1 * time.Second)).ToNot(HaveOccurred())
						return msgs
					}

					buildAndValidateReplay := func(messageReceiverBuilder solace.PersistentMessageReceiverBuilder, messageFormat string, numMessages int) {
						messageReceiver, err := messageReceiverBuilder.Build(resource.QueueDurableExclusive(queueName))
						Expect(err).ToNot(HaveOccurred())
						Expect(messageReceiver.Start()).ToNot(HaveOccurred())
						for i := 0; i < numMessages; i++ {
							msg, err := messageReceiver.ReceiveMessage(1 * time.Second)
							Expect(err).ToNot(HaveOccurred(), fmt.Sprintf("Error occurred when waiting for message %d", i))
							payload, ok := msg.GetPayloadAsString()
							Expect(ok).To(BeTrue())
							Expect(payload).To(Equal(fmt.Sprintf(messageFormat, i)))
							Expect(messageReceiver.Ack(msg)).ToNot(HaveOccurred())
						}
						msg, err := messageReceiver.ReceiveMessage(1 * time.Second)
						helpers.ValidateError(err, &solace.TimeoutError{})
						Expect(msg).To(BeNil())
						Expect(messageReceiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
					}

					DescribeTable("All Messages Success",
						func(builderFunc func(solace.PersistentMessageReceiverBuilder)) {
							const messageFormat = "message %d"
							const numMessages = 10
							helpers.PublishNPersistentMessages(messagingService, replaySubscription, numMessages, messageFormat)
							drainQueue(numMessages)
							messageReceiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
							builderFunc(messageReceiverBuilder)
							buildAndValidateReplay(messageReceiverBuilder, messageFormat, numMessages)
						},
						Entry("can start replay with builder function defined replay", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.WithMessageReplay(config.ReplayStrategyAllMessages())
						}),
						Entry("can start replay with configuration defined replay", func(builder solace.PersistentMessageReceiverBuilder) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageReplayStrategy: config.PersistentReplayAll,
							})
						}),
					)

					DescribeTable("Time Based Success",
						func(builderFunc func(solace.PersistentMessageReceiverBuilder, time.Time)) {
							const badMessageFormat = "unexpected %d"
							const messageFormat = "message %d"
							const numMessages = 10
							helpers.PublishNPersistentMessages(messagingService, replaySubscription, numMessages, badMessageFormat)
							drainQueue(numMessages)
							// We want to give it a bit
							time.Sleep(1 * time.Second)
							startTime := time.Now()
							time.Sleep(1 * time.Second)
							helpers.PublishNPersistentMessages(messagingService, replaySubscription, numMessages, messageFormat)
							drainQueue(numMessages)
							messageReceiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
							builderFunc(messageReceiverBuilder, startTime)
							buildAndValidateReplay(messageReceiverBuilder, messageFormat, numMessages)
						},
						Entry("can start replay with builder function defined replay", func(builder solace.PersistentMessageReceiverBuilder, startTime time.Time) {
							builder.WithMessageReplay(config.ReplayStrategyTimeBased(startTime))
						}),
						Entry("can start replay with configuration defined replay using time.Time", func(builder solace.PersistentMessageReceiverBuilder, startTime time.Time) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageReplayStrategy:                   config.PersistentReplayTimeBased,
								config.ReceiverPropertyPersistentMessageReplayStrategyTimeBasedStartTime: startTime,
							})
						}),
						Entry("can start replay with configuration defined replay using unix time", func(builder solace.PersistentMessageReceiverBuilder, startTime time.Time) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageReplayStrategy:                   config.PersistentReplayTimeBased,
								config.ReceiverPropertyPersistentMessageReplayStrategyTimeBasedStartTime: startTime.Unix(),
							})
						}),
						Entry("can start replay with configuration defined replay using time string", func(builder solace.PersistentMessageReceiverBuilder, startTime time.Time) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageReplayStrategy:                   config.PersistentReplayTimeBased,
								config.ReceiverPropertyPersistentMessageReplayStrategyTimeBasedStartTime: startTime.Format("2006-01-02T15:04:05Z07:00"),
							})
						}),
					)

					DescribeTable("Message ID Based Success",
						func(builderFunc func(solace.PersistentMessageReceiverBuilder, rgmid.ReplicationGroupMessageID)) {
							const badMessageFormat = "unexpected %d"
							const messageFormat = "message %d"
							const numMessages = 10
							helpers.PublishNPersistentMessages(messagingService, replaySubscription, numMessages, badMessageFormat)
							// Replay after given message ID, we want the last ID
							msgs := drainQueue(numMessages)
							msgID, ok := msgs[numMessages-1].GetReplicationGroupMessageID()
							helpers.PublishNPersistentMessages(messagingService, replaySubscription, numMessages, messageFormat)
							drainQueue(numMessages)
							Expect(ok).To(BeTrue())
							messageReceiverBuilder := messagingService.CreatePersistentMessageReceiverBuilder()
							builderFunc(messageReceiverBuilder, msgID)
							buildAndValidateReplay(messageReceiverBuilder, messageFormat, numMessages)
						},
						Entry("with builder function defined replay", func(builder solace.PersistentMessageReceiverBuilder, msgId rgmid.ReplicationGroupMessageID) {
							builder.WithMessageReplay(config.ReplayStrategyReplicationGroupMessageID(msgId))
						}),
						Entry("with configuration defined replay using rgmid", func(builder solace.PersistentMessageReceiverBuilder, msgId rgmid.ReplicationGroupMessageID) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageReplayStrategy:                                 config.PersistentReplayIDBased,
								config.ReceiverPropertyPersistentMessageReplayStrategyIDBasedReplicationGroupMessageID: msgId,
							})
						}),
						Entry("with configuration defined replay using string", func(builder solace.PersistentMessageReceiverBuilder, msgId rgmid.ReplicationGroupMessageID) {
							builder.FromConfigurationProvider(config.ReceiverPropertyMap{
								config.ReceiverPropertyPersistentMessageReplayStrategy:                                 config.PersistentReplayIDBased,
								config.ReceiverPropertyPersistentMessageReplayStrategyIDBasedReplicationGroupMessageID: msgId.String(),
							})
						}),
					)

					It("can receive all messages when management triggers replay", func() {
						const messageFormat = "message %d"
						const numMessages = 10
						helpers.PublishNPersistentMessages(messagingService, replaySubscription, numMessages, messageFormat)
						drainQueue(numMessages)
						messageReceiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
							WithMessageAutoAcknowledgement().
							Build(resource.QueueDurableExclusive(queueName))
						Expect(err).ToNot(HaveOccurred())
						messageBuffer := make(chan message.InboundMessage, numMessages)
						messageReceiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
							messageBuffer <- inboundMessage
						})
						Expect(messageReceiver.Start()).ToNot(HaveOccurred())
						Consistently(messageBuffer).ShouldNot(Receive())
						testcontext.SEMP().Action().QueueApi.DoMsgVpnQueueStartReplay(
							testcontext.SEMP().ActionCtx(),
							action.MsgVpnQueueStartReplay{
								ReplayLogName: replayLogName,
							},
							testcontext.Messaging().VPN,
							queueName,
						)
						for i := 0; i < numMessages; i++ {
							var msg message.InboundMessage
							Eventually(messageBuffer).Should(Receive(&msg))
							payload, ok := msg.GetPayloadAsString()
							Expect(ok).To(BeTrue())
							Expect(payload).To(Equal(fmt.Sprintf(messageFormat, i)))
						}
						Expect(messageReceiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
					})

					It("fails to start replay when given a nonexistent RGMID when using strategy", func() {
						rgmid, err := messaging.ReplicationGroupMessageIDOf("rmid1:0d77c-b0b2e66aec0-00000000-00000001")
						Expect(err).ToNot(HaveOccurred())
						receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
							WithMessageReplay(config.ReplayStrategyReplicationGroupMessageID(rgmid)).
							Build(resource.QueueDurableExclusive(queueName))
						Expect(err).ToNot(HaveOccurred())
						err = receiver.Start()
						helpers.ValidateError(err, &solace.MessageReplayError{})
						var wrapped *solace.NativeError
						Expect(errors.As(err, &wrapped)).To(BeTrue())
						helpers.ValidateNativeError(wrapped, subcode.ReplayStartMessageUnavailable)
					})

					It("fails to start replay on a temporary queue", func() {
						receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
							WithMessageReplay(config.ReplayStrategyAllMessages()).
							Build(resource.QueueNonDurableExclusive("tempReplayQueue"))
						Expect(err).ToNot(HaveOccurred())
						err = receiver.Start()
						helpers.ValidateError(err, &solace.MessageReplayError{})
						var wrapped *solace.NativeError
						Expect(errors.As(err, &wrapped)).To(BeTrue())
						helpers.ValidateNativeError(wrapped, subcode.ReplayTemporaryNotSupported, subcode.ReplayAnonymousNotSupported)
					})

				})

				It("fails to build receiver when given a bad strategy", func() {
					const badStrategy = "notastrategy"
					receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
						FromConfigurationProvider(config.ReceiverPropertyMap{
							config.ReceiverPropertyPersistentMessageReplayStrategy: badStrategy,
						}).
						Build(resource.QueueDurableExclusive(queueName))
					helpers.ValidateError(err, &solace.IllegalArgumentError{}, badStrategy)
					Expect(receiver).To(BeNil())
				})

				It("fails to start replay when given a bad time through config", func() {
					receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
						FromConfigurationProvider(config.ReceiverPropertyMap{
							config.ReceiverPropertyPersistentMessageReplayStrategy:                   config.PersistentReplayTimeBased,
							config.ReceiverPropertyPersistentMessageReplayStrategyTimeBasedStartTime: "notatime",
						}).
						Build(resource.QueueDurableExclusive(queueName))
					Expect(err).ToNot(HaveOccurred())
					err = receiver.Start()
					helpers.ValidateError(err, &solace.InvalidConfigurationError{})
				})

				It("fails to start replay with no replay log", func() {
					receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().
						WithMessageReplay(config.ReplayStrategyAllMessages()).
						Build(resource.QueueDurableExclusive(queueName))
					Expect(err).ToNot(HaveOccurred())
					err = receiver.Start()
					helpers.ValidateError(err, &solace.MessageReplayError{})
					var wrapped *solace.NativeError
					Expect(errors.As(err, &wrapped)).To(BeTrue())
					helpers.ValidateNativeError(wrapped, subcode.ReplayDisabled)
				})

			})

		})

	})

	Describe("Ungraceful Termination Tests", func() {
		const topicString = "ungraceful-termination-tests"
		const numMessages = 5
		var messagingService solace.MessagingService
		var messageReceiver solace.PersistentMessageReceiver

		BeforeEach(func() {
			helpers.CreateQueue(queueName, topicString)

			messagingService = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().
				WithReconnectionRetryStrategy(config.RetryStrategyNeverRetry()).
				FromConfigurationProvider(helpers.DefaultConfiguration()))
			Expect(messagingService.Connect()).ToNot(HaveOccurred())

			messageReceiver = helpers.NewPersistentReceiver(messagingService,
				resource.QueueDurableExclusive(queueName))
			Expect(messageReceiver.Start()).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			if messageReceiver.IsRunning() {
				messageReceiver.Terminate(0)
			}
			messagingService.Disconnect()
			helpers.DeleteQueue(queueName)
		})

		terminateFunctions := []time.Duration{
			0, 2 * time.Second,
		}
		for _, terminateDurationRef := range terminateFunctions {
			terminateDuration := terminateDurationRef

			Context("with terminate duration of "+terminateDuration.String(), func() {
				It("terminates ungracefully with undelivered messages", func() {
					helpers.PublishNPersistentMessages(messagingService, topicString, numMessages)
					helpers.ValidateMetric(messagingService, metrics.PersistentMessagesReceived, numMessages)
					err := messageReceiver.Terminate(terminateDuration)
					helpers.ValidateError(err, &solace.IncompleteMessageDeliveryError{}, fmt.Sprintf("%d undelivered", numMessages))
					helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, numMessages)
					msg, err := messageReceiver.ReceiveMessage(1 * time.Second)
					Expect(msg).To(BeNil())
					helpers.ValidateError(err, &solace.IllegalStateError{})
				})

				It("terminates ungracefully with undelivered messages using receive async", func() {
					blocker := make(chan struct{})
					messageChannel := make(chan message.InboundMessage, numMessages+1)
					messageReceiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
						<-blocker
						messageChannel <- inboundMessage
					})
					helpers.PublishNPersistentMessages(messagingService, topicString, numMessages+1)
					helpers.ValidateMetric(messagingService, metrics.PersistentMessagesReceived, numMessages+1)
					errChan := messageReceiver.TerminateAsync(terminateDuration)
					time.Sleep(terminateDuration + 50*time.Millisecond)
					close(blocker)
					helpers.ValidateChannelError(errChan, &solace.IncompleteMessageDeliveryError{}, fmt.Sprintf("%d undelivered", numMessages))
					helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, numMessages)
					Eventually(messageChannel).Should(Receive(Not(BeNil())))
					Consistently(messageChannel).ShouldNot(Receive())
				})
			})
		}
	})

	Describe("Unsolicited Termination Tests", func() {
		var messagingService solace.MessagingService
		var messageReceiver solace.PersistentMessageReceiver
		const queueName = "unsolicitedPersistentTerminationQueue"

		type unsolicitedTerminationContext struct {
			terminateFunction func(messagingService solace.MessagingService,
				messageReceiver solace.PersistentMessageReceiver)
			configuration func() config.ServicePropertyMap
			cleanupFunc   func()
		}
		terminationCases := map[string]unsolicitedTerminationContext{
			"messaging service disconnect": {
				terminateFunction: func(messagingService solace.MessagingService, messageReceiver solace.PersistentMessageReceiver) {
					Expect(messagingService.Disconnect()).ToNot(HaveOccurred())
				},
				configuration: func() config.ServicePropertyMap {
					return helpers.DefaultConfiguration()
				},
			},
			"management disconnect": {
				terminateFunction: func(messagingService solace.MessagingService, messageReceiver solace.PersistentMessageReceiver) {
					helpers.ForceDisconnectViaSEMPv2(messagingService)
				},
				configuration: func() config.ServicePropertyMap {
					return helpers.DefaultConfiguration()
				},
			},
			"toxic disconnect": {
				terminateFunction: func(messagingService solace.MessagingService, messageReceiver solace.PersistentMessageReceiver) {
					testcontext.Toxi().SMF().Disable()
				},
				configuration: func() config.ServicePropertyMap {
					helpers.CheckToxiProxy()
					return helpers.ToxicConfiguration()
				},
				cleanupFunc: func() {
					testcontext.Toxi().ResetProxies()
				},
			},
			"queue delete": {
				terminateFunction: func(messagingService solace.MessagingService, messageReceiver solace.PersistentMessageReceiver) {
					_, _, err := testcontext.SEMP().Config().QueueApi.DeleteMsgVpnQueue(testcontext.SEMP().ConfigCtx(),
						testcontext.Messaging().VPN, queueName)
					Expect(err).ToNot(HaveOccurred())
				},
				configuration: func() config.ServicePropertyMap {
					return helpers.DefaultConfiguration()
				},
			},
		}

		for terminationCaseName, terminationContextRef := range terminationCases {
			terminationConfiguration := terminationContextRef.configuration
			terminationFunction := terminationContextRef.terminateFunction
			terminationCleanup := terminationContextRef.cleanupFunc
			Context("using "+terminationCaseName, func() {
				const topicString = "unsolicited-persistent-terminations"
				const numMessages = 5

				var terminate func()
				var terminationReceived chan struct{}

				BeforeEach(func() {
					helpers.CreateQueue(queueName, topicString)

					messagingService = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().
						WithReconnectionRetryStrategy(config.RetryStrategyNeverRetry()).
						FromConfigurationProvider(terminationConfiguration()))
					Expect(messagingService.Connect()).ToNot(HaveOccurred())

					messageReceiver = helpers.NewPersistentReceiver(messagingService,
						resource.QueueDurableExclusive(queueName))
					Expect(messageReceiver.Start()).ToNot(HaveOccurred())

					terminationReceived = make(chan struct{})
					messageReceiver.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
						defer GinkgoRecover()
						close(terminationReceived)
					})

					terminate = func() {
						terminationFunction(messagingService, messageReceiver)
						// wait for 5 seconds for the termination event from the receiver
						// this is to account for flow reconnects on queue cases
						Eventually(terminationReceived, 5*time.Second).Should(BeClosed())
						Expect(messageReceiver.IsRunning()).To(BeFalse())
						Expect(messageReceiver.IsTerminated()).To(BeTrue())
					}
				})

				AfterEach(func() {
					if messageReceiver.IsRunning() {
						messageReceiver.Terminate(0)
					}
					if messagingService.IsConnected() {
						messagingService.Disconnect()
					}
					if terminationCleanup != nil {
						terminationCleanup()
					}
					// Check if the queue exists first
					if _, _, err := testcontext.SEMP().Config().QueueApi.GetMsgVpnQueue(testcontext.SEMP().ConfigCtx(),
						testcontext.Messaging().VPN, queueName, nil); err == nil {
						helpers.DeleteQueue(queueName)
					}
				})

				It("terminates with discarded messages without a message callback", func() {
					helpers.PublishNPersistentMessages(messagingService, topicString, numMessages)
					helpers.ValidateMetric(messagingService, metrics.PersistentMessagesReceived, numMessages)
					terminate()
					helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, numMessages)
					msg, err := messageReceiver.ReceiveMessage(1 * time.Second)
					Expect(msg).To(BeNil())
					helpers.ValidateError(err, &solace.IllegalStateError{})
				})

				It("terminates with discarded messages with a message callback", func() {
					blocker := make(chan struct{})
					messageChannel := make(chan message.InboundMessage, numMessages+1)
					messageReceiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
						<-blocker
						messageChannel <- inboundMessage
					})
					helpers.PublishNPersistentMessages(messagingService, topicString, numMessages+1)
					helpers.ValidateMetric(messagingService, metrics.PersistentMessagesReceived, numMessages+1)
					terminate()
					close(blocker)
					helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, numMessages)
					Eventually(messageChannel).Should(Receive(Not(BeNil())))
					Consistently(messageChannel).ShouldNot(Receive())
				})

				It("handles unsolicited terminations while already terminating", func() {
					blocker := make(chan struct{})
					signal := make(chan bool)
					messageChannel := make(chan message.InboundMessage, numMessages+1)

					// This timer needs to be big enough to give us a very
					// high chance of both receiving the messages and getting
					// past the state change in Terminate. It may need to be
					// updated in the future since this only mitigates the
					// race condition and doesn't eliminate it.
					waitToReceiveMessages := 30 * time.Second
					waitForTermination := waitToReceiveMessages
					messageReceiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
						signal <- true
						<-blocker
						messageChannel <- inboundMessage
					})
					helpers.PublishNPersistentMessages(messagingService, topicString, numMessages+1)
					helpers.ValidateMetric(messagingService, metrics.PersistentMessagesReceived, numMessages+1)
					terminateDuration := 2 * time.Second
					Eventually(signal, waitToReceiveMessages).Should(Receive())
					terminateChan := messageReceiver.TerminateAsync(terminateDuration)
					Eventually(messageReceiver.IsRunning, waitForTermination).Should(BeFalse())
					Expect(messageReceiver.IsRunning()).To(BeFalse())
					terminationFunction(messagingService, messageReceiver)
					Consistently(terminationReceived).ShouldNot(Receive())
					Consistently(terminateChan, terminateDuration).ShouldNot(Receive())
					close(blocker)
					close(signal)
					Eventually(terminateChan).Should(Receive())
					helpers.ValidateMetric(messagingService, metrics.ReceivedMessagesTerminationDiscarded, numMessages)
					Eventually(messageChannel).Should(Receive(Not(BeNil())))
					Consistently(messageChannel).ShouldNot(Receive())
				})
			})
		}

		Context("with toxiproxy", func() {
			var messagingService solace.MessagingService

			BeforeEach(func() {
				helpers.CheckToxiProxy()

				messagingService = helpers.BuildMessagingService(
					messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.ToxicConfiguration()),
				)
				helpers.ConnectMessagingService(messagingService)
			})

			AfterEach(func() {
				if messagingService.IsConnected() {
					helpers.DisconnectMessagingService(messagingService)
				}
			})

			Context("with a started receiver", func() {

				const queueName = "persistent_toxic_queue"
				const alreadyAddedTopic = "pre_added_topic"
				var receiver solace.PersistentMessageReceiver

				BeforeEach(func() {
					helpers.CreateQueue(queueName)
					receiver = helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
					Expect(receiver.Start()).ToNot(HaveOccurred())
					Expect(receiver.AddSubscription(resource.TopicSubscriptionOf(alreadyAddedTopic))).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					if !receiver.IsTerminated() {
						receiver.Terminate(0)
					}
					helpers.DeleteQueue(queueName)
				})

				Context("with high latency", func() {
					const latencyToxicName = "persistent_receiver_latency"
					const toxicDelay = 7000
					var subFuncCalled chan struct{}
					BeforeEach(func() {
						testcontext.Toxi().SMF().AddToxic(latencyToxicName, "latency", "upstream", 1.0, toxiproxy.Attributes{
							"latency": toxicDelay,
						})
						subFuncCalled = make(chan struct{})
					})

					AfterEach(func() {
						testcontext.Toxi().ResetProxies()
					})

					DescribeTable("can terminate with outstanding subscription operations",
						func(subscriptionOper func() chan error) {
							subscriptionErr := subscriptionOper()
							receiverTerminated := make(chan struct{})
							receiver.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
								close(receiverTerminated)
							})
							Eventually(subFuncCalled, "3000ms", "500ms").Should(BeClosed()) // add/remove sub have been called
							Eventually(messagingService.DisconnectAsync()).Should(Receive(BeNil()))
							Eventually(receiverTerminated).Should(BeClosed())
							var err error
							Eventually(subscriptionErr).Should(Receive(&err))
							helpers.ValidateError(err, &solace.ServiceUnreachableError{})
						},
						Entry("with a call to subscribe", func() chan error {
							subscriptionErr := make(chan error, 1)
							subscription := resource.TopicSubscriptionOf("some_subscription")
							go func() {
								// notify the channel that AddSubscription is just about to be called
								close(subFuncCalled)
								subscriptionErr <- receiver.AddSubscription(subscription)
							}()
							return subscriptionErr
						}),
						Entry("with a call to unsubscribe", func() chan error {
							subscriptionErr := make(chan error, 1)
							subscription := resource.TopicSubscriptionOf("some_subscription")
							go func() {
								// notify the channel that RemoveSubscription is just about to be called
								close(subFuncCalled)
								subscriptionErr <- receiver.RemoveSubscription(subscription)
							}()
							return subscriptionErr
						}),
						Entry("with a call to unsubscribe when subscription was already added", func() chan error {
							subscriptionErr := make(chan error, 1)
							subscription := resource.TopicSubscriptionOf(alreadyAddedTopic)
							go func() {
								// notify the channel that RemoveSubscription is just about to be called
								close(subFuncCalled)
								subscriptionErr <- receiver.RemoveSubscription(subscription)
							}()
							return subscriptionErr
						}),
						Entry("with a call to async subscribe", func() chan error {
							subscriptionErr := make(chan error, 1)
							subscription := resource.TopicSubscriptionOf("some_subscription")
							// notify the channel that AddSubscriptionAsync is just about to be called
							close(subFuncCalled)
							err := receiver.AddSubscriptionAsync(subscription,
								func(callbackSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
									defer GinkgoRecover()
									Expect(callbackSubscription).To(Equal(subscription))
									Expect(operation).To(Equal(solace.SubscriptionAdded))
									subscriptionErr <- errOrNil
								},
							)
							Expect(err).ToNot(HaveOccurred())
							return subscriptionErr
						}),
						Entry("with a call to async unsubscribe", func() chan error {
							subscriptionErr := make(chan error, 1)
							subscription := resource.TopicSubscriptionOf("some_subscription")
							// notify the channel that RemoveSubscriptionAsync is just about to be called
							close(subFuncCalled)
							err := receiver.RemoveSubscriptionAsync(subscription,
								func(callbackSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
									defer GinkgoRecover()
									Expect(callbackSubscription).To(Equal(subscription))
									Expect(operation).To(Equal(solace.SubscriptionRemoved))
									subscriptionErr <- errOrNil
								},
							)
							Expect(err).ToNot(HaveOccurred())
							return subscriptionErr
						}),
						Entry("with a call to async unsubscribe when subscription was already added", func() chan error {
							subscriptionErr := make(chan error, 1)
							subscription := resource.TopicSubscriptionOf(alreadyAddedTopic)
							// notify the channel that RemoveSubscriptionAsync is just about to be called
							close(subFuncCalled)
							err := receiver.RemoveSubscriptionAsync(subscription,
								func(callbackSubscription resource.Subscription, operation solace.SubscriptionOperation, errOrNil error) {
									defer GinkgoRecover()
									Expect(callbackSubscription).To(Equal(subscription))
									Expect(operation).To(Equal(solace.SubscriptionRemoved))
									subscriptionErr <- errOrNil
								},
							)
							Expect(err).ToNot(HaveOccurred())
							return subscriptionErr
						}),
					)
				})
			})
		})
	})
})
