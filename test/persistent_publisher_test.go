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
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	sempconfig "solace.dev/go/messaging/test/sempclient/config"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const defaultPubWindowSize = 255

var persistentPublisherStartFunctions = map[string](func(publisher solace.PersistentMessagePublisher) <-chan error){
	"sync": func(publisher solace.PersistentMessagePublisher) <-chan error {
		c := make(chan error)
		go func() {
			c <- publisher.Start()
		}()
		return c
	},
	"async": func(publisher solace.PersistentMessagePublisher) <-chan error {
		return publisher.StartAsync()
	},
	"callback": func(publisher solace.PersistentMessagePublisher) <-chan error {
		c := make(chan error)
		publisher.StartAsyncCallback(func(dmp solace.PersistentMessagePublisher, e error) {
			defer GinkgoRecover()
			Expect(dmp).To(Equal(publisher))
			c <- e
		})
		return c
	},
}
var persistentPublisherTerminateFunctions = map[string](func(publisher solace.PersistentMessagePublisher) <-chan error){
	"sync": func(publisher solace.PersistentMessagePublisher) <-chan error {
		c := make(chan error)
		go func() {
			c <- publisher.Terminate(gracePeriod)
		}()
		return c
	},
	"async": func(publisher solace.PersistentMessagePublisher) <-chan error {
		return publisher.TerminateAsync(gracePeriod)
	},
	"callback": func(publisher solace.PersistentMessagePublisher) <-chan error {
		c := make(chan error)
		publisher.TerminateAsyncCallback(gracePeriod, func(e error) {
			c <- e
		})
		return c
	},
}

var _ = Describe("PersistentPublisher", func() {
	const topicString = "hello/world"
	var topic = resource.TopicOf(topicString)
	const queueName = "testqueue"

	Context("with a default messaging service", func() {
		var messagingService solace.MessagingService
		var messageBuilder solace.OutboundMessageBuilder
		BeforeEach(func() {
			messagingService = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration()))
			messageBuilder = messagingService.MessageBuilder()
		})

		Describe("Builder verification", func() {
			invalidConfigurations := map[string]config.PublisherPropertyMap{
				"invalid backpressure configuration": {
					config.PublisherPropertyBackPressureStrategy: "not a strategy",
				},
				"invalid backpressure configuration type": {
					config.PublisherPropertyBackPressureStrategy: 123,
				},
				"invalid backpressure buffer size type": {
					config.PublisherPropertyBackPressureBufferCapacity: "asdf",
				},
			}
			for key, val := range invalidConfigurations {
				invalidConfiguration := val
				It("fails to build with "+key, func() {
					_, err := messagingService.CreatePersistentMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
					Expect(err).To(HaveOccurred())
					Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
				})
			}
			invalidBufferCapacity := map[string]config.PublisherPropertyMap{
				"invalid backpressure capacity in reject": {
					config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
					config.PublisherPropertyBackPressureBufferCapacity: -1,
				},
				"invalid backpressure capacity in wait": {
					config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferWaitWhenFull,
					config.PublisherPropertyBackPressureBufferCapacity: 0,
				},
			}
			for key, val := range invalidBufferCapacity {
				invalidConfiguration := val
				It("fails to build with "+key, func() {
					_, err := messagingService.CreatePersistentMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
					Expect(err).To(HaveOccurred())
					Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
				})
			}
			requiredProperties := map[string]config.PublisherPropertyMap{
				"nil backpressure configuration": {
					config.PublisherPropertyBackPressureStrategy: nil,
				},
				"nil buffer size": {
					config.PublisherPropertyBackPressureBufferCapacity: nil,
				},
			}
			for key, value := range requiredProperties {
				invalidConfiguration := value
				It("should error with "+key, func() {
					_, err := messagingService.CreatePersistentMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
					Expect(err).To(HaveOccurred())
					Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
					Expect(err.Error()).To(ContainSubstring("required property"))
				})
			}

			It("can print the builder to a string and see the pointer", func() {
				builder := messagingService.CreatePersistentMessagePublisherBuilder()
				str := fmt.Sprint(builder)
				Expect(str).ToNot(BeEmpty())
				Expect(str).To(ContainSubstring(fmt.Sprintf("%p", builder)))
			})

		})

		Context("with a connected messaging service", func() {
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
			It("can print the publisher to a string and see the pointer", func() {
				publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				str := fmt.Sprint(publisher)
				Expect(str).ToNot(BeEmpty())
				Expect(str).To(ContainSubstring(fmt.Sprintf("%p", publisher)))
			})

			It("can publish a message and get an acknowledgement", func() {
				publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				Expect(publisher.Start()).ToNot(HaveOccurred())
				publishExpectAck(messagingService, messageBuilder, publisher, topic)
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("can publish a message and get an acknowledgement using additional config", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
				Expect(publisher.Start()).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())

				payload := "hello world"
				context := "this is the context"
				msgID := "helloWorld"

				publishExpectAckWithValidationAndPublish(messagingService, publisher, func() {
					msg, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
						config.MessagePropertyPersistentAckImmediately: true,
					}).BuildWithStringPayload(payload)
					Expect(err).ToNot(HaveOccurred())
					err = publisher.Publish(msg, topic, config.MessagePropertyMap{
						config.MessagePropertyApplicationMessageID: msgID,
					}, context)
					Expect(err).ToNot(HaveOccurred())
				}, func(publishReceipt solace.PublishReceipt) {
					actualPayload, ok := publishReceipt.GetMessage().GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(actualPayload).To(Equal(payload))
					Expect(publishReceipt.GetUserContext()).To(Equal(context))
				})

				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
				helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, 1)
				inboundMessage, err := receiver.ReceiveMessage(10 * time.Second)
				Expect(err).ToNot(HaveOccurred())
				actualMsgID, ok := inboundMessage.GetApplicationMessageID()
				Expect(ok).To(BeTrue())
				Expect(actualMsgID).To(Equal(msgID))
				Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("can publish a message and get an acknowledgement with PublishAwaitAcknowledgement", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				Expect(publisher.Start()).ToNot(HaveOccurred())
				msg, err := messagingService.MessageBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				Expect(publisher.PublishAwaitAcknowledgement(msg, topic, 10*time.Second, nil)).ToNot(HaveOccurred())
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
				helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, 1)
			})

			It("can publish a message and get an acknowledgement with PublishAwaitAcknowledgement using additional config", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				receiver := helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
				Expect(publisher.Start()).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())
				msg, err := messagingService.MessageBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				msgID := "helloWorld"
				Expect(publisher.PublishAwaitAcknowledgement(msg, topic, 10*time.Second, config.MessagePropertyMap{
					config.MessagePropertyApplicationMessageID: msgID,
				})).ToNot(HaveOccurred())
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
				helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, 1)
				inboundMessage, err := receiver.ReceiveMessage(10 * time.Second)
				Expect(err).ToNot(HaveOccurred())
				actualMsgID, ok := inboundMessage.GetApplicationMessageID()
				Expect(ok).To(BeTrue())
				Expect(actualMsgID).To(Equal(msgID))
				Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("can publish a message using additional bad config and expect error", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				Expect(publisher.Start()).ToNot(HaveOccurred())
				publishReceiptCalled := make(chan struct{})
				publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
					defer GinkgoRecover()
					close(publishReceiptCalled)
				})
				msg, err := messagingService.MessageBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				err = publisher.Publish(msg, topic, config.MessagePropertyMap{
					config.MessagePropertyPriority: 1000,
				}, nil)
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				Consistently(publishReceiptCalled).ShouldNot(BeClosed())
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("can publish a message with PublishAwaitAcknowledgement using additional bad config and expect error", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				Expect(publisher.Start()).ToNot(HaveOccurred())
				msg, err := messagingService.MessageBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				err = publisher.PublishAwaitAcknowledgement(msg, topic, 10*time.Second, config.MessagePropertyMap{
					config.MessagePropertyPriority: 1000,
				})
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("can publish messages using PublishString", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				Expect(publisher.Start()).ToNot(HaveOccurred())
				payload := "hello world"
				publishExpectAckWithValidationAndPublish(messagingService, publisher, func() {
					err := publisher.PublishString(payload, topic)
					Expect(err).ToNot(HaveOccurred())
				}, func(publishReceipt solace.PublishReceipt) {
					actualPayload, ok := publishReceipt.GetMessage().GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(actualPayload).To(Equal(payload))
					Expect(publishReceipt.GetUserContext()).To(BeNil())
				})
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("can publish messages using PublishBytes", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				Expect(publisher.Start()).ToNot(HaveOccurred())
				payload := []byte("hello world")
				publishExpectAckWithValidationAndPublish(messagingService, publisher, func() {
					err := publisher.PublishBytes(payload, topic)
					Expect(err).ToNot(HaveOccurred())
				}, func(publishReceipt solace.PublishReceipt) {
					actualPayload, ok := publishReceipt.GetMessage().GetPayloadAsBytes()
					Expect(ok).To(BeTrue())
					Expect(actualPayload).To(Equal(payload))
					Expect(publishReceipt.GetUserContext()).To(BeNil())
				})
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			Describe("Publish Receipts", func() {

				It("can publish lots of messages and not process receipts", func() {
					const numMessages = 10000
					publisher := helpers.NewPersistentPublisher(messagingService)
					Expect(publisher.Start()).ToNot(HaveOccurred())
					receiptChan := make(chan solace.PublishReceipt, numMessages)
					blocker := make(chan struct{})
					publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
						<-blocker
						receiptChan <- pr
					})
					msg := helpers.NewMessage(messagingService)
					for i := 0; i < numMessages; i++ {
						publisher.Publish(msg, topic, nil, i)
					}
					// Really give the receipts a chance to pile up
					time.Sleep(500 * time.Millisecond)
					close(blocker)
					for i := 0; i < numMessages; i++ {
						Eventually(receiptChan).Should(Receive(), fmt.Sprintf("Failed to receive receipt %d", i))
					}
					Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
				})

				It("can terminate ungracefully with unprocessed events", func() {
					const numMessages = 10
					publisher := helpers.NewPersistentPublisher(messagingService)
					Expect(publisher.Start()).ToNot(HaveOccurred())
					receiptChan := make(chan solace.PublishReceipt)
					publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
						receiptChan <- pr
					})
					msg := helpers.NewMessage(messagingService)
					for i := 0; i < numMessages; i++ {
						publisher.Publish(msg, topic, nil, i)
					}
					// Give the messages a chance to publish
					<-time.After(500 * time.Millisecond)
					terminateChan := publisher.TerminateAsync(500 * time.Millisecond)
					Consistently(terminateChan).ShouldNot(Receive())
					for i := 0; i < numMessages; i++ {
						Eventually(receiptChan).Should(Receive(), fmt.Sprintf("Failed to receive receipt %d", i))
					}
					Eventually(terminateChan).Should(Receive())
				})

				It("can terminate unsolicitedly with unprocessed events", func() {
					const numMessages = 10
					publisher := helpers.NewPersistentPublisher(messagingService)
					Expect(publisher.Start()).ToNot(HaveOccurred())
					receiptChan := make(chan solace.PublishReceipt)
					publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
						receiptChan <- pr
					})
					msg := helpers.NewMessage(messagingService)
					for i := 0; i < numMessages; i++ {
						publisher.Publish(msg, topic, nil, i)
					}
					// Give the messages a chance to publish
					<-time.After(500 * time.Millisecond)
					// this should kill the publisher
					messagingService.Disconnect()
					// then we should still get all events
					for i := 0; i < numMessages; i++ {
						Eventually(receiptChan).Should(Receive(), fmt.Sprintf("Failed to receive receipt %d", i))
					}
				})
			})

			Describe("Lifecycle", func() {
				for startName, fn := range persistentPublisherStartFunctions {
					for terminateName, fn2 := range persistentPublisherTerminateFunctions {
						start := fn
						terminate := fn2
						It("can start and terminate with start "+startName+" and terminate "+terminateName, func() {
							publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
							Expect(err).ToNot(HaveOccurred())
							// check that not started
							helpers.ValidateReadyState(publisher, false, false, false, false)
							// start and check state
							Eventually(start(publisher)).Should(Receive(Not(HaveOccurred())))
							helpers.ValidateReadyState(publisher, true, true, false, false)
							// try using publisher
							publishExpectAck(messagingService, messageBuilder, publisher, topic)
							Eventually(func() int64 {
								return helpers.GetClient(messagingService).DataRxMsgCount
							}).Should(BeNumerically("==", 1))
							// terminate and check state
							Eventually(terminate(publisher)).Should(Receive(Not(HaveOccurred())))
							helpers.ValidateReadyState(publisher, false, false, false, true)
						})
						It("can start and terminate idempotently with start "+startName+" and terminate "+terminateName, func() {
							publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
							Expect(err).ToNot(HaveOccurred())
							// check that not started
							helpers.ValidateReadyState(publisher, false, false, false, false)
							// start and check state
							c1 := start(publisher)
							c2 := start(publisher)
							Eventually(c1).Should(Receive(Not(HaveOccurred())))
							Eventually(c2).Should(Receive(Not(HaveOccurred())))
							helpers.ValidateReadyState(publisher, true, true, false, false)
							// try using publisher
							publishExpectAck(messagingService, messageBuilder, publisher, topic)
							Eventually(func() int64 {
								return helpers.GetClient(messagingService).DataRxMsgCount
							}).Should(BeNumerically("==", 1))
							// terminate and check state
							c1 = terminate(publisher)
							c2 = terminate(publisher)
							Eventually(c1).Should(Receive(Not(HaveOccurred())))
							Eventually(c2).Should(Receive(Not(HaveOccurred())))
							helpers.ValidateReadyState(publisher, false, false, false, true)
							c1 = start(publisher)
							c2 = start(publisher)
							Eventually(c1).Should(Receive(BeAssignableToTypeOf(&solace.IllegalStateError{})))
							Eventually(c2).Should(Receive(BeAssignableToTypeOf(&solace.IllegalStateError{})))
						})
					}
				}
				for startName, fn := range persistentPublisherStartFunctions {
					start := fn
					It("should fail to start when messaging service is disconnected using start "+startName, func() {
						publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
						Expect(err).ToNot(HaveOccurred())

						messagingService.Disconnect()

						select {
						case err := <-start(publisher):
							Expect(err).To(HaveOccurred())
							Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
						case <-time.After(1 * time.Second):
							Fail("timed out waiting for start to complete")
						}
					})
					It("should fail to start when messaging service is down using start "+startName, func() {
						publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
						Expect(err).ToNot(HaveOccurred())

						helpers.ForceDisconnectViaSEMPv2(messagingService)
						Eventually(messagingService.IsConnected).Should(BeFalse())
						Expect(messagingService.Disconnect()).ToNot(HaveOccurred())

						select {
						case err := <-start(publisher):
							Expect(err).To(HaveOccurred())
							Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
						case <-time.After(1 * time.Second):
							Fail("timed out waiting for start to complete")
						}
					})
				}
				for terminateName, fn := range persistentPublisherTerminateFunctions {
					terminate := fn
					It("should be able to terminate when not started using terminate "+terminateName, func() {
						publisher := helpers.NewPersistentPublisher(messagingService)
						Eventually(terminate(publisher)).Should(Receive(BeNil()))
						Expect(publisher.IsTerminated()).To(BeTrue())
						helpers.ValidateError(publisher.Start(), &solace.IllegalStateError{})
					})
				}
				It("should fail to publish on unstarted publisher", func() {
					publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
					Expect(err).ToNot(HaveOccurred())

					msg, err := messagingService.MessageBuilder().Build()
					Expect(err).ToNot(HaveOccurred())
					helpers.ValidateError(publisher.Publish(msg, topic, nil, nil), &solace.IllegalStateError{})
					helpers.ValidateError(publisher.PublishAwaitAcknowledgement(msg, topic, -1, nil), &solace.IllegalStateError{})
					helpers.ValidateError(publisher.PublishString("hello world", topic), &solace.IllegalStateError{})
					helpers.ValidateError(publisher.PublishBytes([]byte{1, 2, 3, 4}, topic), &solace.IllegalStateError{})
				})
				It("should fail to publish on terminated publisher", func() {
					publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
					Expect(err).ToNot(HaveOccurred())

					publisher.Start()
					publisher.Terminate(gracePeriod)

					msg, err := messagingService.MessageBuilder().Build()
					Expect(err).ToNot(HaveOccurred())
					helpers.ValidateError(publisher.Publish(msg, topic, nil, nil), &solace.IllegalStateError{})
					helpers.ValidateError(publisher.PublishAwaitAcknowledgement(msg, topic, -1, nil), &solace.IllegalStateError{})
					helpers.ValidateError(publisher.PublishString("hello world", topic), &solace.IllegalStateError{})
					helpers.ValidateError(publisher.PublishBytes([]byte{1, 2, 3, 4}, topic), &solace.IllegalStateError{})
				})
			}) // end lifecycle

			Describe("Backpressure", func() {
				backpressureConfigurations := map[string]func(builder solace.PersistentMessagePublisherBuilder){
					"with backpressure wait": func(builder solace.PersistentMessagePublisherBuilder) {
						builder.OnBackPressureWait(1000)
					},
					"with backpressure reject": func(builder solace.PersistentMessagePublisherBuilder) {
						builder.OnBackPressureReject(1000)
					},
					"with backpressure direct": func(builder solace.PersistentMessagePublisherBuilder) {
						builder.OnBackPressureReject(0)
					},
				}
				for test, backpressureFunctionRef := range backpressureConfigurations {
					backpressureConfig := backpressureFunctionRef
					It("can publish and get acknowledgements in backpressure "+test, func() {
						topic := topic
						builder := messagingService.CreatePersistentMessagePublisherBuilder()
						backpressureConfig(builder)
						publisher, err := builder.Build()
						Expect(err).ToNot(HaveOccurred())
						Expect(publisher.Start()).ToNot(HaveOccurred())
						publishExpectAck(messagingService, messageBuilder, publisher, topic)
						Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
					})
				}
			}) // end backpressure

			Context("with an ACL deny exception", func() {
				const deniedTopic = "persistent-publisher-acl-deny"
				BeforeEach(func() {
					helpers.AddPublishTopicException(deniedTopic)
				})

				AfterEach(func() {
					helpers.RemovePublishTopicException(deniedTopic)
				})

				DescribeTable("Publish Fail with buffered backpressure",
					func(publishFunction func(solace.PersistentMessagePublisher, *resource.Topic) error, expectError bool) {
						publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
						Expect(err).ToNot(HaveOccurred())
						Expect(publisher.Start()).ToNot(HaveOccurred())
						publishReceiptError := make(chan error, 1)
						publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
							defer GinkgoRecover()
							publishReceiptError <- pr.GetError()
						})
						err = publishFunction(publisher, resource.TopicOf(deniedTopic))
						if expectError {
							Consistently(publishReceiptError).ShouldNot(Receive())
						} else {
							Expect(err).ToNot(HaveOccurred())
							Eventually(publishReceiptError).Should(Receive(&err))
						}
						helpers.ValidateNativeError(err, subcode.PublishAclDenied)
						Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
					},
					Entry("Publish", func(publisher solace.PersistentMessagePublisher, topic *resource.Topic) error {
						return publisher.Publish(helpers.NewMessage(messagingService), topic, nil, nil)
					}, false),
					Entry("PublishString", func(publisher solace.PersistentMessagePublisher, topic *resource.Topic) error {
						return publisher.PublishString("hello", topic)
					}, false),
					Entry("PublishBytes", func(publisher solace.PersistentMessagePublisher, topic *resource.Topic) error {
						return publisher.PublishBytes([]byte("hello"), topic)
					}, false),
					Entry("PublishAwaitAcknowledgement", func(publisher solace.PersistentMessagePublisher, topic *resource.Topic) error {
						return publisher.PublishAwaitAcknowledgement(helpers.NewMessage(messagingService), topic, 10*time.Second, nil)
					}, true),
				)
			})
		}) // end connected messaging service

		Context("with a connected messaging service without a valid destination", func() {
			BeforeEach(func() {
				_, _, err := testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						RejectMsgToSenderOnNoSubscriptionMatchEnabled: helpers.True,
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
				Expect(err).ToNot(HaveOccurred())
				helpers.ConnectMessagingService(messagingService)
			})

			AfterEach(func() {
				if messagingService.IsConnected() {
					helpers.DisconnectMessagingService(messagingService)
				}
				_, _, err := testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						RejectMsgToSenderOnNoSubscriptionMatchEnabled: helpers.False,
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
				Expect(err).ToNot(HaveOccurred())
			})

			It("gets a reject in a publish receipt when no matching subscription", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				Expect(publisher.Start()).ToNot(HaveOccurred())

				receiptChan := make(chan solace.PublishReceipt, 1)
				publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
					receiptChan <- pr
				})
				startTime := time.Now()

				context := "SomeContext"
				stringPayload := "hello world"
				msg, err := messagingService.MessageBuilder().BuildWithStringPayload(stringPayload)
				Expect(err).ToNot(HaveOccurred())
				publisher.Publish(msg, topic, config.MessagePropertyMap{
					config.MessagePropertyPersistentAckImmediately: true,
				}, context)

				var publishReceipt solace.PublishReceipt
				Eventually(receiptChan).Should(Receive(&publishReceipt))
				endTime := time.Now()
				// validate receipt
				helpers.ValidateNativeError(publishReceipt.GetError(), subcode.NoSubscriptionMatch)
				Expect(publishReceipt.IsPersisted()).To(BeFalse())
				Expect(publishReceipt.GetTimeStamp()).To(BeTemporally(">=", startTime))
				Expect(publishReceipt.GetTimeStamp()).To(BeTemporally("<=", endTime))
				Expect(publishReceipt.GetUserContext()).To(Equal(context))
				Expect(publishReceipt.GetMessage()).ToNot(BeNil())
				payload, ok := publishReceipt.GetMessage().GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal(stringPayload))

				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("gets a reject in a publish receipt when no matching subscription using PubAwaitAcknowledgement", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				Expect(publisher.Start()).ToNot(HaveOccurred())

				receiptChan := make(chan solace.PublishReceipt, 1)
				publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
					receiptChan <- pr
				})

				stringPayload := "hello world"
				msg, err := messagingService.MessageBuilder().BuildWithStringPayload(stringPayload)
				Expect(err).ToNot(HaveOccurred())
				err = publisher.PublishAwaitAcknowledgement(msg, topic, 10*time.Second, nil)
				helpers.ValidateNativeError(err, subcode.NoSubscriptionMatch)
				Consistently(receiptChan).ShouldNot(Receive())
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})
		})
	})

	Context("with a toxiproxy messaging service", func() {
		var messagingService solace.MessagingService
		var messageBuilder solace.OutboundMessageBuilder

		BeforeEach(func() {
			helpers.CheckToxiProxy()
			messagingService = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().
				FromConfigurationProvider(helpers.ToxicConfiguration()).
				WithReconnectionRetryStrategy(config.RetryStrategyNeverRetry()))
			messageBuilder = messagingService.MessageBuilder()
			helpers.ConnectMessagingService(messagingService)
			helpers.CreateQueue(queueName, topicString)
		})

		AfterEach(func() {
			if messagingService.IsConnected() {
				Expect(messagingService.Disconnect()).ToNot(HaveOccurred())
			}
			helpers.DeleteQueue(queueName)
		})

		Context("with high latency", func() {
			const latencyToxicName = "latency_upstream"
			const toxicDelay = 4000

			saturatePubWindow := func(numMessages int) {
				publisher := helpers.NewPersistentPublisher(messagingService, config.PublisherPropertyMap{
					config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
					config.PublisherPropertyBackPressureBufferCapacity: 0,
				})
				Expect(publisher.Start()).ToNot(HaveOccurred())
				msg, err := messagingService.MessageBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				for i := 0; i < numMessages; i++ {
					for {
						err := publisher.Publish(msg, topic, nil, nil)
						if err == nil {
							break
						}
						if _, ok := err.(*solace.PublisherOverflowError); !ok {
							Expect(err).ToNot(HaveOccurred())
						}
					}
				}
				helpers.ValidateError(publisher.Terminate(1*time.Second), &solace.IncompleteMessageDeliveryError{}, fmt.Sprintf("%d unacknowledged", numMessages))
			}

			BeforeEach(func() {
				testcontext.Toxi().SMF().AddToxic(latencyToxicName, "latency", "upstream", 1.0, toxiproxy.Attributes{
					"latency": toxicDelay,
				})
			})
			AfterEach(func() {
				testcontext.Toxi().ResetProxies()
			})

			for _, timeout := range []time.Duration{30 * time.Second, -1} {
				timeoutRef := timeout
				It("blocks until an acknowledgement is received with publish await ack with timeout "+timeoutRef.String(), func() {
					publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
					Expect(err).ToNot(HaveOccurred())
					publishReceiptChan := make(chan solace.PublishReceipt)
					publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
						publishReceiptChan <- pr
					})
					Expect(publisher.Start()).ToNot(HaveOccurred())
					payload := "hello world"
					msg, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
						config.MessagePropertyPersistentAckImmediately: true,
					}).BuildWithStringPayload(payload)
					Expect(err).ToNot(HaveOccurred())

					publishComplete := make(chan struct{})
					go func() {
						defer GinkgoRecover()
						err = publisher.PublishAwaitAcknowledgement(msg, topic, timeoutRef, nil)
						Expect(err).ToNot(HaveOccurred())
						close(publishComplete)
					}()
					// wait for publish to be successful
					Eventually(func() uint64 {
						return messagingService.Metrics().GetValue(metrics.PersistentMessagesSent)
					}, 5*time.Second).Should(BeNumerically("==", 1))

					// Should definitely not complete for at least half of the delay
					Consistently(publishComplete, (toxicDelay/2)*time.Millisecond).ShouldNot(BeClosed())

					// We expect the function to block until an ack is received, should arrive in twice delay easily
					Eventually(publishComplete, (2*toxicDelay)*time.Millisecond).Should(BeClosed())

					// When calling PublishAwaitAcknowledgement, we do not expect the receipt listener to be called
					Consistently(publishReceiptChan).ShouldNot(Receive())

					Expect(publisher.Terminate(toxicDelay * 3 * time.Millisecond)).ToNot(HaveOccurred())
				})
			}

			It("times out with wait acknowledgement small timeout", func() {
				publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				publishReceiptChan := make(chan solace.PublishReceipt)
				publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
					publishReceiptChan <- pr
				})
				Expect(publisher.Start()).ToNot(HaveOccurred())
				payload := "hello world"
				msg, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
					config.MessagePropertyPersistentAckImmediately: true,
				}).BuildWithStringPayload(payload)
				Expect(err).ToNot(HaveOccurred())

				publishComplete := make(chan struct{})
				go func() {
					defer GinkgoRecover()
					err = publisher.PublishAwaitAcknowledgement(msg, topic, (toxicDelay/4)*time.Millisecond, nil)
					helpers.ValidateError(err, &solace.TimeoutError{})
					close(publishComplete)
				}()
				// wait for publish to be successful
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.PersistentMessagesSent)
				}, 5*time.Second).Should(BeNumerically("==", 1))

				// We expect the function to block until an ack is received, should arrive in twice delay easily
				Eventually(publishComplete, (toxicDelay/2)*time.Millisecond).Should(BeClosed())

				// When calling PublishAwaitAcknowledgement, we do not expect the receipt listener to be called
				Consistently(publishReceiptChan).ShouldNot(Receive())

				Expect(publisher.Terminate(5 * time.Second)).ToNot(HaveOccurred())
			})

			It("waits for all messages to be published on graceful terminate", func() {
				saturatePubWindow(defaultPubWindowSize)
				messagingService.Metrics().Reset()
				publisher := helpers.NewPersistentPublisher(messagingService)
				publishReceiptChan := make(chan solace.PublishReceipt)
				publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
					defer GinkgoRecover()
					close(publishReceiptChan)
				})
				Expect(publisher.Start()).ToNot(HaveOccurred())
				payload := "hello world"
				msg, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
					config.MessagePropertyPersistentAckImmediately: true,
				}).BuildWithStringPayload(payload)
				Expect(err).ToNot(HaveOccurred())
				publisher.Publish(msg, topic, nil, nil)
				terminateChan := publisher.TerminateAsync(-1)
				Eventually(terminateChan, 3*toxicDelay*time.Millisecond).Should(Receive(BeNil()))
				Expect(publishReceiptChan).To(BeClosed())
				helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, 1)
			})

			It("waits for all acknowledgements to be processed on graceful terminate", func() {
				publisher := helpers.NewPersistentPublisher(messagingService)
				publishReceiptChan := make(chan solace.PublishReceipt)
				publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
					defer GinkgoRecover()
					close(publishReceiptChan)
				})
				Expect(publisher.Start()).ToNot(HaveOccurred())
				payload := "hello world"
				msg, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
					config.MessagePropertyPersistentAckImmediately: true,
				}).BuildWithStringPayload(payload)
				Expect(err).ToNot(HaveOccurred())
				publisher.Publish(msg, topic, nil, nil)
				terminateChan := publisher.TerminateAsync(-1)
				Eventually(terminateChan, 3*toxicDelay*time.Millisecond).Should(Receive(BeNil()))
				Expect(publishReceiptChan).To(BeClosed())
				helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, 1)
			})

			Describe("backpressure notifications", func() {
				It("blocks when the buffer fills in backpressure buffered wait", func() {
					bufferSize := 5
					publisher := helpers.NewPersistentPublisher(messagingService, config.PublisherPropertyMap{
						config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferWaitWhenFull,
						config.PublisherPropertyBackPressureBufferCapacity: bufferSize,
					})
					Expect(publisher.Start()).ToNot(HaveOccurred())
					msgToPublish, err := messagingService.MessageBuilder().BuildWithStringPayload("hello world")
					Expect(err).ToNot(HaveOccurred())
					// Publish window size + buffer size total messages
					for i := 0; i < defaultPubWindowSize+bufferSize; i++ {
						err := publisher.Publish(msgToPublish, topic, nil, nil)
						Expect(err).ToNot(HaveOccurred())
					}
					publishComplete := make(chan struct{})
					go func() {
						publisher.Publish(msgToPublish, topic, nil, nil)
						close(publishComplete)
					}()
					Consistently(publishComplete, (toxicDelay/2)*time.Millisecond).ShouldNot(BeClosed())
					Eventually(publishComplete, (2*toxicDelay)*time.Millisecond).Should(BeClosed())
					Expect(publisher.Terminate(toxicDelay * 3 * time.Millisecond)).ToNot(HaveOccurred())
				})

				for _, bufSizeRef := range []int{5, 0} {
					bufferSize := bufSizeRef
					It("returns a can send when the buffer fills in backpressure buffered reject of size "+fmt.Sprint(bufferSize), func() {
						publisher := helpers.NewPersistentPublisher(messagingService, config.PublisherPropertyMap{
							config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
							config.PublisherPropertyBackPressureBufferCapacity: bufferSize,
						})
						canSend := make(chan interface{}, 1)
						publisher.SetPublisherReadinessListener(func() {
							select {
							case canSend <- nil:
							default:
							}
						})

						Expect(publisher.Start()).ToNot(HaveOccurred())
						msgToPublish, err := messagingService.MessageBuilder().BuildWithStringPayload("hello world")
						Expect(err).ToNot(HaveOccurred())
						// Publish window size + buffer size total messages
						for i := 0; i < defaultPubWindowSize+bufferSize; i++ {
							for {
								err := publisher.Publish(msgToPublish, topic, nil, i)
								if err == nil {
									break
								}
								// retry if we get a pub overflow, can happen depending on race, shouldn't last long
								if _, ok := err.(*solace.PublisherOverflowError); ok {
									Eventually(canSend).Should(Receive())
								} else {
									Expect(err).ToNot(HaveOccurred())
								}
							}
						}
						// Drain can sends if needed
						// There is a condition where the number of messages goes from 5 to 4 back to 5
						// in the buffered scenario causing an additional can send that is not consumed.
						select {
						case <-canSend:
						case <-time.After(100 * time.Millisecond):
						}
						err = publisher.Publish(msgToPublish, topic, nil, nil)
						helpers.ValidateError(err, &solace.PublisherOverflowError{})
						publisher.NotifyWhenReady()
						Eventually(canSend, (2*toxicDelay)*time.Millisecond).Should(Receive())
						Expect(publisher.Terminate(toxicDelay * 3 * time.Millisecond)).ToNot(HaveOccurred())
					})
				}
			}) // End backpressure notifications

			Describe("ungraceful terminations", func() {
				// helper function to saturate the pub window with numMessages messages
				backpressureConfigurations := map[string]config.PublisherPropertyMap{
					"wait": { // wait with buffer
						config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferWaitWhenFull,
						config.PublisherPropertyBackPressureBufferCapacity: 1,
					},
					"buffered reject": { // reject with buffer
						config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
						config.PublisherPropertyBackPressureBufferCapacity: 10,
					},
					"unbuffered reject": { // reject without buffer (direct)
						config.PublisherPropertyBackPressureStrategy:       config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
						config.PublisherPropertyBackPressureBufferCapacity: 0,
					},
				}

				// We want to test on all backpressure functions
				for backpressureStringRef, backpressureConfigRef := range backpressureConfigurations {
					backpressureString := backpressureStringRef
					backpressureConfig := backpressureConfigRef

					Context("with backpressure "+backpressureString, func() {

						backpressureBufferSize := backpressureConfig[config.PublisherPropertyBackPressureBufferCapacity]

						var publisher solace.PersistentMessagePublisher

						BeforeEach(func() {
							publisher = helpers.NewPersistentPublisher(messagingService, backpressureConfig)
							Expect(publisher.Start()).ToNot(HaveOccurred())
						})

						AfterEach(func() {
							if !publisher.IsTerminated() {
								publisher.Terminate(0)
							}
						})

						const payloadTemplate = "hello world %d"

						// Publishes n messages with the payload template as a string payload and a context both using the index of the message
						publishMessages := func(messages int) {
							for i := 0; i < messages; i++ {
								msg, err := messagingService.MessageBuilder().BuildWithStringPayload(fmt.Sprintf(payloadTemplate, i))
								Expect(err).ToNot(HaveOccurred())
								Expect(publisher.Publish(msg, topic, nil, i)).ToNot(HaveOccurred())
							}
						}

						// Terminates the receiver and expects all published messages to be undelivered
						terminateAndExpectReceiptsWithReceiptValidation := func(messages int, publishReceiptErrorValidation func(error), terminateFunction func()) {
							startTime := time.Now()
							receiptListenerCalled := make(chan struct{})
							numReceiptListenerCalled := 0
							receiptsCalled := make([]bool, messages)

							publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
								defer GinkgoRecover()
								// validate error
								publishReceiptErrorValidation(pr.GetError())
								// validate timestamp
								Expect(pr.GetTimeStamp()).To(BeTemporally(">", startTime))
								Expect(pr.GetTimeStamp()).To(BeTemporally("<", time.Now()))
								Expect(pr.IsPersisted()).To(BeFalse())
								// validate that a valid user context int was called
								receiptCalled := pr.GetUserContext().(int)
								Expect(receiptsCalled[receiptCalled]).To(BeFalse())
								// validate message content
								Expect(pr.GetMessage()).ToNot(BeNil())
								payload, ok := pr.GetMessage().GetPayloadAsString()
								Expect(ok).To(BeTrue())
								Expect(payload).To(Equal(fmt.Sprintf(payloadTemplate, receiptCalled)))
								// notify complete
								numReceiptListenerCalled++
								if numReceiptListenerCalled == messages {
									Expect(func() {
										close(receiptListenerCalled)
									}).ToNot(Panic())
								}
							})
							messagingService.Metrics().Reset()
							terminateFunction()
							Eventually(receiptListenerCalled).Should(BeClosed())
							// Validate we cannot publish and are in the correct state
							helpers.ValidateError(publisher.PublishString("hello world", topic), &solace.IllegalStateError{})
							Expect(publisher.IsTerminated()).To(BeTrue())
							Expect(publisher.IsReady()).To(BeFalse())
							Expect(publisher.IsRunning()).To(BeFalse())
						}

						// Publishes n messages with the payload template as a string payload and a context both using the index of the message
						publishMessagesAwaitAck := func(messages int) []chan error {
							errChans := make([]chan error, messages)
							for i := 0; i < messages; i++ {
								msg, err := messagingService.MessageBuilder().BuildWithStringPayload(fmt.Sprintf(payloadTemplate, i))
								Expect(err).ToNot(HaveOccurred())
								errChan := make(chan error, 1)
								go func() {
									defer GinkgoRecover()
									errChan <- publisher.PublishAwaitAcknowledgement(msg, topic, 10*time.Second, nil)
								}()
								errChans[i] = errChan
							}
							return errChans
						}

						terminateAndExpectResults := func(errors []chan error, publisherErrorValidation func(error), terminateFunction func()) {
							messagingService.Metrics().Reset()
							terminateFunction()
							for _, errChan := range errors {
								var err error
								Eventually(errChan).Should(Receive(&err))
								publisherErrorValidation(err)
							}
						}

						terminateWithWaitValidation := func(numUndelivered, numUnacknowledged int, terminateFunc func(int, int)) func() {
							return func() {
								publishInterrupted := make(chan struct{})
								if backpressureString == "wait" {
									go func() {
										defer GinkgoRecover()
										helpers.ValidateError(publisher.PublishString("hello world", topic), &solace.IllegalStateError{})
										close(publishInterrupted)
									}()
									time.Sleep(50 * time.Millisecond)
								} else {
									close(publishInterrupted)
								}
								terminateFunc(numUndelivered, numUnacknowledged)
								Eventually(publishInterrupted).Should(BeClosed())
							}
						}

						generateTerminationTests := func(contextString string, receiptValidation func(error),
							terminateFunction func(expectedUndelivered, expectedUnacknowledged int)) {
							Context(contextString, func() {
								// we can only have undelivered messages in buffered backpressure
								if backpressureBufferSize != 0 {
									It("should terminate with undelivered messages when the pub window is saturated", func() {
										saturatePubWindow(defaultPubWindowSize)
										publishMessages(1)
										Consistently(func() uint64 {
											return messagingService.Metrics().GetValue(metrics.PersistentMessagesSent)
										}).Should(BeNumerically("==", defaultPubWindowSize))
										terminateAndExpectReceiptsWithReceiptValidation(1, receiptValidation,
											terminateWithWaitValidation(1, 0, terminateFunction))
									})
									It("should terminate with undelivered messages when the pub window is saturated using pub await ack", func() {
										saturatePubWindow(defaultPubWindowSize)
										errors := publishMessagesAwaitAck(1)
										Consistently(func() uint64 {
											return messagingService.Metrics().GetValue(metrics.PersistentMessagesSent)
										}).Should(BeNumerically("==", defaultPubWindowSize))
										terminateAndExpectResults(errors, receiptValidation, func() {
											terminateFunction(1, 0)
										})
									})
									It("should terminate with both unacknowledged messages and undeliverred messages when the pub window is saturated", func() {
										saturatePubWindow(defaultPubWindowSize - 1)
										publishMessages(2)
										helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, defaultPubWindowSize)
										terminateAndExpectReceiptsWithReceiptValidation(2, receiptValidation,
											terminateWithWaitValidation(1, 1, terminateFunction))
									})
									It("should terminate with both unacknowledged messages and undeliverred messages when the pub window is saturated using pub await ack", func() {
										saturatePubWindow(defaultPubWindowSize - 1)
										errors := publishMessagesAwaitAck(2)
										helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, defaultPubWindowSize)
										terminateAndExpectResults(errors, receiptValidation, func() {
											terminateFunction(1, 1)
										})
									})
								}
								It("should terminate with unacknowledged messages with latency", func() {
									publishMessages(1)
									helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, 1)
									terminateAndExpectReceiptsWithReceiptValidation(1, receiptValidation, func() {
										terminateFunction(0, 1)
									})
								})
								It("should terminate with unacknowledged messages with latency using pub await ack", func() {
									errors := publishMessagesAwaitAck(1)
									helpers.ValidateMetric(messagingService, metrics.PersistentMessagesSent, 1)
									terminateAndExpectResults(errors, receiptValidation, func() {
										terminateFunction(0, 1)
									})
								})
							})
						}

						// We want to test the following tests with a variety of terminate calls initiated by the user
						for _, terminateTimeRef := range []time.Duration{0, 500 * time.Millisecond} {
							terminateTime := terminateTimeRef

							generateTerminationTests("with user-controlled termination, timeout "+terminateTime.String(), func(err error) {
								helpers.ValidateError(err, &solace.IncompleteMessageDeliveryError{}, "terminated")
							}, func(expectedUndelivered, expectedUnacknowledged int) {
								err := publisher.Terminate(terminateTime)
								helpers.ValidateError(err, &solace.IncompleteMessageDeliveryError{}, fmt.Sprintf("%d undelivered", expectedUndelivered), fmt.Sprintf("%d unacknowledged", expectedUnacknowledged))
								helpers.ValidateMetric(messagingService, metrics.PublishMessagesTerminationDiscarded, uint64(expectedUndelivered))
							})
						}

						// we have a few unsolicited termination tests which expect all messages to be correlated
						unsolicitedTermination := func(dcFunc func(), errorValidation func(error)) func(int, int) {
							return func(expectedUndelivered, expectedUnacknowledged int) {
								startTime := time.Now()
								terminationReceived := make(chan struct{})
								publisher.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
									defer GinkgoRecover()
									errorValidation(te.GetCause())
									Expect(te.GetTimestamp()).To(BeTemporally(">=", startTime))
									Expect(te.GetTimestamp()).To(BeTemporally("<=", time.Now()))
									Expect(te.GetMessage()).ToNot(BeEmpty())
									close(terminationReceived)
								})
								dcFunc()
								Eventually(terminationReceived).Should(BeClosed())
								helpers.ValidateMetric(messagingService, metrics.PublishMessagesTerminationDiscarded, uint64(expectedUndelivered))
							}
						}

						// test messaging service disconnects, expecting a service unreachable error
						messagingServiceDCValidation := func(e error) {
							helpers.ValidateError(e, &solace.ServiceUnreachableError{})
						}
						generateTerminationTests("with unsolicited termination via MessagingService disconnect",
							messagingServiceDCValidation,
							unsolicitedTermination(func() {
								messagingService.Disconnect()
							}, messagingServiceDCValidation))

						// test a socket close
						socketCloseValidation := func(e error) {
							helpers.ValidateNativeError(e, subcode.CommunicationError)
						}
						generateTerminationTests("with unsolicited termination via ToxiProxy",
							socketCloseValidation,
							unsolicitedTermination(func() {
								Expect(testcontext.Toxi().SMF().Disable()).ToNot(HaveOccurred())
							}, socketCloseValidation))
					})
				}

			}) // End ungraceful termination tests
		})
	})

})

func publishExpectAck(messagingService solace.MessagingService, messageBuilder solace.OutboundMessageBuilder, publisher solace.PersistentMessagePublisher, topic *resource.Topic) {
	payload := "hello world"
	context := "this is the context"

	publishExpectAckWithValidationAndPublish(messagingService, publisher, func() {
		msg, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
			config.MessagePropertyPersistentAckImmediately: true,
		}).BuildWithStringPayload(payload)
		Expect(err).ToNot(HaveOccurred())
		Expect(publisher.Publish(msg, topic, nil, context)).ToNot(HaveOccurred())
	}, func(publishReceipt solace.PublishReceipt) {
		actualPayload, ok := publishReceipt.GetMessage().GetPayloadAsString()
		Expect(ok).To(BeTrue())
		Expect(actualPayload).To(Equal(payload))
		Expect(publishReceipt.GetUserContext()).To(Equal(context))
	})
}

func publishExpectAckWithValidationAndPublish(messagingService solace.MessagingService, publisher solace.PersistentMessagePublisher,
	publishFunction func(), validationFunction func(receipt solace.PublishReceipt)) {
	publishReceiptChan := make(chan solace.PublishReceipt)
	publisher.SetMessagePublishReceiptListener(func(pr solace.PublishReceipt) {
		publishReceiptChan <- pr
	})
	startTime := time.Now()
	// publish message
	publishFunction()
	Eventually(func() uint64 {
		return messagingService.Metrics().GetValue(metrics.PersistentMessagesSent)
	}, 5*time.Second).Should(BeNumerically("==", 1))
	var publishReceipt solace.PublishReceipt
	Eventually(publishReceiptChan, 5*time.Second).Should(Receive(&publishReceipt))
	endTime := time.Now()
	// validate receipt
	validationFunction(publishReceipt)
	Expect(publishReceipt.GetError()).To(BeNil())
	Expect(publishReceipt.IsPersisted()).To(BeTrue())
	Expect(publishReceipt.GetTimeStamp()).To(BeTemporally(">=", startTime))
	Expect(publishReceipt.GetTimeStamp()).To(BeTemporally("<=", endTime))
}
