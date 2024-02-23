// pubsubplus-go-client
//
// Copyright 2021-2024 Solace Corporation. All rights reserved.
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

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/test/helpers"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("RequestReplyPublisher", func() {

	var messagingService solace.MessagingService

	BeforeEach(func() {
		builder := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration())
		messagingService = helpers.BuildMessagingService(builder)
	})

	Describe("Builder verification", func() {
		invalidConfigurations := map[string]config.PublisherPropertyMap{
			"invalid backpressure configuration": {
				config.PublisherPropertyBackPressureStrategy: "not a strategy",
			},
			"invalid backpressure configuration type": {
				config.PublisherPropertyBackPressureStrategy: 1234,
			},
			"invalid backpressure buffer size type": {
				config.PublisherPropertyBackPressureBufferCapacity: "asdf",
			},
		}
		for key, val := range invalidConfigurations {
			invalidConfiguration := val
			It("fails to build with "+key, func() {
				_, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
				helpers.ValidateError(err, &solace.IllegalArgumentError{})
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
				_, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
				helpers.ValidateError(err, &solace.InvalidConfigurationError{})
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
				_, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
				helpers.ValidateError(err, &solace.InvalidConfigurationError{}, "required property")
			})
		}

		validConfigurations := map[string]config.PublisherPropertyMap{
			"valid backpressure configuration": {
				config.PublisherPropertyBackPressureStrategy: config.PublisherPropertyBackPressureStrategyBufferWaitWhenFull,
			},
			"valid backpressure configuration type": {
				config.PublisherPropertyBackPressureStrategy: config.PublisherPropertyBackPressureStrategyBufferRejectWhenFull,
			},
			"valid backpressure buffer size type": {
				config.PublisherPropertyBackPressureBufferCapacity: 50,
			},
		}
		for key, val := range validConfigurations {
			validConfiguration := val
			It("succeeds to build with "+key, func() {
				requestReplyPublisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().FromConfigurationProvider(validConfiguration).Build()

				expected := &solace.IllegalArgumentError{}
				Expect(requestReplyPublisher).ShouldNot(Equal(nil))
				Expect(requestReplyPublisher.IsRunning()).To(BeFalse()) // running state should be false
				ExpectWithOffset(2, err).ToNot(HaveOccurred(), "Expected error to not have occurred")
				ExpectWithOffset(2, err).ToNot(BeAssignableToTypeOf(expected), fmt.Sprintf("Expected error of type %T to not be assignable of type %T", err, expected))
			})
		}

		It("can print the builder to a string and see the pointer", func() {
			builder := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder()
			str := fmt.Sprint(builder)
			Expect(str).ToNot(BeEmpty())
			Expect(str).To(ContainSubstring(fmt.Sprintf("%p", builder)))
		})

		// this may be a duplicate test but won't hurt to add it here for Request-Reply
		It("can print a topic to a string", func() {
			topicString := "request-reply-try-me"
			topic := resource.TopicOf(topicString)
			Expect(topic.String()).To(ContainSubstring(topicString))
		})

	})

	Context("with a connected messaging service", func() {
		var messageBuilder solace.OutboundMessageBuilder
		BeforeEach(func() {
			helpers.ConnectMessagingService(messagingService)
			messageBuilder = messagingService.MessageBuilder()
		})

		AfterEach(func() {
			if messagingService.IsConnected() {
				helpers.DisconnectMessagingService(messagingService)
			}
		})

		It("can print the publisher to a string and see the pointer", func() {
			publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			str := fmt.Sprint(publisher)
			Expect(str).ToNot(BeEmpty())
			Expect(str).To(ContainSubstring(fmt.Sprintf("%p", publisher)))
		})

		Describe("request-reply publisher termination tests", func() {
			topic := resource.TopicOf("hello/world")
			// topicSubscription := resource.TopicSubscriptionOf("hello/world")
			largeByteArray := make([]byte, 16384)
			timeOut := 5 * time.Second

			/*
				// A helper function to handle repliers for publishers
				receiverInstance := func(subscription *resource.TopicSubscription) {
					receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(subscription)
					Expect(err).ToNot(HaveOccurred())
					startErr := receiver.Start()
					Expect(startErr).ToNot(HaveOccurred())
					Expect(receiver.IsRunning()).To(BeTrue()) // running state should be true
					requestMessageHandler := func(message message.InboundMessage, replier solace.Replier) {
						if replier == nil { // the replier is only set when received message is request message that be replied to
							return
						}

						replyMsg, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
						Expect(err).ToNot(HaveOccurred())

						replier.Reply(replyMsg)
						// replyErr := replier.Reply(replyMsg)
						// Expect(replyErr).ToNot(HaveOccurred())
					}
					// have receiver push request messages to request message handler
					regErr := receiver.ReceiveAsync(requestMessageHandler)
					Expect(regErr).ToNot(HaveOccurred())
				}
			*/

			// A helper function to saturate a given publisher. Counts the number of published messages at the given int pointer
			// Returns a channel that is closed when the publisher receives an error from a call to Publish
			// Returns a channel that can be closed when the test completes, ie. if an error occurred
			publisherSaturation := func(publisher solace.RequestReplyMessagePublisher, publishedMessages *int) (publisherComplete, testComplete chan struct{}) {
				publisherComplete = make(chan struct{})
				testComplete = make(chan struct{})

				toPublish, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
				Expect(err).ToNot(HaveOccurred())

				publisherReplyHandler := func(message message.InboundMessage, userContext interface{}, err error) {
					if err == nil { // Good, a reply was received
						Expect(message).ToNot(BeNil())
						payload, _ := message.GetPayloadAsString()
						fmt.Printf("The reply inbound payload: %s\n", payload)
					} else if terr, ok := err.(*solace.TimeoutError); ok { // Not good, a timeout occurred and no reply was received
						// message should be nil
						// This handles the situation that the requester application did not receive a reply for the published message within the specified timeout.
						// This would be a good location for implementing resiliency or retry mechanisms.
						fmt.Printf("The message reply timed out with %s\n", terr)
					} else { // async error occurred.
						// panic(err)
						Expect(err).ToNot(BeNil())
					}
				}

				go func() {
					defer GinkgoRecover()
				loop:
					for {
						select {
						case <-testComplete:
							break loop
						default:
						}
						err := publisher.Publish(toPublish, publisherReplyHandler, topic, timeOut, nil /* properties */, nil /* usercontext */)

						if err != nil {
							Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
							break loop
						} else {
							(*publishedMessages)++
						}
					}
					close(publisherComplete)
				}()
				// allow the goroutine above to saturate the publisher
				time.Sleep(100 * time.Millisecond)
				return publisherComplete, testComplete
			}

			It("should publish all messages on graceful termination (no waiting for reply messages)", func() {
				publishedMessages := 0

				bufferSize := uint(1000)
				// publisher, err := messagingService.CreateDirectMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				// receiverInstance(topicSubscription) // start a receiver to reply back to the publisher
				publisherComplete, testComplete := publisherSaturation(publisher, &publishedMessages)
				defer close(testComplete)

				// allow the goroutine above to saturate the publisher
				time.Sleep(100 * time.Millisecond)

				publisherTerminate := publisher.TerminateAsync(30 * time.Second)

				select {
				case <-publisherComplete:
					// success
				case <-publisherTerminate:
					Fail("expected publisher to complete prior to termination completion")
				case <-time.After(15 * time.Second):
					Fail("timed out waiting for publisher to complete")
				}
				select {
				case err := <-publisherTerminate:
					Expect(err).ToNot(HaveOccurred())
				case <-time.After(15 * time.Second):
					Fail("timed out waiting for publisher to terminate")
				}
				Expect(publisher.IsTerminated()).To(BeTrue())
				// to account for the reply messages too
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", publishedMessages))
			})

			FIt("should have undelivered messages on ungraceful termination (no waiting for reply messages)", func() {
				publishedMessages := 0

				bufferSize := uint(10000)
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				publisherComplete, testComplete := publisherSaturation(publisher, &publishedMessages)
				defer close(testComplete)

				publisherTerminate := publisher.TerminateAsync(0 * time.Second)
				Eventually(publisherTerminate).Should(Receive(&err))
				helpers.ValidateError(err, &solace.IncompleteMessageDeliveryError{})

				Eventually(publisherComplete).Should(BeClosed())
				Expect(publisher.IsTerminated()).To(BeTrue())

				directSent := messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				directDropped := messagingService.Metrics().GetValue(metrics.PublishMessagesTerminationDiscarded)
				Expect(directSent).To(BeNumerically("<", publishedMessages))
				Expect(directDropped).To(BeNumerically(">", 0))
				Expect(directSent + directDropped).To(BeNumerically("==", publishedMessages))
			})

			/*
				It("should have undelivered messages on unsolicited termination", func() {
					publishedMessages := 0
					startTime := time.Now()
					bufferSize := uint(10000)
					publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
					// publisher, err := messagingService.CreateDirectMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
					Expect(err).ToNot(HaveOccurred())

					err = publisher.Start()
					Expect(err).ToNot(HaveOccurred())
					defer publisher.Terminate(0)

					terminationListenerCalled := make(chan solace.TerminationEvent)
					publisher.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
						terminationListenerCalled <- te
					})

					undeliveredCount := uint64(0)
					failedPublishCount := uint64(0)
					publisher.SetPublishFailureListener(func(fpe solace.FailedPublishEvent) {
						defer GinkgoRecover()
						if nativeError, ok := fpe.GetError().(*solace.NativeError); (ok && nativeError.SubCode() == subcode.CommunicationError) ||
							// Due to SOL-66163, the error may occasionally be nil when session disconnects occur
							// To avoid test failures we should be updating the undelivered count in this case
							// This should be reverted once SOL-66163 is fixed
							fpe.GetError() == nil {
							// we were terminated
							atomic.AddUint64(&undeliveredCount, 1)
						} else {
							// some other errsor occurred
							atomic.AddUint64(&failedPublishCount, 1)
						}
						Expect(fpe.GetDestination()).To(Equal(topic))
						Expect(fpe.GetTimeStamp()).To(BeTemporally(">", startTime))
						Expect(fpe.GetTimeStamp()).To(BeTemporally("<", time.Now()))
						Expect(fpe.GetMessage()).ToNot(BeNil())
						payload, ok := fpe.GetMessage().GetPayloadAsBytes()
						Expect(ok).To(BeTrue())
						Expect(payload).ToNot(BeNil())
						Expect(payload).To(HaveLen(len(largeByteArray)))
					})

					publisherComplete, testComplete := publisherSaturation(publisher, &publishedMessages)
					defer close(testComplete)

					shutdownTime := time.Now()
					helpers.ForceDisconnectViaSEMPv2(messagingService)

					Eventually(publisherComplete).Should(BeClosed())
					Eventually(publisher.IsTerminated).Should(BeTrue())

					select {
					case te := <-terminationListenerCalled:
						Expect(te.GetTimestamp()).To(BeTemporally(">", shutdownTime))
						Expect(te.GetTimestamp()).To(BeTemporally("<", time.Now()))
						// SOL-66163: a race condition in CCSMP may cause the error to be nil
						// helpers.ValidateNativeError(te.GetCause(), subcode.CommunicationError)
						Expect(te.GetMessage()).To(ContainSubstring("Publisher"))
					case <-time.After(100 * time.Millisecond):
						Fail("timed out waiting for termination listener to be called")
					}

					directSent := messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
					directDropped := messagingService.Metrics().GetValue(metrics.PublishMessagesTerminationDiscarded)
					Expect(directSent).To(BeNumerically("<", publishedMessages))
					Eventually(func() uint64 {
						return atomic.LoadUint64(&undeliveredCount)
					}, 10*time.Second).Should(BeNumerically("==", directDropped))

					// There is a known issue where there may be an additional message counted as undelivered.
					// Given a terminating publisher in buffered backpressure, it is possible that a message gets
					// queued in the publish buffer after message publishing has halted. This results in a situation
					// where the call to Publish fails but the message is still counted as discarded due to termination.
					Eventually(func() uint64 {
						return directSent + directDropped + atomic.LoadUint64(&failedPublishCount)
					}).Should(BeNumerically(">=", publishedMessages))
					Expect(directSent + directDropped + atomic.LoadUint64(&failedPublishCount)).Should(BeNumerically("<=", publishedMessages+1))

				})
				/*
					It("should have undelivered messages on messaging service shutdown", func() {
						publishedMessages := 0

						bufferSize := uint(100)
						publisher, err := messagingService.CreateDirectMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
						Expect(err).ToNot(HaveOccurred())

						err = publisher.Start()
						Expect(err).ToNot(HaveOccurred())
						defer publisher.Terminate(0)

						terminationListenerCalled := make(chan solace.TerminationEvent)
						publisher.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
							terminationListenerCalled <- te
						})

						undeliveredCount := uint64(0)
						failedPublishCount := uint64(0)
						publisher.SetPublishFailureListener(func(fpe solace.FailedPublishEvent) {
							if _, ok := fpe.GetError().(*solace.ServiceUnreachableError); ok {
								// we were terminated
								atomic.AddUint64(&undeliveredCount, 1)
							} else {
								// some other error occurred
								atomic.AddUint64(&failedPublishCount, 1)
							}
						})

						publisherComplete, testComplete := publisherSaturation(publisher, &publishedMessages)
						defer close(testComplete)

						shutdownTime := time.Now()
						helpers.DisconnectMessagingService(messagingService)

						Eventually(publisherComplete).Should(BeClosed())
						Eventually(publisher.IsTerminated).Should(BeTrue())

						select {
						case te := <-terminationListenerCalled:
							Expect(te.GetTimestamp()).To(BeTemporally(">", shutdownTime))
							Expect(te.GetTimestamp()).To(BeTemporally("<", time.Now()))
							Expect(te.GetCause()).To(BeAssignableToTypeOf(&solace.ServiceUnreachableError{}))
						case <-time.After(100 * time.Millisecond):
							Fail("timed out waiting for termination listener to be called")
						}

						directSent := messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
						directDropped := messagingService.Metrics().GetValue(metrics.PublishMessagesTerminationDiscarded)
						Expect(directSent).To(BeNumerically("<", publishedMessages))
						Eventually(func() uint64 {
							return atomic.LoadUint64(&undeliveredCount)
						}, 10*time.Second).Should(BeNumerically("==", directDropped))
						// There is a known issue where there may be an additional message counted as undelivered.
						// Given a terminating publisher in buffered backpressure, it is possible that a message gets
						// queued in the publish buffer after message publishing has halted. This results in a situation
						// where the call to Publish fails but the message is still counted as discarded due to termination.
						Eventually(func() uint64 {
							return directSent + directDropped + atomic.LoadUint64(&failedPublishCount)
						}).Should(BeNumerically(">=", publishedMessages))
						Expect(directSent + directDropped + atomic.LoadUint64(&failedPublishCount)).Should(BeNumerically("<=", publishedMessages+1))
					})
			*/
		})

	})

})
