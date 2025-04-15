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
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().FromConfigurationProvider(validConfiguration).Build()

				expected := &solace.IllegalArgumentError{}
				Expect(publisher).ShouldNot(Equal(nil))
				Expect(publisher.IsRunning()).To(BeFalse()) // running state should be false
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
			publishTopic := resource.TopicOf("hello/world")
			topicSubscription := resource.TopicSubscriptionOf("hello/world")
			largeByteArray := make([]byte, 16384)
			timeOut := 5 * time.Second

			// A handler for the request-reply publisher
			publisherReplyHandler := func(message message.InboundMessage, userContext interface{}, err error) {
				if err == nil { // Good, a reply was received
					Expect(message).ToNot(BeNil())
				} else {
					// message should be nil
					Expect(message).To(BeNil())
					Expect(err).ToNot(BeNil())
				}
			}

			// A helper function to saturate a given publisher (fill up its internal buffers).
			// Counts the number of published messages at the given int pointer
			// Returns a channel that is closed when the publisher receives an error from a call to Publish
			// Returns a channel that can be closed when the test completes, ie. if an error occurred
			publisherSaturation := func(publisher solace.RequestReplyMessagePublisher, bufferSize uint, publishedMessages *int) (publisherSaturated, publisherComplete, testComplete chan struct{}) {
				isSaturated := false
				publisherSaturated = make(chan struct{})
				publisherComplete = make(chan struct{})
				testComplete = make(chan struct{})

				toPublish, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
				Expect(err).ToNot(HaveOccurred())

				go func() {
					defer GinkgoRecover()
				loop:
					for {
						select {
						case <-testComplete:
							break loop
						default:
						}
						err := publisher.Publish(toPublish, publisherReplyHandler, publishTopic, timeOut, nil /* properties */, nil /* usercontext */)

						if err != nil {
							Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
							break loop
						} else {
							(*publishedMessages)++
						}

						// buffer is at least halfway filled
						if !isSaturated && (uint(*publishedMessages) > bufferSize/2) {
							close(publisherSaturated)
							isSaturated = true
						}
					}
					close(publisherComplete)
				}()
				return publisherSaturated, publisherComplete, testComplete
			}

			It("should publish all messages on graceful termination (no waiting for reply messages)", func() {
				publishedMessages := 0

				bufferSize := uint(1000)
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				publisherSaturated, publisherComplete, testComplete := publisherSaturation(publisher, bufferSize, &publishedMessages)
				defer close(testComplete)

				// allow the goroutine above to saturate the publisher
				select {
				case <-publisherComplete:
					// block until publish complete
					Fail("Expected publisher to not be complete")
				case <-publisherSaturated:
					// allow the goroutine above to saturate the publisher (at least halfway filled)
				case <-time.After(2 * time.Second):
					// should not timeout while saturating the publisher
					Fail("Not expected to timeout while saturating publisher; Should not get here")
				}

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

			It("should have undelivered messages on ungraceful termination (no waiting for reply messages)", func() {
				publishedMessages := 0

				bufferSize := uint(5000)
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				publisherSaturated, publisherComplete, testComplete := publisherSaturation(publisher, bufferSize, &publishedMessages)
				defer close(testComplete)

				// allow the goroutine above to saturate the publisher
				select {
				case <-publisherComplete:
					// block until publish complete
					Fail("Expected publisher to not be complete")
				case <-publisherSaturated:
					// allow the goroutine above to saturate the publisher (at least halfway filled)
				case <-time.After(5 * time.Second):
					// should not timeout while saturating the publisher
					Fail("Not expected to timeout while saturating publisher; Should not get here")
				}

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

			It("should have undelivered messages on unsolicited termination of messaging service", func() {
				publishedMessages := 0
				bufferSize := uint(10000)
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				terminationListenerCalled := make(chan solace.TerminationEvent)
				publisher.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
					terminationListenerCalled <- te
				})

				_, publisherComplete, testComplete := publisherSaturation(publisher, bufferSize, &publishedMessages)
				defer close(testComplete)

				shutdownTime := time.Now()
				helpers.ForceDisconnectViaSEMPv2(messagingService)

				Eventually(publisherComplete).Should(BeClosed())
				Eventually(publisher.IsTerminated).Should(BeTrue())

				select {
				case te := <-terminationListenerCalled:
					Expect(te.GetTimestamp()).To(BeTemporally(">", shutdownTime))
					Expect(te.GetTimestamp()).To(BeTemporally("<", time.Now()))
					// Expect(te.GetCause()).To(BeAssignableToTypeOf(&solace.NativeError{}))
					// SOL-66163: a race condition in CCSMP may cause the error to be nil
					// helpers.ValidateNativeError(te.GetCause(), subcode.CommunicationError)
					Expect(te.GetMessage()).To(ContainSubstring("Publisher"))
				case <-time.After(100 * time.Millisecond):
					Fail("timed out waiting for termination listener to be called")
				}

				publishSent := messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				Expect(publishSent).To(BeNumerically("<", publishedMessages))
			})

			It("should have undelivered messages on messaging service shutdown/disconnection", func() {
				publishedMessages := 0
				bufferSize := uint(100)
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				terminationListenerCalled := make(chan solace.TerminationEvent)
				publisher.SetTerminationNotificationListener(func(te solace.TerminationEvent) {
					terminationListenerCalled <- te
				})

				_, publisherComplete, testComplete := publisherSaturation(publisher, bufferSize, &publishedMessages)
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

				publishSent := messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				Expect(publishSent).To(BeNumerically("<", publishedMessages))
			})

			startFunctions := map[string](func(publisher solace.RequestReplyMessagePublisher) <-chan error){
				"sync": func(publisher solace.RequestReplyMessagePublisher) <-chan error {
					c := make(chan error)
					go func() {
						c <- publisher.Start()
					}()
					return c
				},
				"async": func(publisher solace.RequestReplyMessagePublisher) <-chan error {
					return publisher.StartAsync()
				},
				"callback": func(publisher solace.RequestReplyMessagePublisher) <-chan error {
					c := make(chan error)
					publisher.StartAsyncCallback(func(dmp solace.RequestReplyMessagePublisher, e error) {
						defer GinkgoRecover()
						Expect(dmp).To(Equal(publisher))
						c <- e
					})
					return c
				},
			}

			// gracePeriod := 5 * time.Second
			terminateFunctions := map[string](func(publisher solace.RequestReplyMessagePublisher, gracePeriod time.Duration) <-chan error){
				"sync": func(publisher solace.RequestReplyMessagePublisher, gracePeriod time.Duration) <-chan error {
					c := make(chan error)
					go func() {
						c <- publisher.Terminate(gracePeriod)
					}()
					return c
				},
				"async": func(publisher solace.RequestReplyMessagePublisher, gracePeriod time.Duration) <-chan error {
					return publisher.TerminateAsync(gracePeriod)
				},
				"callback": func(publisher solace.RequestReplyMessagePublisher, gracePeriod time.Duration) <-chan error {
					c := make(chan error)
					publisher.TerminateAsyncCallback(gracePeriod, func(e error) {
						c <- e
					})
					return c
				},
			}

			// for the success paths
			for startName, fn := range startFunctions {
				for terminateName, fn2 := range terminateFunctions {
					start := fn
					terminate := fn2
					It("can start and terminate with start "+startName+" and terminate "+terminateName+" with subscribed reply receiver on topic", func() {
						// build a request replier
						receiver := helpers.NewRequestReplyMessageReceiver(messagingService, topicSubscription)
						helpers.StartRequestReplyMessageReceiverWithDefault(messagingService, receiver)
						publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
						Expect(err).ToNot(HaveOccurred())
						// check that not started
						helpers.ValidateReadyState(publisher, false, false, false, false)
						// start and check state
						Eventually(start(publisher)).Should(Receive(Not(HaveOccurred())))
						helpers.ValidateReadyState(publisher, true, true, false, false)
						// try using publisher
						publisher.PublishString("hello world", publisherReplyHandler, publishTopic, timeOut, nil /* usercontext */)
						Eventually(func() int64 {
							return helpers.GetClient(messagingService).DataRxMsgCount
						}).Should(BeNumerically("==", 2))
						// terminate and check state
						Eventually(terminate(publisher, 5*time.Second)).Should(Receive(Not(HaveOccurred())))
						helpers.ValidateReadyState(publisher, false, false, false, true)
						receiver.Terminate(1 * time.Second) // terminate the receiver
					})
					It("can start and terminate idempotently with start "+startName+" and terminate "+terminateName+" with subscribed reply receiver on topic", func() {
						// build a request replier
						receiver := helpers.NewRequestReplyMessageReceiver(messagingService, topicSubscription)
						helpers.StartRequestReplyMessageReceiverWithDefault(messagingService, receiver)
						publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
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
						publisher.PublishString("hello world", publisherReplyHandler, publishTopic, timeOut, nil /* usercontext */)
						Eventually(func() int64 {
							return helpers.GetClient(messagingService).DataRxMsgCount
						}).Should(BeNumerically("==", 2))
						// terminate and check state
						c1 = terminate(publisher, 5*time.Second)
						c2 = terminate(publisher, 5*time.Second)
						Eventually(c1).Should(Receive(Not(HaveOccurred())))
						Eventually(c2).Should(Receive(Not(HaveOccurred())))
						helpers.ValidateReadyState(publisher, false, false, false, true)
						c1 = start(publisher)
						c2 = start(publisher)
						Eventually(c1).Should(Receive(BeAssignableToTypeOf(&solace.IllegalStateError{})))
						Eventually(c2).Should(Receive(BeAssignableToTypeOf(&solace.IllegalStateError{})))
						receiver.Terminate(1 * time.Second) // terminate the receiver
					})
					It("can start and terminate with start "+startName+" and terminate "+terminateName+" without subscribed reply receiver and zero grace period", func() {
						publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
						Expect(err).ToNot(HaveOccurred())
						// check that not started
						helpers.ValidateReadyState(publisher, false, false, false, false)
						// start and check state
						Eventually(start(publisher)).Should(Receive(Not(HaveOccurred())))
						helpers.ValidateReadyState(publisher, true, true, false, false)
						// try using publisher
						publisher.PublishString("hello world", publisherReplyHandler, publishTopic, timeOut, nil /* usercontext */)
						Eventually(func() int64 {
							return helpers.GetClient(messagingService).DataRxMsgCount
						}).Should(BeNumerically("==", 1))
						// terminate and check state
						Eventually(terminate(publisher, 0*time.Second)).Should(Receive(Not(HaveOccurred())))
						helpers.ValidateReadyState(publisher, false, false, false, true)
					})
					It("can start and terminate with start "+startName+" and terminate "+terminateName+" without subscribed reply receiver and > 0 grace period", func() {
						publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
						Expect(err).ToNot(HaveOccurred())
						// check that not started
						helpers.ValidateReadyState(publisher, false, false, false, false)
						// start and check state
						Eventually(start(publisher)).Should(Receive(Not(HaveOccurred())))
						helpers.ValidateReadyState(publisher, true, true, false, false)
						// try using publisher
						publisher.PublishString("hello world", publisherReplyHandler, publishTopic, timeOut, nil /* usercontext */)
						Eventually(func() int64 {
							return helpers.GetClient(messagingService).DataRxMsgCount
						}).Should(BeNumerically("==", 1))
						// terminate and check state
						terminateChan := terminate(publisher, 5*time.Second)
						Eventually(terminateChan, "5000ms").Should(Receive(BeNil()))
						helpers.ValidateReadyState(publisher, false, false, false, true)
					})
				}
			}

			// more success paths
			for terminateName, fn := range terminateFunctions {
				terminate := fn
				It("should be able to terminate when not started using terminate "+terminateName, func() {
					publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
					Expect(err).ToNot(HaveOccurred(), "Encountered error while building request-reply publisher")
					Eventually(terminate(publisher, 5*time.Second)).Should(Receive(BeNil()))
					Expect(publisher.IsTerminated()).To(BeTrue())
					helpers.ValidateError(publisher.Start(), &solace.IllegalStateError{})
				})
			}

			// failure paths
			It("should fail to publish on unstarted publisher", func() {
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
				Expect(err).ToNot(HaveOccurred(), "Encountered error while building request-reply publisher")
				helpers.ValidateError(
					publisher.Publish(helpers.NewMessage(messagingService), publisherReplyHandler, publishTopic, timeOut, nil /* properties */, nil /* usercontext */),
					&solace.IllegalStateError{},
				)
			})
			It("should fail to publish on terminated publisher", func() {
				publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
				Expect(err).ToNot(HaveOccurred(), "Encountered error while building request-reply publisher")

				Expect(publisher.Start()).ToNot(HaveOccurred())
				Expect(publisher.Terminate(gracePeriod)).ToNot(HaveOccurred())

				helpers.ValidateError(publisher.Publish(helpers.NewMessage(messagingService), publisherReplyHandler, publishTopic, timeOut, nil /* properties */, nil /* usercontext */), &solace.IllegalStateError{})
			})

			// for the failure paths
			for startName, fn := range startFunctions {
				start := fn
				It("should fail to start when messaging service is disconnected using start "+startName, func() {
					publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
					Expect(err).ToNot(HaveOccurred())

					messagingService.Disconnect()
					Eventually(start(publisher)).Should(Receive(&err))
					Expect(err).To(HaveOccurred())
					Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
				})
				It("should fail to start when messaging service is down using start "+startName, func() {
					publisher, err := messagingService.RequestReply().CreateRequestReplyMessagePublisherBuilder().Build()
					Expect(err).ToNot(HaveOccurred())

					helpers.ForceDisconnectViaSEMPv2(messagingService)
					Eventually(messagingService.IsConnected).Should(BeFalse())
					Expect(messagingService.Disconnect()).ToNot(HaveOccurred())

					helpers.ValidateChannelError(start(publisher), &solace.IllegalStateError{})
				})
			}
		})

	})

})
