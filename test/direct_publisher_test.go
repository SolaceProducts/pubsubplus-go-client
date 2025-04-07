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
	"sync"
	"sync/atomic"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/helpers"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("DirectPublisher", func() {

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
				config.PublisherPropertyBackPressureStrategy: 123,
			},
			"invalid backpressure buffer size type": {
				config.PublisherPropertyBackPressureBufferCapacity: "asdf",
			},
		}
		for key, val := range invalidConfigurations {
			invalidConfiguration := val
			It("fails to build with "+key, func() {
				_, err := messagingService.CreateDirectMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
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
				_, err := messagingService.CreateDirectMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
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
				_, err := messagingService.CreateDirectMessagePublisherBuilder().FromConfigurationProvider(invalidConfiguration).Build()
				helpers.ValidateError(err, &solace.InvalidConfigurationError{}, "required property")
			})
		}

		It("can print the builder to a string and see the pointer", func() {
			builder := messagingService.CreateDirectMessagePublisherBuilder()
			str := fmt.Sprint(builder)
			Expect(str).ToNot(BeEmpty())
			Expect(str).To(ContainSubstring(fmt.Sprintf("%p", builder)))
		})

		It("can print a topic to a string", func() {
			topicString := "try-me"
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
			publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			str := fmt.Sprint(publisher)
			Expect(str).ToNot(BeEmpty())
			Expect(str).To(ContainSubstring(fmt.Sprintf("%p", publisher)))
		})

		Describe("basic publish test", func() {
			startFunctions := map[string](func(publisher solace.DirectMessagePublisher) <-chan error){
				"sync": func(publisher solace.DirectMessagePublisher) <-chan error {
					c := make(chan error)
					go func() {
						c <- publisher.Start()
					}()
					return c
				},
				"async": func(publisher solace.DirectMessagePublisher) <-chan error {
					return publisher.StartAsync()
				},
				"callback": func(publisher solace.DirectMessagePublisher) <-chan error {
					c := make(chan error)
					publisher.StartAsyncCallback(func(dmp solace.DirectMessagePublisher, e error) {
						defer GinkgoRecover()
						Expect(dmp).To(Equal(publisher))
						c <- e
					})
					return c
				},
			}
			gracePeriod := 5 * time.Second
			terminateFunctions := map[string](func(publisher solace.DirectMessagePublisher) <-chan error){
				"sync": func(publisher solace.DirectMessagePublisher) <-chan error {
					c := make(chan error)
					go func() {
						c <- publisher.Terminate(gracePeriod)
					}()
					return c
				},
				"async": func(publisher solace.DirectMessagePublisher) <-chan error {
					return publisher.TerminateAsync(gracePeriod)
				},
				"callback": func(publisher solace.DirectMessagePublisher) <-chan error {
					c := make(chan error)
					publisher.TerminateAsyncCallback(gracePeriod, func(e error) {
						c <- e
					})
					return c
				},
			}
			for startName, fn := range startFunctions {
				for terminateName, fn2 := range terminateFunctions {
					start := fn
					terminate := fn2
					It("can start and terminate with start "+startName+" and terminate "+terminateName, func() {
						publisher, err := messagingService.CreateDirectMessagePublisherBuilder().Build()
						Expect(err).ToNot(HaveOccurred())
						// check that not started
						helpers.ValidateReadyState(publisher, false, false, false, false)
						// start and check state
						Eventually(start(publisher)).Should(Receive(Not(HaveOccurred())))
						helpers.ValidateReadyState(publisher, true, true, false, false)
						// try using publisher
						publisher.PublishString("hello world", resource.TopicOf("hello/world"))
						Eventually(func() int64 {
							return helpers.GetClient(messagingService).DataRxMsgCount
						}).Should(BeNumerically("==", 1))
						// terminate and check state
						Eventually(terminate(publisher)).Should(Receive(Not(HaveOccurred())))
						helpers.ValidateReadyState(publisher, false, false, false, true)
					})
					It("can start and terminate idempotently with start "+startName+" and terminate "+terminateName, func() {
						publisher, err := messagingService.CreateDirectMessagePublisherBuilder().Build()
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
						publisher.PublishString("hello world", resource.TopicOf("hello/world"))
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
			for startName, fn := range startFunctions {
				start := fn
				It("should fail to start when messaging service is disconnected using start "+startName, func() {
					publisher, err := messagingService.CreateDirectMessagePublisherBuilder().Build()
					Expect(err).ToNot(HaveOccurred())

					messagingService.Disconnect()

					Eventually(start(publisher)).Should(Receive(&err))
					Expect(err).To(HaveOccurred())
					Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
				})
				It("should fail to start when messaging service is down using start "+startName, func() {
					publisher, err := messagingService.CreateDirectMessagePublisherBuilder().Build()
					Expect(err).ToNot(HaveOccurred())

					helpers.ForceDisconnectViaSEMPv2(messagingService)
					Eventually(messagingService.IsConnected).Should(BeFalse())
					Expect(messagingService.Disconnect()).ToNot(HaveOccurred())

					helpers.ValidateChannelError(start(publisher), &solace.IllegalStateError{})
				})
			}
			for terminateName, fn := range terminateFunctions {
				terminate := fn
				It("should be able to terminate when not started using terminate "+terminateName, func() {
					publisher := helpers.NewDirectPublisher(messagingService)
					Eventually(terminate(publisher)).Should(Receive(BeNil()))
					Expect(publisher.IsTerminated()).To(BeTrue())
					helpers.ValidateError(publisher.Start(), &solace.IllegalStateError{})
				})
			}
			It("should fail to publish on unstarted publisher", func() {
				publisher := helpers.NewDirectPublisher(messagingService)
				helpers.ValidateError(
					publisher.Publish(helpers.NewMessage(messagingService), resource.TopicOf("hello/world")),
					&solace.IllegalStateError{},
				)
			})
			It("should fail to publish on terminated publisher", func() {
				publisher := helpers.NewDirectPublisher(messagingService)

				Expect(publisher.Start()).ToNot(HaveOccurred())
				Expect(publisher.Terminate(gracePeriod)).ToNot(HaveOccurred())

				helpers.ValidateError(publisher.Publish(helpers.NewMessage(messagingService), resource.TopicOf("hello/world")), &solace.IllegalStateError{})
			})
		})

		Describe("publisher backpressure tests", func() {
			var builder solace.DirectMessagePublisherBuilder

			BeforeEach(func() {
				builder = messagingService.CreateDirectMessagePublisherBuilder()
			})

			backpressureConfigurations := map[string]func(builder solace.DirectMessagePublisherBuilder){
				"with backpressure wait": func(builder solace.DirectMessagePublisherBuilder) {
					builder.OnBackPressureWait(1000)
				},
				"with backpressure reject": func(builder solace.DirectMessagePublisherBuilder) {
					builder.OnBackPressureReject(1000)
				},
				"with backpressure direct": func(builder solace.DirectMessagePublisherBuilder) {
					builder.OnBackPressureReject(0)
				},
			}
			for test, fn := range backpressureConfigurations {
				setupBuilder := fn
				It("should be able to publish a message successfully "+test, func() {
					setupBuilder(builder)
					publisher, err := builder.Build()
					Expect(err).ToNot(HaveOccurred())
					// start a publisher
					Expect(publisher.Start()).ToNot(HaveOccurred())
					// get client
					client := helpers.GetClient(messagingService)
					Expect(client.DataRxMsgCount).To(Equal(int64(0)))
					// send message
					msg, err := messageBuilder.BuildWithStringPayload("Hello World")
					Expect(err).ToNot(HaveOccurred())
					topic := resource.TopicOf("try-me")
					publisher.Publish(msg, topic)
					// check that the message was sent
					client = helpers.GetClient(messagingService)
					Expect(client.DataRxMsgCount).To(Equal(int64(1)))
					Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(Equal(uint64(1)))
					// terminate
					msg.Dispose()
					err = publisher.Terminate(30 * time.Second)
					Expect(err).To(BeNil())
				})
			}

			It("should reject messages when backpressure buffer fills up", func() {
				bufferSize := uint(2)
				publishedMessages := 0
				largeByteArray := make([]byte, 16384)
				topic := resource.TopicOf("hello/world")
				publisher, err := builder.OnBackPressureReject(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				ready := make(chan interface{}, 1)
				publisher.SetPublisherReadinessListener(func() {
					select {
					case ready <- nil:
					default:
					}
				})

				// Fill up the backpressure
				toPublish, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
				Expect(err).ToNot(HaveOccurred())
				for i := uint(0); i < bufferSize; i++ {
					err := publisher.Publish(toPublish, topic)
					Expect(err).ToNot(HaveOccurred())
					publishedMessages++
				}

				// have some flex room in case scheduling isn't on our side
				maxRetries := 5
				for retries := 0; retries < maxRetries; retries++ {
					err = publisher.Publish(toPublish, topic)
					if err != nil {
						break
					}
					publishedMessages++
				}
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.PublisherOverflowError{}))
				Eventually(ready, 1*time.Minute).Should(Receive())
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				}, 1*time.Minute).Should(BeNumerically(">=", publishedMessages))
			})

			It("should wait to publish messages when backpressure buffer fills up", func() {
				bufferSize := uint(2)
				publishedMessages := 0
				largeByteArray := make([]byte, 40000000)
				topic := resource.TopicOf("hello/world")
				publisher, err := builder.OnBackPressureWait(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				// Fill up the backpressure
				toPublish, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
				Expect(err).ToNot(HaveOccurred())
				for i := uint(0); i < bufferSize; i++ {
					err := publisher.Publish(toPublish, topic)
					Expect(err).ToNot(HaveOccurred())
					publishedMessages++
				}

				retries := 0
				maxRetries := 5
				for {
					complete := make(chan error)
					go func() {
						err = publisher.Publish(toPublish, topic)
						if err != nil {
							publishedMessages++
						}
						complete <- err
					}()
					select {
					case <-complete:
						retries++
						if retries == maxRetries {
							Fail("Never blocked for more than 10ms waiting to add message to buffer!")
						}
						continue
					case <-time.After(10 * time.Millisecond):
						// success
					}

					select {
					case err := <-complete:
						Expect(err).ToNot(HaveOccurred())
						// success
					case <-time.After(20 * time.Second):
						Fail("blocked for longer than max timeout")
					}
					break
				}
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				}, 1*time.Minute).Should(BeNumerically(">=", publishedMessages))
			})

			It("should get a ready event after each call to notify when ready", func() {
				bufferSize := uint(2)
				publishedMessages := 0
				largeByteArray := make([]byte, 16384)
				topic := resource.TopicOf("hello/world")
				publisher, err := builder.OnBackPressureReject(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

				readyMutex := &sync.Mutex{}
				readyCond := sync.NewCond(readyMutex)
				publisher.SetPublisherReadinessListener(func() {
					readyMutex.Lock()
					defer readyMutex.Unlock()
					readyCond.Broadcast()
				})

				// Fill up the backpressure
				toPublish, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
				Expect(err).ToNot(HaveOccurred())
				rejected := false
				for i := uint(0); i < 128*bufferSize; i++ {
					err := publisher.Publish(toPublish, topic)
					if err != nil {
						rejected = true
						Expect(err).To(BeAssignableToTypeOf(&solace.PublisherOverflowError{}))
						ready := make(chan struct{})
						waiting := make(chan struct{})
						go func() {
							readyMutex.Lock()
							defer readyMutex.Unlock()
							close(waiting)
							readyCond.Wait()
							close(ready)
						}()
						Eventually(waiting).Should(BeClosed())
						// guarantee a ready notification
						publisher.NotifyWhenReady()
						Eventually(ready, 5*time.Second).Should(BeClosed())
					} else {
						publishedMessages++
					}
				}
				Expect(rejected).To(BeTrue())

				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				}, 1*time.Minute).Should(BeNumerically(">=", publishedMessages))
			})
		})
		Describe("publisher termination tests", func() {
			topic := resource.TopicOf("hello/world")
			largeByteArray := make([]byte, 16384)

			// A helper function to saturate a given publisher. Counts the number of published messages at the given int pointer
			// Returns a channel that is closed when the publisher receives an error from a call to Publish
			// Returns a channel that can be closed when the test completes, ie. if an error occurred
			publisherSaturation := func(publisher solace.DirectMessagePublisher, publishedMessages *int) (publisherComplete, testComplete chan struct{}) {
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
						err := publisher.Publish(toPublish, topic)
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

			It("should publish all messages on graceful termination", func() {
				publishedMessages := 0

				bufferSize := uint(1000)
				publisher, err := messagingService.CreateDirectMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())
				defer publisher.Terminate(0)

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
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", publishedMessages))
			})

			It("should have undelivered messages on ungraceful termination", func() {
				publishedMessages := 0

				bufferSize := uint(10000)
				publisher, err := messagingService.CreateDirectMessagePublisherBuilder().OnBackPressureWait(bufferSize).Build()
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
			It("should have undelivered messages on unsolicited termination", func() {
				publishedMessages := 0
				startTime := time.Now()
				bufferSize := uint(10000)
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
		})
	})

})
