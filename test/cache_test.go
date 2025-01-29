// pubsubplus-go-client
//
// Copyright 2025 Solace Corporation. All rights reserved.
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
	"sync/atomic"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/logging"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func CheckCache() {
	if !testcontext.CacheEnabled() {
		Skip("The infrastructure required for running cache tests is not available, skipping this test since it requires a cache.")
	}
}

func CheckCacheProxy() {
	if !testcontext.CacheProxyEnabled() {
		Skip("The infrastructure required for running cache proxy tests is not available, skipping this test since it requires a cache proxy.")
	}
}

var _ = Describe("Cache Strategy", func() {
	logging.SetLogLevel(logging.LogLevelDebug)
	Describe("When the cache is available and configured", func() {
		var messagingService solace.MessagingService
		var receiver solace.DirectMessageReceiver
		/* NOTE: deferredOperation is used for conducting operations after termination, such as closing a channel
		 * that is may be used during termination but that is declared within a single test case because its use is
		 * specific to that test case. If the closing of this channel were handled within the test case using a `defer`,
		 * when termination tried to access the channel in `AfterEach`, it would panic.
		 */
		var deferredOperation func()
		BeforeEach(func() {
			logging.SetLogLevel(logging.LogLevelDebug)
			CheckCache() // skips test with message if cache image is not available
			helpers.InitAllCacheClustersWithMessages()
			var err error
			messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultCacheConfiguration()).Build()
			Expect(err).To(BeNil())
			err = messagingService.Connect()
			Expect(err).To(BeNil())
			receiver, err = messagingService.CreateDirectMessageReceiverBuilder().Build()
			Expect(err).To(BeNil())
			err = receiver.Start()
			Expect(err).To(BeNil())
			deferredOperation = nil
		})
		AfterEach(func() {
			var err error
			if receiver.IsRunning() {
				err = receiver.Terminate(0)
				Expect(err).To(BeNil())
			}
			Expect(receiver.IsRunning()).To(BeFalse())
			Expect(receiver.IsTerminated()).To(BeTrue())
			if messagingService.IsConnected() {
				err = messagingService.Disconnect()
				Expect(err).To(BeNil())
			}
			Expect(messagingService.IsConnected()).To(BeFalse())
			if deferredOperation != nil {
				deferredOperation()
			}
		})
		It("a direct receiver should get an error when trying to send an invalid cache request", func() {
			/* NOTE: This test also asserts that the receiver can terminate after a failed attempt to send a cache
			 * request.
			 */
			var cacheRequestID message.CacheRequestID = 0
			numExpectedCacheRequestsSent := 0
			numExpectedCacheRequestsFailed := 0
			numExpectedCacheRequestsSucceeded := 0
			trivialCacheName := "trivial cache name"
			trivialTopic := "trivial topic"
			strategy := resource.AsAvailable
			invalidCacheRequestConfig := helpers.GetInvalidCacheRequestConfig(strategy, trivialCacheName, trivialTopic)
			channel, err := receiver.RequestCachedAsync(invalidCacheRequestConfig, cacheRequestID)
			Expect(channel).To(BeNil())
			Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
			callback := func(solace.CacheResponse) {
				Fail("This callback function should never run!")
			}
			err = receiver.RequestCachedAsyncWithCallback(invalidCacheRequestConfig, cacheRequestID, callback)
			Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", numExpectedCacheRequestsSent))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", numExpectedCacheRequestsFailed))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", numExpectedCacheRequestsSucceeded))
		})
		It("a direct receiver should be able to submit multiple concurrent cache requests with the same cache request ID without error", func() {
			err := receiver.ReceiveAsync(func(_ message.InboundMessage) {})
			Expect(err).To(BeNil())
			/* NOTE: We don't need to run this test for both channel and callback types because we're only really
			 * testing the call to submit a cache request with duplicate cache request IDs. The method of
			 * processing the cache response should not be affected by duplicate cache request IDs since the ID is
			 * only used by the application for correlation, and not by the API. We do assert that we receive the
			 * cache response, but only as a means of ensuring that the cache request was sent properly in CCSMP.
			 */
			cacheRequestID := message.CacheRequestID(1)
			numExpectedCachedMessages := 3
			/* NOTE: delay will give us time to have concurrent cache requests with the same ID */
			delay := 2000
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
			cacheName := fmt.Sprintf("MaxMsgs%d/delay=%d,", numExpectedCachedMessages, delay)
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.AsAvailable, cacheName, resource.TopicSubscriptionOf(topic), int32(delay+1000), helpers.ValidMaxCachedMessages, helpers.ValidCachedMessageAge)
			channelOne, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(channelOne).ToNot(BeNil())
			Expect(err).To(BeNil())
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }).Should(BeNumerically("==", 1))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))

			channelTwo, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(channelTwo).ToNot(BeNil())
			Expect(err).To(BeNil())
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }).Should(BeNumerically("==", 2))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))

			/* NOTE: This just asserts that we didn't immediately get a failure response due to an error on send. The
			 * is intended to be extremely short, but otherwise of arbitrary value.
			 */
			Consistently(channelOne, "1ms").ShouldNot(Receive())
			Consistently(channelTwo, "1ms").ShouldNot(Receive())

			/* NOTE: Assert that the cache response was received. */
			var cacheResponse1 solace.CacheResponse
			Eventually(channelOne, delay+1000).Should(Receive(&cacheResponse1))
			Expect(cacheResponse1).ToNot(BeNil())
			var cacheResponse2 solace.CacheResponse
			Eventually(channelTwo, delay+1000).Should(Receive(&cacheResponse2))
			Expect(cacheResponse2).ToNot(BeNil())
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 2))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 2))
		})
		It("a direct receiver should be able to submit multiple consecutive cache requests with the same cache request ID without error", func() {
			err := receiver.ReceiveAsync(func(message.InboundMessage) {})
			Expect(err).To(BeNil())
			cacheRequestID := message.CacheRequestID(1)
			numExpectedCachedMessages := 3
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
			cacheName := fmt.Sprintf("MaxMsgs%d", numExpectedCachedMessages)
			cacheRequestConfig := helpers.GetValidCacheRequestConfig(resource.AsAvailable, cacheName, topic)

			channelOne, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(channelOne).ToNot(BeNil())
			Expect(err).To(BeNil())
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "5s").Should(BeNumerically("==", 1))
			var cacheResponseOne solace.CacheResponse
			Eventually(channelOne, "10s").Should(Receive(&cacheResponseOne))
			Expect(cacheResponseOne).ToNot(BeNil())
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 1))

			channelTwo, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(channelTwo).ToNot(BeNil())
			Expect(err).To(BeNil())
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "5s").Should(BeNumerically("==", 2))
			var cacheResponseTwo solace.CacheResponse
			Eventually(channelTwo, "10s").Should(Receive(&cacheResponseTwo))
			Expect(cacheResponseTwo).ToNot(BeNil())
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 2))
		})
		It("a direct receiver that tries to submit more than the maximum number of cache requests should get an IllegalStateError", func() {
			err := receiver.ReceiveAsync(func(message.InboundMessage) {})
			Expect(err).To(BeNil())
			maxCacheRequests := 1024
			/* NOTE: First we will fill the internal buffer, then we will try one more and assert an error */
			numExpectedCachedMessages := 3
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
			cacheName := fmt.Sprintf("MaxMsgs%d", numExpectedCachedMessages)
			cacheRequestConfig := helpers.GetValidCacheRequestConfig(resource.AsAvailable, cacheName, topic)
			cacheResponseSignalChan := make(chan solace.CacheResponse)
			callback := func(cacheResponse solace.CacheResponse) {
				cacheResponseSignalChan <- cacheResponse
			}
			for i := 0; i <= maxCacheRequests; i++ {
				cacheRequestID := message.CacheRequestID(i)
				err := receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, callback)
				Expect(err).To(BeNil())
			}
			/* NOTE: We only need to assert at the end of the loop because we only care about the state at this point.
			 * We assert that there are maxCacheRequests+1 successful/sent cache requests, because the max cache
			 * requests limit is based on an internal buffer whose oldest item is given to the callback, freeing up the
			 * additional slot.
			 * callback          buffer (assume buffer size 4 for example)       num cache requests sent/succeeded
			 * |*|                           |*|*|*|*|                                       5 = 4 + 1
			 */
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }).Should(BeNumerically("==", maxCacheRequests+1))
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded) }).Should(BeNumerically("==", maxCacheRequests+1))

			err = receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, message.CacheRequestID(maxCacheRequests+1), callback)
			Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))

			channel, err := receiver.RequestCachedAsync(cacheRequestConfig, message.CacheRequestID(maxCacheRequests+1))
			Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			Expect(channel).To(BeNil())
			/* NOTE: We need to clear the internal buffer so that we can terminate. */
			for i := 0; i <= maxCacheRequests; i++ {
				<-cacheResponseSignalChan
			}
		})
		DescribeTable("a direct receiver should be able to submit a valid cache request, receive a response, and terminate",
			func(strategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
				logging.SetLogLevel(logging.LogLevelDebug)
				strategyString := ""
				numExpectedCachedMessages := 3
				numExpectedLiveMessages := 1
				numSentCacheRequests := 1
				numExpectedCacheResponses := numSentCacheRequests
				numExpectedSentMessages := 0
				totalMessagesReceived := 0
				numExpectedReceivedMessages := numExpectedSentMessages
				switch strategy {
				case resource.AsAvailable:
					strategyString = "AsAvailable"
					numExpectedReceivedMessages += numExpectedCachedMessages
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.LiveCancelsCached:
					strategyString = "LiveCancelsCached"
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CachedFirst:
					strategyString = "CachedFirst"
					numExpectedReceivedMessages += numExpectedCachedMessages
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CachedOnly:
					strategyString = "CachedOnly"
					numExpectedReceivedMessages += numExpectedCachedMessages
				}
				numExpectedSentDirectMessages := numSentCacheRequests + numExpectedSentMessages
				topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
				cacheName := fmt.Sprintf("MaxMsgs%d/delay=2000,msgs=%d", numExpectedCachedMessages, numExpectedLiveMessages)
				cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, topic)
				cacheRequestID := message.CacheRequestID(1)
				receivedMsgChan := make(chan message.InboundMessage, 3)
				defer close(receivedMsgChan)
				receiver.ReceiveAsync(func(msg message.InboundMessage) {
					receivedMsgChan <- msg
				})
				switch cacheResponseProcessStrategy {
				case helpers.ProcessCacheResponseThroughChannel:
					cacheResponseChan, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
					Expect(err).To(BeNil())
					Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "10s").Should(BeNumerically("==", numSentCacheRequests))
					for i := 0; i < numExpectedCacheResponses; i++ {
						Eventually(cacheResponseChan, "10s").Should(Receive())
					}
				case helpers.ProcessCacheResponseThroughCallback:
					cacheResponseSignalChan := make(chan solace.CacheResponse, 1)
					deferredOperation = func() { close(cacheResponseSignalChan) }
					cacheResponseCallback := func(cacheResponse solace.CacheResponse) {
						cacheResponseSignalChan <- cacheResponse
					}
					err := receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
					Expect(err).To(BeNil())
					Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "10s").Should(BeNumerically("==", numSentCacheRequests))
					for i := 0; i < numExpectedCacheResponses; i++ {
						Eventually(cacheResponseSignalChan, "10s").Should(Receive())
					}
				default:
					Fail(fmt.Sprintf("Got unexpected CacheResponseProcessStrategy %d", cacheResponseProcessStrategy))
				}
				for i := 0; i < numExpectedReceivedMessages; i++ {
					Eventually(receivedMsgChan, "10s").Should(Receive())
					totalMessagesReceived++
				}
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSent for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSucceeded for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0), fmt.Sprintf("CacheRequestsFailed for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", numExpectedSentDirectMessages), fmt.Sprintf("DirectMessagesSent for %s was wrong", strategyString))
				Expect(totalMessagesReceived).To(BeNumerically("==", numExpectedReceivedMessages))
			},
			Entry("test cache RR for valid AsAvailable with channel", resource.AsAvailable, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid AsAvailable with callback", resource.AsAvailable, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CachedFirst with channel", resource.CachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CachedFirst with callback", resource.CachedFirst, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CachedOnly with channel", resource.CachedOnly, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CachedOnly with callback", resource.CachedOnly, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid LiveCancelsCached with channel", resource.LiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid LivCancelsCached  with callback", resource.LiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
		)
		DescribeTable("asynchronous cache request with live data",
			func(strategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
				logging.SetLogLevel(logging.LogLevelDebug)
				strategyString := ""
				numExpectedCachedMessages := 3
				numExpectedLiveMessages := 1
				numSentCacheRequests := 1
				numExpectedCacheResponses := numSentCacheRequests
				numExpectedSentMessages := 0
				numExpectedReceivedMessages := numExpectedSentMessages
				switch strategy {
				case resource.AsAvailable:
					strategyString = "AsAvailable"
					numExpectedReceivedMessages += numExpectedCachedMessages
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.LiveCancelsCached:
					strategyString = "LiveCancelsCached"
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CachedFirst:
					strategyString = "CachedFirst"
					numExpectedReceivedMessages += numExpectedCachedMessages
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CachedOnly:
					strategyString = "CachedOnly"
					numExpectedReceivedMessages += numExpectedCachedMessages
				}
				numExpectedSentDirectMessages := numSentCacheRequests + numExpectedSentMessages
				topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
				cacheName := fmt.Sprintf("MaxMsgs%d/delay=2000,msgs=%d", numExpectedCachedMessages, numExpectedLiveMessages)
				cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, topic)
				cacheRequestID := message.CacheRequestID(1)
				receivedMsgChan := make(chan message.InboundMessage, 3)
				defer close(receivedMsgChan)
				receiver.ReceiveAsync(func(msg message.InboundMessage) {
					receivedMsgChan <- msg
				})

				var waitForCacheResponses func()
				var waitForLiveMessage = func() {
					var msg message.InboundMessage
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					Expect(msg.GetDestinationName()).To(Equal(topic))
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeFalse())
					Expect(id).To(BeNumerically("==", 0))
					/* EBP-21: Assert that this message is a live message. */
				}
				var waitForCachedMessages = func() {
					var msg message.InboundMessage
					for i := 0; i < numExpectedCachedMessages; i++ {
						Eventually(receivedMsgChan, "10s").Should(Receive(&msg))
						Expect(msg).ToNot(BeNil())
						Expect(msg.GetDestinationName()).To(Equal(topic))
						id, ok := msg.GetCacheRequestID()
						Expect(ok).To(BeTrue())
						Expect(id).To(BeNumerically("==", cacheRequestID))
						/* EBP-21: Assert that this message is a cached message. */
					}
				}
				switch cacheResponseProcessStrategy {
				case helpers.ProcessCacheResponseThroughChannel:
					cacheResponseChan, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
					Expect(err).To(BeNil())
					waitForCacheResponses = func() {
						var cacheResponse solace.CacheResponse
						for i := 0; i < numExpectedCacheResponses; i++ {
							Eventually(cacheResponseChan, "2s").Should(Receive(&cacheResponse))
							Expect(cacheResponse).ToNot(BeNil())
							/* EBP-25: Assert that this cache response has the same cache request ID as the
							 * submitted cache request.*/
							/* EBP-26: Assert that this cache response has CachRequestOutcome.Ok. */
							/* EBP-28: Assert that the error in this cache response is nil. */
						}
					}
				case helpers.ProcessCacheResponseThroughCallback:
					cacheResponseSignalChan := make(chan solace.CacheResponse, 1)
					deferredOperation = func() { close(cacheResponseSignalChan) }
					cacheResponseCallback := func(cacheResponse solace.CacheResponse) {
						cacheResponseSignalChan <- cacheResponse
					}
					err := receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
					Expect(err).To(BeNil())
					waitForCacheResponses = func() {
						var cacheResponse solace.CacheResponse
						for i := 0; i < numExpectedCacheResponses; i++ {
							Eventually(cacheResponseSignalChan, "10s").Should(Receive(&cacheResponse))
							Expect(cacheResponse).ToNot(BeNil())
							/* EBP-25: Assert that this cache response has the same cache request ID as the
							 * submitted cache request.*/
							/* EBP-26: Assert that this cache response has CachRequestOutcome.Ok. */
							/* EBP-28: Assert that the error in this cache response is nil. */
						}
					}
				default:
					Fail(fmt.Sprintf("Got unexpected CacheResponseProcessStrategy %d", cacheResponseProcessStrategy))
				}
				Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "10s").Should(BeNumerically("==", numSentCacheRequests))
				switch strategy {
				case resource.AsAvailable:
					waitForLiveMessage()
					Consistently(receivedMsgChan, "500ms").ShouldNot(Receive())
					waitForCacheResponses()
					waitForCachedMessages()
				case resource.LiveCancelsCached:
					waitForLiveMessage()
					waitForCacheResponses()
					/* NOTE: We only need to poll for 1ms, because if the API were going to give us cached
					 * messages, they would already be on the queue by the time we go to this assertion.
					 */
					Consistently(receivedMsgChan, "1ms").ShouldNot(Receive())
				case resource.CachedFirst:
					/* NOTE: we wait for 1500 ms since the delay is 2000 ms, and we want to allow a bit of room
					 * in the waiter so that we don't wait to long. Waiting past the delay would race with the
					 * reception of the cache response, coinciding with receivedMsgChan receiving
					 * cached data messages. This coincidence would cause the `Consistently` assertion to fail.
					 */
					Consistently(receivedMsgChan, "1500ms").ShouldNot(Receive())
					waitForCacheResponses()
					waitForCachedMessages()
					waitForLiveMessage()
				case resource.CachedOnly:
					Consistently(receivedMsgChan, "1500ms").ShouldNot(Receive())
					waitForCacheResponses()
					waitForCachedMessages()
					/* NOTE: We don't bother polling to confirm that no more data messages are being received,
					 * because the only other message that could be expected is the live message pulished by
					 * the proxy before the cache request reached the cache instance. That data message would
					 * have reached the API before any of the other messages, so if the previous assertions
					 * didn't fail, we know we didn't get the live message.
					 */
				default:
					Fail(fmt.Sprintf("Got unexpected CacheResponseProcessStrategy %d", cacheResponseProcessStrategy))
				}
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSent for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSucceeded for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0), fmt.Sprintf("CacheRequestsFailed for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", numExpectedSentDirectMessages), fmt.Sprintf("DirectMessagesSent for %s was wrong", strategyString))
			},
			Entry("test cache RR for valid AsAvailable with channel", resource.AsAvailable, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid AsAvailable with callback", resource.AsAvailable, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CachedFirst with channel", resource.CachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CachedFirst with callback", resource.CachedFirst, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CachedOnly with channel", resource.CachedOnly, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CachedOnly with callback", resource.CachedOnly, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid LiveCancelsCached with channel", resource.LiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid LivCancelsCached  with callback", resource.LiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
		)

		Describe("Lifecycle tests", func() {
			var messagingService solace.MessagingService
			var messageReceiver solace.DirectMessageReceiver
			var cacheRequestID = 0
			type terminationContext struct {
				terminateFunction func(messagingService solace.MessagingService,
					messageReceiver solace.DirectMessageReceiver)
				configuration func() config.ServicePropertyMap
				cleanupFunc   func(messagingService solace.MessagingService)
				// blockable indicates whether or not it is feasible for the application to block this
				// termination method. Termination methods that run on the main thread are candidates
				// since the test(application) can block termination only from the main thread and so would
				// reach a deadlock, or have to call terminate in another thread which would be equivalent
				// to the asynchronous interface. This would create redundant test coverage since the
				// asynchronous methods are already being tested.
				blockable bool
				// seversConnection informs the test that it should expect cache requests to be incomplete
				// since the connection is broken before the request completes.
				seversConnection bool
			}
			terminationCases := map[string]terminationContext{
				"messaging service disconnect": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						Expect(messagingService.Disconnect()).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
						Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					blockable:        true,
					seversConnection: true,
					// FFC: Assert subcode SESSION_NOT_ESTABLISHED, outcome.FAILED
				},
				"messaging service disconnect async with channel": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						var err error
						Eventually(messagingService.DisconnectAsync(), "5s").Should(Receive(&err))
						Expect(err).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
						Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					blockable:        true,
					seversConnection: true,
					// FFC: Assert subcode SESSION_NOT_ESTABLISHED, outcome.FAILED
				},
				"messaging service disconnect async with callback": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						errorChan := make(chan error)
						messagingService.DisconnectAsyncWithCallback(func(err error) {
							errorChan <- err
						})
						var err_holder error
						Eventually(errorChan, "5s").Should(Receive(&err_holder))
						Expect(err_holder).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
						Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					blockable:        true,
					seversConnection: true,
					// FFC: Assert subcode SESSION_NOT_ESTABLISHED, outcome.FAILED
				},
				"management disconnect": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						eventChan := make(chan solace.ServiceEvent)
						messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
							eventChan <- event
						})
						helpers.ForceDisconnectViaSEMPv2WithConfiguration(
							messagingService,
							// make sure this is the same config as assigned to the termination
							// strategy's `configuration` field.
							helpers.DefaultCacheConfiguration())
						var event_holder solace.ServiceEvent
						Eventually(eventChan, "5s").Should(Receive(&event_holder))
						Expect(event_holder).To(Not(BeNil()))
						helpers.ValidateNativeError(event_holder.GetCause(), subcode.CommunicationError)

						Expect(messagingService.IsConnected()).To(BeFalse())
						Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					blockable:        true,
					seversConnection: true,
					// FFC: Assert subcode COMMUNICATION_ERROR, outcome.FAILED
				},
				"toxic disconnect": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						eventChan := make(chan solace.ServiceEvent)
						messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
							eventChan <- event
						})
						testcontext.Toxi().SMF().Delete()

						var event_holder solace.ServiceEvent
						Eventually(eventChan, "30s").Should(Receive(&event_holder))
						Expect(event_holder).To(Not(BeNil()))
						helpers.ValidateNativeError(event_holder.GetCause(), subcode.CommunicationError)

						Expect(messagingService.IsConnected()).To(BeFalse())
						Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						helpers.CheckToxiProxy()
						return helpers.CacheToxicConfiguration()
					},
					cleanupFunc: func(_ solace.MessagingService) {
						testcontext.Toxi().ResetProxies()
					},
					blockable:        true,
					seversConnection: true,
					// FFC: Assert subcode COMMUNICATION_ERROR, outcome.FAILED
				},
				"receiver terminate": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						Expect(messageReceiver.Terminate(0)).To(BeNil())
						Expect(messageReceiver.IsRunning()).To(BeFalse())
						Expect(messageReceiver.IsTerminated()).To(BeTrue())
						Expect(messagingService.IsConnected()).To(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					cleanupFunc: func(messagingService solace.MessagingService) {
						Expect(messagingService.Disconnect()).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
					},
					blockable:        false,
					seversConnection: false,
					// FFC: Assert subcode CACHE_REQUEST_CANCELLED, outcome.FAILED
				},
				"receiver terminate with grace period": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						gracePeriod := time.Second * 5
						Expect(messageReceiver.Terminate(gracePeriod)).To(BeNil())
						Expect(messageReceiver.IsRunning()).To(BeFalse())
						Expect(messageReceiver.IsTerminated()).To(BeTrue())
						Expect(messagingService.IsConnected()).To(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					cleanupFunc: func(messagingService solace.MessagingService) {
						Expect(messagingService.Disconnect()).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
					},
					blockable:        false,
					seversConnection: false,
					// FFC: Assert subcode CACHE_REQUEST_CANCELLED, outcome.FAILED
				},
				"receiver terminate async with channel": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						gracePeriod := time.Second * 0
						var err error
						Eventually(messageReceiver.TerminateAsync(gracePeriod), gracePeriod+(time.Second*1)).Should(Receive(&err))
						Expect(err).To(BeNil())
						Expect(messageReceiver.IsRunning()).To(BeFalse())
						Expect(messageReceiver.IsTerminated()).To(BeTrue())
						Expect(messagingService.IsConnected()).To(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					cleanupFunc: func(messagingService solace.MessagingService) {
						Expect(messagingService.Disconnect()).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
					},
					blockable:        true,
					seversConnection: false,
					// FFC: Assert subcode CACHE_REQUEST_CANCELLED, outcome.FAILED
				},
				"receiver terminate async with grace period with channel": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						gracePeriod := time.Second * 5
						var err error
						Eventually(messageReceiver.TerminateAsync(gracePeriod), gracePeriod+(time.Second*1)).Should(Receive(&err))
						Expect(err).To(BeNil())
						Expect(messageReceiver.IsRunning()).To(BeFalse())
						Expect(messageReceiver.IsTerminated()).To(BeTrue())
						Expect(messagingService.IsConnected()).To(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					cleanupFunc: func(messagingService solace.MessagingService) {
						Expect(messagingService.Disconnect()).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
					},
					blockable:        true,
					seversConnection: false,
					// FFC: Assert subcode CACHE_REQUEST_CANCELLED, outcome.FAILED
				},
				"receiver terminate async with callback": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						gracePeriod := time.Second * 0
						errChan := make(chan error)
						var err error
						messageReceiver.TerminateAsyncCallback(gracePeriod, func(err error) {
							errChan <- err
						})
						Eventually(errChan, gracePeriod+(time.Second*1)).Should(Receive(&err))
						Expect(err).To(BeNil())
						Expect(messageReceiver.IsRunning()).To(BeFalse())
						Expect(messageReceiver.IsTerminated()).To(BeTrue())
						Expect(messagingService.IsConnected()).To(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					cleanupFunc: func(messagingService solace.MessagingService) {
						Expect(messagingService.Disconnect()).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
					},
					blockable:        true,
					seversConnection: false,
					// FFC: Assert subcode CACHE_REQUEST_CANCELLED, outcome.FAILED
				},
				"receiver terminate async with grace period with callback": {
					terminateFunction: func(messagingService solace.MessagingService,
						messageReceiver solace.DirectMessageReceiver) {
						gracePeriod := time.Second * 5
						errChan := make(chan error)
						var err error
						messageReceiver.TerminateAsyncCallback(gracePeriod, func(err error) {
							errChan <- err
						})
						Eventually(errChan, gracePeriod+(time.Second*1)).Should(Receive(&err))
						Expect(err).To(BeNil())
						Expect(messageReceiver.IsRunning()).To(BeFalse())
						Expect(messageReceiver.IsTerminated()).To(BeTrue())
						Expect(messagingService.IsConnected()).To(BeTrue())
					},
					configuration: func() config.ServicePropertyMap {
						return helpers.DefaultCacheConfiguration()
					},
					cleanupFunc: func(messagingService solace.MessagingService) {
						Expect(messagingService.Disconnect()).To(BeNil())
						Expect(messagingService.IsConnected()).To(BeFalse())
					},
					blockable:        true,
					seversConnection: false,
					// FFC: Assert subcode CACHE_REQUEST_CANCELLED, outcome.FAILED
				},
			}
			Context("a connected messaging service with a built direct message receiver", func() {
				const cacheName string = "trivial cache name"
				const topic string = "trivial topic"
				const strategy resource.CachedMessageSubscriptionStrategy = resource.AsAvailable
				const cacheRequestID message.CacheRequestID = 1
				BeforeEach(func() {
					logging.SetLogLevel(logging.LogLevelDebug)
					CheckCache() // skips test with message if cache image is not available
					helpers.InitAllCacheClustersWithMessages()
					var err error
					messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultCacheConfiguration()).Build()
					Expect(err).To(BeNil())
					err = messagingService.Connect()
					Expect(err).To(BeNil())
					messageReceiver, err = messagingService.CreateDirectMessageReceiverBuilder().Build()
					Expect(err).To(BeNil())
				})
				AfterEach(func() {
					var err error
					if messageReceiver.IsRunning() {
						err = messageReceiver.Terminate(0)
						Expect(err).To(BeNil())
					}
					Expect(messageReceiver.IsRunning()).To(BeFalse())
					if messagingService.IsConnected() {
						err = messagingService.Disconnect()
						Expect(err).To(BeNil())
					}
					Expect(messagingService.IsConnected()).To(BeFalse())
				})
				It("will return an IllegalStateError when a cache request is attempted before the receiver is started", func() {
					cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, topic)
					_, err := messageReceiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
					Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))

					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 0))
					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))

					cacheResponseCallback := func(cacheResponse solace.CacheResponse) {
						Fail("This function should never be called.")
					}
					err = messageReceiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
					Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))

					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 0))
					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
				})
			})
			for terminationCaseName, terminationContextRef := range terminationCases {
				terminationConfiguration := terminationContextRef.configuration
				terminationFunction := terminationContextRef.terminateFunction
				terminationCleanup := terminationContextRef.cleanupFunc

				Context("a connected messaging service with a built direct message receiver", func() {
					const cacheName string = "trivial cache name"
					const topic string = "trivial topic"
					const strategy resource.CachedMessageSubscriptionStrategy = resource.AsAvailable
					const cacheRequestID message.CacheRequestID = 1
					BeforeEach(func() {
						logging.SetLogLevel(logging.LogLevelDebug)
						CheckCache() // skips test with message if cache image is not available
						helpers.InitAllCacheClustersWithMessages()
						var err error
						messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(terminationConfiguration()).Build()
						Expect(err).To(BeNil())
						err = messagingService.Connect()
						Expect(err).To(BeNil())
						messageReceiver, err = messagingService.CreateDirectMessageReceiverBuilder().Build()
						Expect(err).To(BeNil())
						Expect(messageReceiver.Start()).To(BeNil())
						Expect(messageReceiver.IsRunning()).To(BeTrue())
						Expect(messageReceiver.Terminate(0)).To(BeNil())

					})
					AfterEach(func() {
						var err error
						if messageReceiver.IsRunning() {
							err = messageReceiver.Terminate(0)
							Expect(err).To(BeNil())
						}
						Expect(messageReceiver.IsRunning()).To(BeFalse())
						if messagingService.IsConnected() {
							err = messagingService.Disconnect()
							Expect(err).To(BeNil())
						}
						Expect(messagingService.IsConnected()).To(BeFalse())
						if terminationCleanup != nil {
							terminationCleanup(messagingService)
						}
					})

					It("will return an IllegalStateError when a cache request is attempted after the receiver is terminated using the "+terminationCaseName+" termination method", func() {
						terminationFunction(messagingService, messageReceiver)
						cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, topic)
						_, err := messageReceiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
						Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))

						Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 0))
						Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
						Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))

						cacheResponseCallback := func(cacheResponse solace.CacheResponse) {
							Fail("This function should never be called.")
						}
						err = messageReceiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
						Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))

						Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 0))
						Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
						Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
					})
				})
			}
			for terminationCaseName, terminationContextRef := range terminationCases {
				terminationConfiguration := terminationContextRef.configuration
				terminationFunction := terminationContextRef.terminateFunction
				terminationCleanup := terminationContextRef.cleanupFunc
				Context("using termination scheme "+terminationCaseName, func() {
					const numConfiguredCachedMessages int = 3
					const numExpectedCachedMessages int = 0
					const numLiveMessagesFromCacheProxy = 0
					const numExpectedLiveMessages int = numLiveMessagesFromCacheProxy
					const delay int = 25000
					var cacheName string
					var cacheTopic string
					var directTopic string

					var terminate func()
					var receivedMsgChan chan message.InboundMessage

					BeforeEach(func() {
						cacheName = fmt.Sprintf("MaxMsgs%d/delay=%d", numConfiguredCachedMessages, delay)
						cacheTopic = fmt.Sprintf("MaxMsgs%d/%s/data2", numConfiguredCachedMessages, testcontext.Cache().Vpn)
						cacheRequestID++
						logging.SetLogLevel(logging.LogLevelDebug)
						CheckCache() // skips test with message if cache image is not available
						helpers.InitAllCacheClustersWithMessages()
						var err error
						messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(terminationConfiguration()).Build()
						Expect(err).To(BeNil())
						err = messagingService.Connect()
						Expect(err).To(BeNil())
						messageReceiver, err = messagingService.CreateDirectMessageReceiverBuilder().Build()
						Expect(err).To(BeNil())
						err = messageReceiver.Start()
						Expect(err).To(BeNil())
						receivedMsgChan = make(chan message.InboundMessage, 3)
						messageReceiver.ReceiveAsync(func(msg message.InboundMessage) {
							receivedMsgChan <- msg
						})

						terminate = func() {
							Expect(messagingService.IsConnected()).To(BeTrue())
							Expect(messageReceiver.IsRunning()).To(BeTrue())
							terminationFunction(messagingService, messageReceiver)
							Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
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
							terminationCleanup(messagingService)
						}
						close(receivedMsgChan)
					})

					DescribeTable("a receiver should be able to terminate gracefully with inflight cache requests",
						func(strategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
							logging.SetLogLevel(logging.LogLevelDebug)
							strategyString := ""
							numSentCacheRequests := 1
							numExpectedCacheResponses := numSentCacheRequests
							// The cache request should be cancelled, so it is not successful
							numExpectedSuccessfulCacheRequests := 0
							// The cache request should be cancelled, which counts as the
							// application stopping the cache request, not as an error/failure
							numExpectedFailedCacheRequests := 0
							totalMessagesReceived := 0
							numExpectedReceivedMessages := 0
							switch strategy {
							case resource.AsAvailable:
								strategyString = "AsAvailable"
								numExpectedReceivedMessages += numExpectedCachedMessages
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.LiveCancelsCached:
								strategyString = "LiveCancelsCached"
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CachedFirst:
								strategyString = "CachedFirst"
								numExpectedReceivedMessages += numExpectedCachedMessages
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CachedOnly:
								strategyString = "CachedOnly"
								numExpectedReceivedMessages += numExpectedCachedMessages
							}
							var cacheResponseProcessStrategyString string
							switch cacheResponseProcessStrategy {
							case helpers.ProcessCacheResponseThroughChannel:
								cacheResponseProcessStrategyString = "channel"
							case helpers.ProcessCacheResponseThroughCallback:
								cacheResponseProcessStrategyString = "callback"
							default:
								Fail("Unrecognized CacheResponseProcessStrategy")
							}
							numExpectedSentDirectMessages := numSentCacheRequests

							cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, cacheTopic)
							cacheRequestID := message.CacheRequestID(cacheRequestID)
							directTopic = fmt.Sprintf("T/cache_test/inflight_requests/%s/%s/%s/%d", terminationCaseName, strategyString, cacheResponseProcessStrategyString, cacheRequestID)
							err := messageReceiver.AddSubscription(resource.TopicSubscriptionOf(directTopic))
							Expect(err).To(BeNil())
							switch cacheResponseProcessStrategy {
							case helpers.ProcessCacheResponseThroughChannel:
								cacheResponseChan, err := messageReceiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
								Expect(err).To(BeNil())

								Eventually(func() uint64 {
									return messagingService.Metrics().GetValue(metrics.CacheRequestsSent)
								}, "10s").Should(BeNumerically("==", 1))

								for i := 0; i < numExpectedReceivedMessages; i++ {
									var inboundMessage message.InboundMessage
									Eventually(receivedMsgChan, "10s").Should(Receive(&inboundMessage))
									Expect(inboundMessage.GetDestinationName()).To(BeEquivalentTo(directTopic))
									totalMessagesReceived++
								}

								terminate()
								for i := 0; i < numExpectedCacheResponses; i++ {
									Eventually(cacheResponseChan, delay*2).Should(Receive())
								}
							case helpers.ProcessCacheResponseThroughCallback:
								cacheResponseSignalChan := make(chan solace.CacheResponse, numExpectedCacheResponses)
								defer func() {
									close(cacheResponseSignalChan)
								}()
								cacheResponseCallback := func(cacheResponse solace.CacheResponse) {
									cacheResponseSignalChan <- cacheResponse
								}
								err := messageReceiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
								Expect(err).To(BeNil())

								Eventually(func() uint64 {
									return messagingService.Metrics().GetValue(metrics.CacheRequestsSent)
								}, "10s").Should(BeNumerically("==", 1))

								for i := 0; i < numExpectedReceivedMessages; i++ {
									var inboundMessage message.InboundMessage
									Eventually(receivedMsgChan, "10s").Should(Receive(&inboundMessage))
									Expect(inboundMessage.GetDestinationName()).To(BeEquivalentTo(directTopic))
									totalMessagesReceived++
								}

								terminate()
								for i := 0; i < numExpectedCacheResponses; i++ {
									Eventually(cacheResponseSignalChan, delay*2).Should(Receive())
								}
							default:
								Fail(fmt.Sprintf("Got unexpected CacheResponseProcessStrategy %d", cacheResponseProcessStrategy))
							}

							Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSent for %s with cache request ID %d was wrong", strategyString, cacheRequestID))
							Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", numExpectedSuccessfulCacheRequests), fmt.Sprintf("CacheRequestsSucceeded for %s was wrong", strategyString))
							Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", numExpectedFailedCacheRequests), fmt.Sprintf("CacheRequestsFailed for %s was wrong", strategyString))
							Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", numExpectedSentDirectMessages), fmt.Sprintf("DirectMessagesSent for %s was wrong", strategyString))
							Expect(totalMessagesReceived).To(BeNumerically("==", numExpectedReceivedMessages))
						},
						Entry("test cache RR for valid AsAvailable with channel", resource.AsAvailable, helpers.ProcessCacheResponseThroughChannel),
						Entry("test cache RR for valid AsAvailable with callback", resource.AsAvailable, helpers.ProcessCacheResponseThroughCallback),
						Entry("test cache RR for valid CachedFirst with channel", resource.CachedFirst, helpers.ProcessCacheResponseThroughChannel),
						Entry("test cache RR for valid CachedFirst with callback", resource.CachedFirst, helpers.ProcessCacheResponseThroughCallback),
						Entry("test cache RR for valid CachedOnly with channel", resource.CachedOnly, helpers.ProcessCacheResponseThroughChannel),
						Entry("test cache RR for valid CachedOnly with callback", resource.CachedOnly, helpers.ProcessCacheResponseThroughCallback),
						Entry("test cache RR for valid LiveCancelsCached with channel", resource.LiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
						Entry("test cache RR for valid LivCancelsCached  with callback", resource.LiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
					)
				})
			}
			for terminationCaseName, terminationContextRef := range terminationCases {
				if !terminationContextRef.blockable {
					/* NOTE: The selected termination method cannot be blocked, so this test must be
					 * skipped.
					 */
					continue
				}
				terminationConfiguration := terminationContextRef.configuration
				terminationFunction := terminationContextRef.terminateFunction
				terminationCleanup := terminationContextRef.cleanupFunc
				terminationSeversConnection := terminationContextRef.seversConnection
				Context("using termination scheme "+terminationCaseName, func() {
					const numConfiguredCachedMessages int = 3
					const numExpectedCachedMessages int = 0
					const numLiveMessagesFromCacheProxy int = 0
					const numExpectedLiveMessages int = numLiveMessagesFromCacheProxy
					var cacheName string
					var cacheTopic string

					var terminate func()
					var receivedMsgChan chan message.InboundMessage

					BeforeEach(func() {
						cacheName = fmt.Sprintf("MaxMsgs%d", numConfiguredCachedMessages)
						cacheTopic = fmt.Sprintf("MaxMsgs%d/%s/data1", numConfiguredCachedMessages, testcontext.Cache().Vpn)
						cacheRequestID++
						logging.SetLogLevel(logging.LogLevelDebug)
						CheckCache() // skips test with message if cache image is not available
						helpers.InitAllCacheClustersWithMessages()
						var err error
						messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(terminationConfiguration()).Build()
						Expect(err).To(BeNil())
						err = messagingService.Connect()
						Expect(err).To(BeNil())
						messageReceiver, err = messagingService.CreateDirectMessageReceiverBuilder().Build()
						Expect(err).To(BeNil())
						err = messageReceiver.Start()
						Expect(err).To(BeNil())
						receivedMsgChan = make(chan message.InboundMessage, 3)
						messageReceiver.ReceiveAsync(func(msg message.InboundMessage) {
							receivedMsgChan <- msg
						})

						terminate = func() {
							Expect(messagingService.IsConnected()).To(BeTrue())
							Expect(messageReceiver.IsRunning()).To(BeTrue())
							terminationFunction(messagingService, messageReceiver)
							Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
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
							terminationCleanup(messagingService)
						}
						close(receivedMsgChan)
					})

					DescribeTable("a receiver should be able to terminate gracefully with received cache responses and termination blocked by the application",
						func(strategy resource.CachedMessageSubscriptionStrategy) {
							logging.SetLogLevel(logging.LogLevelDebug)
							strategyString := ""
							numSentCacheRequests := 1
							var numExpectedSuccessfulCacheRequests int
							if terminationSeversConnection {
								// The cache request should not complete, so it's not successful
								numExpectedSuccessfulCacheRequests = 0
							} else {
								// The cache request should be completed, so it's successful
								numExpectedSuccessfulCacheRequests = numSentCacheRequests
							}
							numExpectedFailedCacheRequests := 0
							numExpectedCacheResponses := numSentCacheRequests
							totalMessagesReceived := 0
							numExpectedReceivedMessages := 0
							switch strategy {
							case resource.AsAvailable:
								strategyString = "AsAvailable"
								numExpectedReceivedMessages += numExpectedCachedMessages
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.LiveCancelsCached:
								strategyString = "LiveCancelsCached"
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CachedFirst:
								strategyString = "CachedFirst"
								numExpectedReceivedMessages += numExpectedCachedMessages
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CachedOnly:
								strategyString = "CachedOnly"
								numExpectedReceivedMessages += numExpectedCachedMessages
							}
							numExpectedSentDirectMessages := numSentCacheRequests

							cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, cacheTopic)
							cacheRequestID := message.CacheRequestID(cacheRequestID)

							/* NOTE: This channel receives the cache response and indicates to the
							 * test that it is time to call terminate().
							 */
							var cacheResponseChan atomic.Int32
							cacheResponseChan.Store(0)
							/* NOTE: We need to read all the cache responses because the callbacks will keep getting
							 * executed for each one, and try to write to the cacheResponseChan. If the callback tries
							 * to write to the channel after it has been closed, it will panic. So, we need to close
							 * the channel only after we know it won't be written to again.
							 */
							/* NOTE: We make the signal chan size 0 so that all writers (the API)
							 * have to wait for the reader (the application) to empty the channel.
							 * This allows us to simulate blocking behaviour.
							 */
							var cacheResponseSignal atomic.Bool
							cacheResponseSignal.Store(false)
							cacheResponseCallback := func(cacheResponse solace.CacheResponse) {
								cacheResponseChan.Add(1)
								for !cacheResponseSignal.Load() {
									time.Sleep(time.Millisecond * 500)
								}
							}
							err := messageReceiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
							Expect(err).To(BeNil())

							Eventually(func() uint64 {
								return messagingService.Metrics().GetValue(metrics.CacheRequestsSent)
							}, "10s").Should(BeNumerically("==", 1))

							for i := 0; i < numExpectedReceivedMessages; i++ {
								var inboundMessage message.InboundMessage
								Eventually(receivedMsgChan, "10s").Should(Receive(&inboundMessage))
								Expect(inboundMessage.GetDestinationName()).To(BeEquivalentTo(cacheTopic))
								totalMessagesReceived++
							}

							/* Compare to one because the first cache response callback should block */
							Eventually(func() int32 { return cacheResponseChan.Load() }, "10s").Should(BeNumerically("==", 1))
							/* NOTE: We call terminate after confirming that we have received the
							 * cache response so that we can verify termination behaviour when the
							 * application is blocking termination through the provided callback.
							 */
							terminate()
							cacheResponseSignal.Store(true)
							Eventually(func() int32 { return cacheResponseChan.Load() }, "10s").Should(BeNumerically("==", numExpectedCacheResponses))

							Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSent for %s was wrong with ID %d", strategyString, cacheRequestID))
							Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", numExpectedSuccessfulCacheRequests), fmt.Sprintf("CacheRequestsSucceeded for %s was wrong", strategyString))
							Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", numExpectedFailedCacheRequests), fmt.Sprintf("CacheRequestsFailed for %s was wrong", strategyString))
							Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", numExpectedSentDirectMessages), fmt.Sprintf("DirectMessagesSent for %s was wrong", strategyString))
							Expect(totalMessagesReceived).To(BeNumerically("==", numExpectedReceivedMessages))

						},
						/* NOTE: The point of this test is to verify that if the application's
						 * processing of the cache response is blocking, it blocks termination. Since
						 * the `RequestCachedAsync()` method returns a channel for the application to
						 * listen to, there is no way for the application's processing of its response
						 * to block termination. This makes this test unapplicable to the
						 * `RequestCachedAsync`() method. In contrast, the
						 * `RequestCachedAsyncWithCallback()` interface requires that the application
						 * process the cache response in an application-provided callback. This provides
						 * the application with the opportunity to block during termination, making the
						 * `RequestCachedAsyncWithCallback()` interface relevant to this test.
						 */
						Entry("test cache RR for valid AsAvailable with callback", resource.AsAvailable),
						Entry("test cache RR for valid CachedFirst with callback", resource.CachedFirst),
						Entry("test cache RR for valid CachedOnly with callback", resource.CachedOnly),
						Entry("test cache RR for valid LivCancelsCached  with callback", resource.LiveCancelsCached),
					)
				})
			}
		})
	})
})

var _ = Describe("Remote Cache Message Tests", func() {
	// The following tests are just placeholders until the actual implememntation
	// for retrieving cache messages has been completed.
	// They should be modified to real tests when we have the implementation to retrieve cache messages.

	const topic = "remote-cache-message-tests"

	var messagingService solace.MessagingService
	var messageBuilder solace.OutboundMessageBuilder

	BeforeEach(func() {
		CheckCache()      // skips test with message if cache image is not available
		CheckCacheProxy() // skips test with message if cache proxy image is not available

		builder := messaging.NewMessagingServiceBuilder().
			FromConfigurationProvider(helpers.DefaultConfiguration())

		var err error
		messagingService, err = builder.Build()
		Expect(err).ToNot(HaveOccurred())
		messageBuilder = messagingService.MessageBuilder()
	})

	Describe("Published and received outbound message", func() {
		var publisher solace.DirectMessagePublisher
		var receiver solace.DirectMessageReceiver
		var inboundMessageChannel chan message.InboundMessage

		BeforeEach(func() {
			var err error
			err = messagingService.Connect()
			Expect(err).ToNot(HaveOccurred())

			publisher, err = messagingService.CreateDirectMessagePublisherBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			receiver, err = messagingService.CreateDirectMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topic)).
				Build()
			Expect(err).ToNot(HaveOccurred())

			err = publisher.Start()
			Expect(err).ToNot(HaveOccurred())

			inboundMessageChannel = make(chan message.InboundMessage)
			receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				inboundMessageChannel <- inboundMessage
			})

			err = receiver.Start()
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			var err error
			err = publisher.Terminate(10 * time.Second)
			Expect(err).ToNot(HaveOccurred())
			err = receiver.Terminate(10 * time.Second)
			Expect(err).ToNot(HaveOccurred())

			err = messagingService.Disconnect()
			Expect(err).ToNot(HaveOccurred())
		})

		// EBP-24 (second test case): Cache inbound message - check that messages returned as part of a cache response
		// have valid cached request ID (calling GetCachedRequestID() on a cache message returns the ID and true)
		It("should retrieve the valid cache request ID from received Cached message", func() {
			msg, err := messageBuilder.BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				cacheRequestID, ok := inboundMessage.GetCacheRequestID()
				// @TODO: EBP-24: Modify these assertions for better test
				// coverage when the feature to retrieve cache messages is done

				Expect(ok).To(BeFalse())                                    // for a CACHE message
				Expect(cacheRequestID).To(Equal(message.CacheRequestID(0))) // for a CACHE message
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

	})

})
