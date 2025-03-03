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
	"strings"
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

func GetCacheStatsAsString(messagingService solace.MessagingService) string {
	return fmt.Sprintf("CacheRequestsSent: %d\nCacheRequestsSucceeded: %d\nCacheRequestsFailed: %d\n", messagingService.Metrics().GetValue(metrics.CacheRequestsSent), messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded), messagingService.Metrics().GetValue(metrics.CacheRequestsFailed))
}

var _ = Describe("Cache Strategy", func() {
	logging.SetLogLevel(logging.LogLevelDebug)
	Describe("When the cache is available and configured", func() {
		var messagingService solace.MessagingService
		var receiver solace.DirectMessageReceiver
		/* NOTE: deferredOperation is used for conducting operations after termination, such as closing a channel
		 * that may be used during termination but that is declared within a single test case because its use is
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
			receiver, err = messagingService.CreateDirectMessageReceiverBuilder().OnBackPressureDropOldest(100100).Build()
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
		DescribeTable("cache reply contains no data", func(topic_template string, cacheRequestStrategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
			topic := fmt.Sprintf(topic_template, testcontext.Cache().Vpn)
			cacheRequestID := message.CacheRequestID(1)
			cacheName := "UnitTest"
			var cacheRequestConfig resource.CachedMessageSubscriptionRequest
			switch cacheRequestStrategy {
			case resource.CacheRequestStrategyAsAvailable:
				cacheRequestConfig = helpers.GetValidCacheRequestStrategyAsAvailableCacheRequestConfig(cacheName, topic)
			case resource.CacheRequestStrategyCachedFirst:
				cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedFirstCacheRequestConfig(cacheName, topic)
			case resource.CacheRequestStrategyCachedOnly:
				cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedOnlyCacheRequestConfig(cacheName, topic)
			case resource.CacheRequestStrategyLiveCancelsCached:
				cacheRequestConfig = helpers.GetValidCacheRequestStrategyLiveCancelsCachedRequestConfig(cacheName, topic)
			}
			/* NOTE: Despite expecting to receive 0 messages, we create a channel with a size of 1 to mitigate the
			 * risk of the test blocking receiver terminate in the event that we unexpectedly receive a message. The
			 * `Consistently` assertion will still fail if this channel receives a message, so there will not be any
			 * silent failures caused by this channel size. If the channel size were 0, the receiver would block
			 * termination indefinitely because the application channel would not be have the size to receive the
			 * message from the API.
			 */
			receivedMsgChan := make(chan message.InboundMessage, 1)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())
			var cacheResponse solace.CacheResponse
			switch cacheResponseProcessStrategy {
			case helpers.ProcessCacheResponseThroughChannel:
				channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
				Expect(err).To(BeNil())
				Expect(channel).ToNot(BeNil())
				Eventually(channel, "5s").Should(Receive(&cacheResponse))
			case helpers.ProcessCacheResponseThroughCallback:
				channel := make(chan solace.CacheResponse, 1)
				callback := func(cacheResponse solace.CacheResponse) {
					channel <- cacheResponse
				}
				err = receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, callback)
				Expect(err).To(BeNil())
				Eventually(channel, "5s").Should(Receive(&cacheResponse))
			}
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache reponse ID matches cache request ID. */
			/* EBP-26: Assert CacheRequestOutcome is NoData. */
			/* EBP-28: Assert err is nil. */
			Consistently(receivedMsgChan).ShouldNot(Receive())
		},
			Entry("with topic 1 with channel with CacheRequestStrategyAsAvailable", "MaxMsgs3/%s/notcached", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 1 with callback with CacheRequestStrategyAsAvailable", "MaxMsgs3/%s/notcached", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 1 with channel with CacheRequestStrategyLiveCancelsCached", "MaxMsgs3/%s/notcached", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 1 with callback with CacheRequestStrategyLiveCancelsCached", "MaxMsgs3/%s/notcached", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 1 with channel with CacheRequestStrategyCachedFirst", "MaxMsgs3/%s/notcached", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 1 with callback with CacheRequestStrategyCachedFirst", "MaxMsgs3/%s/notcached", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 1 with channel with CacheRequestStrategyCachedOnly", "MaxMsgs3/%s/notcached", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 1 with callback with CacheRequestStrategyCachedOnly", "MaxMsgs3/%s/notcached", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 2 with channel with CacheRequestStrategyAsAvailable", "Max*sgs3/%s/data1", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 2 with callback with CacheRequestStrategyAsAvailable", "Max*sgs3/%s/data1", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 2 with channel with CacheRequestStrategyLiveCancelsCached", "Max*sgs3/%s/data1", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 2 with callback with CacheRequestStrategyLiveCancelsCached", "Max*sgs3/%s/data1", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 2 with channel with CacheRequestStrategyCachedFirst", "Max*sgs3/%s/data1", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 2 with callback with CacheRequestStrategyCachedFirst", "Max*sgs3/%s/data1", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 2 with channel with CacheRequestStrategyCachedOnly", "Max*sgs3/%s/data1", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 2 with callback with CacheRequestStrategyCachedOnly", "Max*sgs3/%s/data1", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 3 with channel with CacheRequestStrategyAsAvailable", "MaxMsgs3/%s/nodata", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 3 with callback with CacheRequestStrategyAsAvailable", "MaxMsgs3/%s/nodata", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 3 with channel with CacheRequestStrategyCachedFirst", "MaxMsgs3/%s/nodata", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 3 with callback with CacheRequestStrategyCachedFirst", "MaxMsgs3/%s/nodata", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 3 with channel with CacheRequestStrategyCachedOnly", "MaxMsgs3/%s/nodata", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 3 with callback with CacheRequestStrategyCachedOnly", "MaxMsgs3/%s/nodata", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughCallback),
			Entry("with topic 3 with channel with CacheRequestStrategyLiveCancelsCached", "MaxMsgs3/%s/nodata", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("with topic 3 with callback with CacheRequestStrategyLiveCancelsCached", "MaxMsgs3/%s/nodata", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
		)
		DescribeTable("CachedOnly cache requests on different topics on the same receiver concurrent", func(cacheRequestStrategy resource.CachedMessageSubscriptionStrategy, withLiveMessages bool) {
			numConfiguredCachedMessages := 3
			numConfiguredLiveMessages := 1
			delay := 2000
			delayAsTime := time.Second * time.Duration((delay / 1000))
			concurrentCacheRequestID := message.CacheRequestID(1)
			cachedOnlyCacheRequestID := message.CacheRequestID(2)
			var concurrentCacheName string
			cachedOnlyCacheName := fmt.Sprintf("MaxMsgs%d", numConfiguredCachedMessages)
			if withLiveMessages {
				concurrentCacheName = fmt.Sprintf("%s/delay=%d,msgs=%d", cachedOnlyCacheName, delay, numConfiguredLiveMessages)
			} else {
				concurrentCacheName = fmt.Sprintf("%s/delay=%d", cachedOnlyCacheName, delay)
			}
			concurrentCacheTopic := fmt.Sprintf("MaxMsgs%d/%s/data1", numConfiguredCachedMessages, testcontext.Cache().Vpn)
			cachedOnlyCacheTopic := fmt.Sprintf("MaxMsgs%d/%s/data2", numConfiguredCachedMessages, testcontext.Cache().Vpn)
			cachedOnlyCacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyCachedOnly, cachedOnlyCacheName, resource.TopicSubscriptionOf(cachedOnlyCacheTopic), int32(5000), int32(5000), int32(5000))
			concurrentCacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(cacheRequestStrategy, concurrentCacheName, resource.TopicSubscriptionOf(concurrentCacheTopic), int32(5000), int32(5000), int32(5000))

			/* Channel size of 10 is an arbitrary number over the anticipated 7 received messages. */
			receivedMsgChan := make(chan message.InboundMessage, 10)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())

			concurrentChannel, err := receiver.RequestCachedAsync(concurrentCacheRequestConfig, concurrentCacheRequestID)
			Expect(err).To(BeNil())
			Expect(concurrentChannel).ToNot(BeNil())
			if withLiveMessages && (cacheRequestStrategy == resource.CacheRequestStrategyLiveCancelsCached || cacheRequestStrategy == resource.CacheRequestStrategyAsAvailable) {
				/* NOTE: We delay before sending the second cache request to mitigate the risk of cached messages
				 * from the second request arriving before live messages from the first request. This allows us
				 * to test received message ordering later. This only matters for LiveCancelsCached and
				 * AsAvailable, because those strategies do not dictate ordering of received messages. If
				 * CachedOnly or CachedFirst requests are being sent, any received live messages will be
				 * deferred or ignored.
				 */
				time.Sleep(time.Millisecond * 500)
			}
			cachedOnlyChannel, err := receiver.RequestCachedAsync(cachedOnlyCacheRequestConfig, cachedOnlyCacheRequestID)
			Expect(err).To(BeNil())
			Expect(cachedOnlyChannel).ToNot(BeNil())
			/* NOTE: We only wait for a cache response for as much as the delay because the point of the test is to
			 * receive the CachedOnly response while the other request is still in-flight. The proxy will ensure
			 * that the other request is not received by the cache instance for at least `delay` seconds. If the
			 * CachedOnly response is received after that delay has expired, the test results may no longer be
			 * reliable.
			 */
			var cacheResponse solace.CacheResponse
			Eventually(cachedOnlyChannel, delayAsTime).Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID */
			/* EBP-26: Assert CacheRequestOutcome */
			/* EBP-28: Assert err */
			/* NOTE: If using LiveCancelsCached */
			if cacheRequestStrategy != resource.CacheRequestStrategyLiveCancelsCached || !withLiveMessages {
				/* NOTE: Ideally, the cachedOnlyChannel received a response before the concurrent cache request was
				 * received by the cache instance, and the API should still not have received a response. However, it is
				 * possible that latency in test infrastructure causes the response from the first cache request to be
				 * received prematurely, in which case we should fail because the test is not behaving as intended and
				 * the test results are no longer reliable.
				 */
				Consistently(concurrentChannel, "1ms").ShouldNot(Receive())
			}
			/* NOTE: Now that we have asserted the concurrent cache response was not received prematurely, we can
			 * wait for it to be delivered.
			 */
			Eventually(concurrentChannel, delayAsTime+(time.Second*1)).Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID */
			/* EBP-26: Assert CacheRequestOutcome */
			/* EBP-28: Assert err */
			var waitForLiveMessages = func(numExpectedLiveMessages int) {
				for i := 0; i < numExpectedLiveMessages; i++ {
					fmt.Printf("waiting on live messages")
					var msg message.InboundMessage
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					Expect(msg.GetDestinationName()).To(Equal(concurrentCacheTopic))
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeFalse())
					Expect(id).To(BeNumerically("==", 0))
					/* EBP-21: Assert live message. */
				}
			}
			var waitForCachedMessages = func(numExpectedCachedMessages int, expectedCacheRequestId message.CacheRequestID) {
				var expectedTopic string
				switch expectedCacheRequestId {
				case cachedOnlyCacheRequestID:
					fmt.Println("waiting on cachedOnly messages")
					expectedTopic = cachedOnlyCacheTopic
				case concurrentCacheRequestID:
					fmt.Println("waiting on concurrent messages")
					expectedTopic = concurrentCacheTopic
				default:
					Fail("Got unexpected cache request ID")
				}
				for i := 0; i < numExpectedCachedMessages; i++ {
					fmt.Printf("waiting on message %d of %d\n", i, numExpectedCachedMessages)
					var msg message.InboundMessage
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					Expect(msg.GetDestinationName()).To(Equal(expectedTopic))
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeTrue())
					Expect(id).To(BeNumerically("==", expectedCacheRequestId))
					/* EBP-21: Assert live message. */
				}
			}
			numExpectedCachedMessages := numConfiguredCachedMessages
			switch cacheRequestStrategy {
			case resource.CacheRequestStrategyLiveCancelsCached:
				if withLiveMessages {
					numExpectedLiveMessages := numConfiguredLiveMessages
					waitForLiveMessages(numExpectedLiveMessages)
					waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				} else {
					waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
					waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
				}
			case resource.CacheRequestStrategyAsAvailable:
				if withLiveMessages {
					numExpectedLiveMessages := numConfiguredLiveMessages
					waitForLiveMessages(numExpectedLiveMessages)
				}
				waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
			case resource.CacheRequestStrategyCachedFirst:
				waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
				if withLiveMessages {
					numExpectedLiveMessages := numConfiguredLiveMessages
					waitForLiveMessages(numExpectedLiveMessages)
				}
			case resource.CacheRequestStrategyCachedOnly:
				waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
			default:
				Fail("Got unrecognized cacheRequestStrategy")
			}
			/* NOTE: Assert the receiver did not receive further messages. */
			Consistently(receivedMsgChan, "1ms").ShouldNot(Receive())
		},
			Entry("with a LiveCancelsCached without live messages", resource.CacheRequestStrategyLiveCancelsCached, false),
			Entry("with a LiveCancelsCached with live messages", resource.CacheRequestStrategyLiveCancelsCached, true),
			Entry("with a AsAvailable without live messages", resource.CacheRequestStrategyAsAvailable, false),
			Entry("with a AsAvailable with live messages", resource.CacheRequestStrategyAsAvailable, true),
			Entry("with a CachedFirst without live messages", resource.CacheRequestStrategyCachedFirst, false),
			Entry("with a CachedFirst with live messages", resource.CacheRequestStrategyCachedFirst, true),
			Entry("with a CachedOnly without live messages", resource.CacheRequestStrategyCachedOnly, false),
			Entry("with a CachedOnly with live messages", resource.CacheRequestStrategyCachedOnly, true),
		)
		DescribeTable("CachedOnly cache requests on the same topic on the same receiver concurrent", func(cacheRequestStrategy resource.CachedMessageSubscriptionStrategy, withLiveMessages bool) {
			numConfiguredCachedMessages := 3
			numConfiguredLiveMessages := 1
			delay := 2000
			delayAsTime := time.Second * time.Duration((delay / 1000))
			concurrentCacheRequestID := message.CacheRequestID(1)
			cachedOnlyCacheRequestID := message.CacheRequestID(2)
			var concurrentCacheName string
			cachedOnlyCacheName := fmt.Sprintf("MaxMsgs%d", numConfiguredCachedMessages)
			if withLiveMessages {
				concurrentCacheName = fmt.Sprintf("%s/delay=%d,msgs=%d", cachedOnlyCacheName, delay, numConfiguredLiveMessages)
			} else {
				concurrentCacheName = fmt.Sprintf("%s/delay=%d", cachedOnlyCacheName, delay)
			}
			cacheTopic := fmt.Sprintf("MaxMsgs%d/%s/data1", numConfiguredCachedMessages, testcontext.Cache().Vpn)
			cachedOnlyCacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyCachedOnly, cachedOnlyCacheName, resource.TopicSubscriptionOf(cacheTopic), int32(5000), int32(5000), int32(5000))
			concurrentCacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(cacheRequestStrategy, concurrentCacheName, resource.TopicSubscriptionOf(cacheTopic), int32(5000), int32(5000), int32(5000))

			/* Channel size of 10 is an arbitrary number over the anticipated 7 received messages. */
			receivedMsgChan := make(chan message.InboundMessage, 10)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())

			concurrentChannel, err := receiver.RequestCachedAsync(concurrentCacheRequestConfig, concurrentCacheRequestID)
			Expect(err).To(BeNil())
			Expect(concurrentChannel).ToNot(BeNil())
			if withLiveMessages && (cacheRequestStrategy == resource.CacheRequestStrategyLiveCancelsCached || cacheRequestStrategy == resource.CacheRequestStrategyAsAvailable) {
				/* NOTE: We delay before sending the second cache request to mitigate the risk of cached messages
				 * from the second request arriving before live messages from the first request. This allows us
				 * to test received message ordering later. This only matters for LiveCancelsCached and
				 * AsAvailable, because those strategies do not dictate ordering of received messages. If
				 * CachedOnly or CachedFirst requests are being sent, any received live messages will be
				 * deferred or ignored.
				 */
				time.Sleep(time.Millisecond * 500)
			}
			cachedOnlyChannel, err := receiver.RequestCachedAsync(cachedOnlyCacheRequestConfig, cachedOnlyCacheRequestID)
                    var cacheResponse solace.CacheResponse
            if cacheRequestStrategy != resource.CacheRequestStrategyLiveCancelsCached && cacheRequestStrategy != resource.CacheRequestStrategyCachedFirst {
			    Expect(err).To(BeNil())
			    Expect(cachedOnlyChannel).ToNot(BeNil())
                    /* NOTE: We only wait for a cache response for as much as the delay because the point of the test is to
                     * receive the CachedOnly response while the other request is still in-flight. The proxy will ensure
                     * that the other request is not received by the cache instance for at least `delay` seconds. If the
                     * CachedOnly response is received after that delay has expired, the test results may no longer be
                     * reliable.
                     */
                    Eventually(cachedOnlyChannel, delayAsTime).Should(Receive(&cacheResponse))
                    Expect(cacheResponse).ToNot(BeNil())
                    /* EBP-25: Assert cache request ID */
                    /* EBP-26: Assert CacheRequestOutcome */
                    /* EBP-28: Assert err */
            } else {
                Expect(err).ToNot(BeNil())
                helpers.ValidateNativeError(err, subcode.CacheAlreadyInProgress)
                Expect(cachedOnlyChannel).To(BeNil())
            }

			if !withLiveMessages {
				/* NOTE: Ideally, the cachedOnlyChannel received a response before the concurrent cache request was
				 * received by the cache instance, and the API should still not have received a response. However, it is
				 * possible that latency in test infrastructure causes the response from the first cache request to be
				 * received prematurely, in which case we should fail because the test is not behaving as intended and
				 * the test results are no longer reliable.
				 */
				Consistently(concurrentChannel, "1ms").ShouldNot(Receive())
			}
			/* NOTE: Now that we have asserted the concurrent cache response was not received prematurely, we can
			 * wait for it to be delivered.
			 */
			Eventually(concurrentChannel, delayAsTime+(time.Second*1)).Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID */
			/* EBP-26: Assert CacheRequestOutcome */
			/* EBP-28: Assert err */
			var waitForLiveMessages = func(numExpectedLiveMessages int) {
				fmt.Printf("waiting on live messages\n")
				for i := 0; i < numExpectedLiveMessages; i++ {
					fmt.Printf("waiting on message %d of %d\n", i, numExpectedLiveMessages)
					var msg message.InboundMessage
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					Expect(msg.GetDestinationName()).To(Equal(cacheTopic))
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeFalse())
					Expect(id).To(BeNumerically("==", 0))
					/* EBP-21: Assert live message. */
				}
			}
			var waitForCachedMessages = func(numExpectedCachedMessages int, expectedCacheRequestId message.CacheRequestID) {
				fmt.Printf("waiting on cached messages\n")
				for i := 0; i < numExpectedCachedMessages; i++ {
					fmt.Printf("waiting on message %d of %d\n", i, numExpectedCachedMessages)
					var msg message.InboundMessage
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					Expect(msg.GetDestinationName()).To(Equal(cacheTopic))
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeTrue())
					Expect(id).To(BeNumerically("==", expectedCacheRequestId))
					/* EBP-21: Assert live message. */
				}
			}
			numExpectedCachedMessages := numConfiguredCachedMessages
			switch cacheRequestStrategy {
                    /* re-enable once EBP-638 is resolved.
            case resource.CacheRequestStrategyLiveCancelsCached:
                    if withLiveMessages {
                            numExpectedLiveMessages := numConfiguredLiveMessages
                        waitForLiveMessages(numExpectedLiveMessages)
                    }
                    waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
                    */
			case resource.CacheRequestStrategyAsAvailable:
				if withLiveMessages {
					numExpectedLiveMessages := numConfiguredLiveMessages
					waitForLiveMessages(numExpectedLiveMessages)
				}
				/* NOTE: We expect twice as many cached messages matching the CachedOnly ID because
				 * there will be one set of messages from that the corresponding cache response that
				 * is given to the application through the topic dispatch associated with the
				 * CachedOnly request, and one set of messges given to the application through the
				 * topic dispatch associated with the concurrent request. This is due to overlapping
				 * subscriptions on identical topics but with different callbacks.
				 */
				waitForCachedMessages(numExpectedCachedMessages*2, cachedOnlyCacheRequestID)
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
			case resource.CacheRequestStrategyCachedFirst:
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
				if withLiveMessages {
					numExpectedLiveMessages := numConfiguredLiveMessages
					waitForLiveMessages(numExpectedLiveMessages)
				}
			case resource.CacheRequestStrategyCachedOnly:
				waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
			default:
				Fail("Got unrecognized cacheRequestStrategy")
			}
			/* NOTE: Assert the receiver did not receive further messages. */
			Consistently(receivedMsgChan, "1ms").ShouldNot(Receive())
		},
            /* NOTE: LiveCancelsCached is disabled for this test until EBP-638 is resolved. These variants of the test
             * are not critical to the use case coverage intended by this test, so it's fine for them to be disabled
             * for now. These variants are expected to fail immediately, but do not as per the description of EBP-638.
             * That use case is covered in other tests within the test suite, and is not the focus of this test. The
             * focus of this test is to assert the behaviour of received messages when there are identical/overlapping
             * subscriptions on the same receiver for concurrent requests.
             */
			//Entry("with a LiveCancelsCached without live messages", resource.CacheRequestStrategyLiveCancelsCached, false),
			//Entry("with a LiveCancelsCached with live messages", resource.CacheRequestStrategyLiveCancelsCached, true),
			Entry("with a AsAvailable without live messages", resource.CacheRequestStrategyAsAvailable, false),
			Entry("with a AsAvailable with live messages", resource.CacheRequestStrategyAsAvailable, true),
			Entry("with a CachedFirst without live messages", resource.CacheRequestStrategyCachedFirst, false),
			Entry("with a CachedFirst with live messages", resource.CacheRequestStrategyCachedFirst, true),
			Entry("with a CachedOnly without live messages", resource.CacheRequestStrategyCachedOnly, false),
			Entry("with a CachedOnly with live messages", resource.CacheRequestStrategyCachedOnly, true),
		)
		DescribeTable("CachedOnly cache requests on different topics on different receivers concurrent", func(cacheRequestStrategy resource.CachedMessageSubscriptionStrategy, withLiveMessages bool) {
                cachedOnlyReceiver, err := messagingService.CreateDirectMessageReceiverBuilder().Build()
                Expect(err).To(BeNil())
                Expect(cachedOnlyReceiver).ToNot(BeNil())
                err = cachedOnlyReceiver.Start()
                Expect(err).To(BeNil())
			/* Channel size of 10 is an arbitrary number over the anticipated 7 received messages. */
			cachedOnlyMsgChan := make(chan message.InboundMessage, 10)
			err = cachedOnlyReceiver.ReceiveAsync(func(msg message.InboundMessage) {
                    fmt.Printf("Received message on cachedOnly receiver:\n%s\n", msg.String())
				cachedOnlyMsgChan <- msg
			})
			numConfiguredCachedMessages := 3
			numConfiguredLiveMessages := 1
			delay := 2000
			delayAsTime := time.Second * time.Duration((delay / 1000))
			concurrentCacheRequestID := message.CacheRequestID(1)
			cachedOnlyCacheRequestID := message.CacheRequestID(2)
			var concurrentCacheName string
			cachedOnlyCacheName := fmt.Sprintf("MaxMsgs%d", numConfiguredCachedMessages)
			if withLiveMessages {
				concurrentCacheName = fmt.Sprintf("%s/delay=%d,msgs=%d", cachedOnlyCacheName, delay, numConfiguredLiveMessages)
			} else {
				concurrentCacheName = fmt.Sprintf("%s/delay=%d", cachedOnlyCacheName, delay)
			}
			concurrentCacheTopic := fmt.Sprintf("MaxMsgs%d/%s/data1", numConfiguredCachedMessages, testcontext.Cache().Vpn)
			cachedOnlyCacheTopic := fmt.Sprintf("MaxMsgs%d/%s/data2", numConfiguredCachedMessages, testcontext.Cache().Vpn)
			cachedOnlyCacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyCachedOnly, cachedOnlyCacheName, resource.TopicSubscriptionOf(cachedOnlyCacheTopic), int32(5000), int32(5000), int32(5000))
			concurrentCacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(cacheRequestStrategy, concurrentCacheName, resource.TopicSubscriptionOf(concurrentCacheTopic), int32(5000), int32(5000), int32(5000))

			/* Channel size of 10 is an arbitrary number over the anticipated 7 received messages. */
			receivedMsgChan := make(chan message.InboundMessage, 10)
			err = receiver.ReceiveAsync(func(msg message.InboundMessage) {
                    fmt.Printf("Received message on concurrent receiver:\n%s\n", msg.String())
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())

			concurrentChannel, err := receiver.RequestCachedAsync(concurrentCacheRequestConfig, concurrentCacheRequestID)
			Expect(err).To(BeNil())
			Expect(concurrentChannel).ToNot(BeNil())
			if withLiveMessages && (cacheRequestStrategy == resource.CacheRequestStrategyLiveCancelsCached || cacheRequestStrategy == resource.CacheRequestStrategyAsAvailable) {
				/* NOTE: We delay before sending the second cache request to mitigate the risk of cached messages
				 * from the second request arriving before live messages from the first request. This allows us
				 * to test received message ordering later. This only matters for LiveCancelsCached and
				 * AsAvailable, because those strategies do not dictate ordering of received messages. If
				 * CachedOnly or CachedFirst requests are being sent, any received live messages will be
				 * deferred or ignored.
				 */
				time.Sleep(time.Millisecond * 500)
			}
			cachedOnlyChannel, err := cachedOnlyReceiver.RequestCachedAsync(cachedOnlyCacheRequestConfig, cachedOnlyCacheRequestID)
			Expect(err).To(BeNil())
			Expect(cachedOnlyChannel).ToNot(BeNil())
			/* NOTE: We only wait for a cache response for as much as the delay because the point of the test is to
			 * receive the CachedOnly response while the other request is still in-flight. The proxy will ensure
			 * that the other request is not received by the cache instance for at least `delay` seconds. If the
			 * CachedOnly response is received after that delay has expired, the test results may no longer be
			 * reliable.
			 */
			var cacheResponse solace.CacheResponse
			Eventually(cachedOnlyChannel, delayAsTime).Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID */
			/* EBP-26: Assert CacheRequestOutcome */
			/* EBP-28: Assert err */
			/* NOTE: If using LiveCancelsCached */
			if cacheRequestStrategy != resource.CacheRequestStrategyLiveCancelsCached || !withLiveMessages {
				/* NOTE: Ideally, the cachedOnlyChannel received a response before the concurrent cache request was
				 * received by the cache instance, and the API should still not have received a response. However, it is
				 * possible that latency in test infrastructure causes the response from the first cache request to be
				 * received prematurely, in which case we should fail because the test is not behaving as intended and
				 * the test results are no longer reliable.
				 */
				Consistently(concurrentChannel, "1ms").ShouldNot(Receive())
			}
			/* NOTE: Now that we have asserted the concurrent cache response was not received prematurely, we can
			 * wait for it to be delivered.
			 */
			Eventually(concurrentChannel, delayAsTime+(time.Second*1)).Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID */
			/* EBP-26: Assert CacheRequestOutcome */
			/* EBP-28: Assert err */
			var waitForLiveMessages = func(numExpectedLiveMessages int) {
				for i := 0; i < numExpectedLiveMessages; i++ {
					fmt.Printf("waiting on live messages")
					var msg message.InboundMessage
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					Expect(msg.GetDestinationName()).To(Equal(concurrentCacheTopic))
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeFalse())
					Expect(id).To(BeNumerically("==", 0))
					/* EBP-21: Assert live message. */
				}
			}
			var waitForCachedMessages = func(numExpectedCachedMessages int, expectedCacheRequestId message.CacheRequestID) {
				var expectedTopic string
                var msgChan chan message.InboundMessage
				switch expectedCacheRequestId {
				case cachedOnlyCacheRequestID:
					fmt.Println("waiting on cachedOnly messages")
                    msgChan = cachedOnlyMsgChan
					expectedTopic = cachedOnlyCacheTopic
				case concurrentCacheRequestID:
					fmt.Println("waiting on concurrent messages")
                    msgChan = receivedMsgChan
					expectedTopic = concurrentCacheTopic
				default:
					Fail("Got unexpected cache request ID")
				}
				for i := 0; i < numExpectedCachedMessages; i++ {
					fmt.Printf("waiting on message %d of %d\n", i, numExpectedCachedMessages)
					var msg message.InboundMessage
					Eventually(msgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
                    fmt.Printf("Drained message:\n%s\n", msg.String())
					Expect(msg.GetDestinationName()).To(Equal(expectedTopic))
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeTrue())
					Expect(id).To(BeNumerically("==", expectedCacheRequestId))
					/* EBP-21: Assert live message. */
				}
			}
			numExpectedCachedMessages := numConfiguredCachedMessages
			switch cacheRequestStrategy {
                    /* re-enable once EBP-638 is resolved.
			case resource.CacheRequestStrategyLiveCancelsCached:
				if withLiveMessages {
					numExpectedLiveMessages := numConfiguredLiveMessages
					waitForLiveMessages(numExpectedLiveMessages)
					waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				} else {
					waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
					waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
				}
                */
			case resource.CacheRequestStrategyAsAvailable:
				if withLiveMessages {
					numExpectedLiveMessages := numConfiguredLiveMessages
					waitForLiveMessages(numExpectedLiveMessages)
				}
				waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
			case resource.CacheRequestStrategyCachedFirst:
				waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
				if withLiveMessages {
					numExpectedLiveMessages := numConfiguredLiveMessages
					waitForLiveMessages(numExpectedLiveMessages)
				}
			case resource.CacheRequestStrategyCachedOnly:
				waitForCachedMessages(numExpectedCachedMessages, cachedOnlyCacheRequestID)
				waitForCachedMessages(numExpectedCachedMessages, concurrentCacheRequestID)
			default:
				Fail("Got unrecognized cacheRequestStrategy")
			}
			/* NOTE: Assert the receiver did not receive further messages. */
			Consistently(receivedMsgChan, "1ms").ShouldNot(Receive())

            err = cachedOnlyReceiver.Terminate(0)
            Expect(err).To(BeNil())
		},
        /* NOTE: Re-enable once EBP-638 is resolved. See previous test comment for details.*/
			//Entry("with a LiveCancelsCached without live messages", resource.CacheRequestStrategyLiveCancelsCached, false),
			//Entry("with a LiveCancelsCached with live messages", resource.CacheRequestStrategyLiveCancelsCached, true),
			Entry("with a AsAvailable without live messages", resource.CacheRequestStrategyAsAvailable, false),
			Entry("with a AsAvailable with live messages", resource.CacheRequestStrategyAsAvailable, true),
			Entry("with a CachedFirst without live messages", resource.CacheRequestStrategyCachedFirst, false),
			Entry("with a CachedFirst with live messages", resource.CacheRequestStrategyCachedFirst, true),
			Entry("with a CachedOnly without live messages", resource.CacheRequestStrategyCachedOnly, false),
			Entry("with a CachedOnly with live messages", resource.CacheRequestStrategyCachedOnly, true),
		)
		It("a direct receiver should get CacheRequestOutcome.Suspect when there is at least one suspect message in the cache response", func() {
			cacheRequestID := message.CacheRequestID(1)
			cacheName := "UnitTestSuspect"
			topic := "Suspect/data1"
			cacheRequestConfig := helpers.GetValidCacheRequestStrategyCachedFirstCacheRequestConfig(cacheName, topic)
			receivedMsgChan := make(chan message.InboundMessage, 1)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())
			channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(err).To(BeNil())
			Expect(channel).ToNot(BeNil())
			var cacheResponse solace.CacheResponse
			Eventually(channel, "5s").Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache response ID matches request ID. */
			/* EBP-26: Assert CacheRequestOutcome is Suspect. */
			/* EBP-28: Assert err is nil */
			var msg message.InboundMessage
			Eventually(receivedMsgChan).Should(Receive(&msg))
			Expect(msg).ToNot(BeNil())
			Expect(msg.GetDestinationName()).To(Equal(topic))
			id, ok := msg.GetCacheRequestID()
			Expect(ok).To(BeTrue())
			Expect(id).To(BeNumerically("==", cacheRequestID))
			/* EBP-21: Assert this message is suspect. */
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
			strategy := resource.CacheRequestStrategyAsAvailable
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
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyAsAvailable, cacheName, resource.TopicSubscriptionOf(topic), int32(delay+1000), helpers.ValidMaxCachedMessages, helpers.ValidCachedMessageAge)
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
			cacheRequestConfig := helpers.GetValidCacheRequestConfig(resource.CacheRequestStrategyAsAvailable, cacheName, topic)

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
		It("requests subsequent to non-wildcard AsAvailable request are rejected, except for AsAvailable and CachedOnly", func() {
			firstCacheRequestID := message.CacheRequestID(1)
			numExpectedCachedMessages := 3
			cacheName := fmt.Sprintf("MaxMsgs%d/delay=10000", numExpectedCachedMessages)
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyAsAvailable, cacheName, resource.TopicSubscriptionOf(topic), int32(12000), int32(0), int32(0))
			/* NOTE: We expect the first AsAvailable, second AsAvailable, and CachedOnly requests to succeed, so our
			 * application buffer may contain as many as 3 times the number of cached messages expected from a single
			 * cache request to MaxMsgs3 before we are able to clear it.
			 */
			receivedMsgChan := make(chan message.InboundMessage, numExpectedCachedMessages*3)
			receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			firstChannel, err := receiver.RequestCachedAsync(cacheRequestConfig, firstCacheRequestID)
			Expect(err).To(BeNil())
			Expect(firstChannel).ToNot(BeNil())
			cacheName = fmt.Sprintf("MaxMsgs%d", numExpectedCachedMessages)

			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }).Should(BeNumerically("==", 1))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))

			/* NOTE: Subsequent LiveCancelsCached fails. */
			cacheRequestConfig = helpers.GetValidCacheRequestStrategyLiveCancelsCachedRequestConfig(cacheName, topic)
			secondCacheRequestID := message.CacheRequestID(2)
			secondChannel, err := receiver.RequestCachedAsync(cacheRequestConfig, secondCacheRequestID)
			Expect(err).To(BeAssignableToTypeOf(&solace.NativeError{}))
			helpers.ValidateNativeError(err, subcode.CacheAlreadyInProgress)
			Expect(secondChannel).To(BeNil())
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 1))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))

			/* NOTE: Subsequent CachedFirst fails. */
			cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedFirstCacheRequestConfig(cacheName, topic)
			thirdCacheRequestID := message.CacheRequestID(3)
			thirdChannel, err := receiver.RequestCachedAsync(cacheRequestConfig, thirdCacheRequestID)
			Expect(err).To(BeAssignableToTypeOf(&solace.NativeError{}))
			helpers.ValidateNativeError(err, subcode.CacheAlreadyInProgress)
			Expect(thirdChannel).To(BeNil())
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 1))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))

			var cacheResponse1 solace.CacheResponse
			cacheName = fmt.Sprintf("MaxMsgs%d", numExpectedCachedMessages)

			/* NOTE: Subsequent CachedOnly succeeds. */
			cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedOnlyCacheRequestConfig(cacheName, topic)
			fourthCacheRequestID := message.CacheRequestID(4)
			fourthChannel, err := receiver.RequestCachedAsync(cacheRequestConfig, fourthCacheRequestID)
			Expect(err).To(BeNil())
			Expect(fourthChannel).ToNot(BeNil())
			Eventually(fourthChannel, "10s").Should(Receive(&cacheResponse1))
			Expect(cacheResponse1).ToNot(BeNil())
			/* EBP-25: Assert cache request ID matches cache response ID. */
			/* EBP-26: Assert CacheRequestOutcome Ok. */
			/* EBP-28: Assert err is nil. */
			for i := 0; i < numExpectedCachedMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeTrue())
				Expect(id).To(BeNumerically("==", fourthCacheRequestID))
				/* EBP-21: Assert that this message is cached. */
			}
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }).Should(BeNumerically("==", 2))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 1))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))

			/* NOTE: Subsequent AsAvailable succeeds. */
			var cacheResponse2 solace.CacheResponse
			cacheRequestConfig = helpers.GetValidCacheRequestStrategyAsAvailableCacheRequestConfig(cacheName, topic)
			fifthCacheRequestID := message.CacheRequestID(5)
			fifthChannel, err := receiver.RequestCachedAsync(cacheRequestConfig, fifthCacheRequestID)
			Expect(err).To(BeNil())
			Expect(fifthChannel).ToNot(BeNil())
			Eventually(fifthChannel, "10s").Should(Receive(&cacheResponse2))
			Expect(cacheResponse2).ToNot(BeNil())
			/* EBP-25: Assert cache request ID matches cache response ID. */
			/* EBP-26: Assert CacheRequestOutcome Ok. */
			/* EBP-28: Assert err is nil. */
			for i := 0; i < numExpectedCachedMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeTrue())
				Expect(id).To(BeNumerically("==", fifthCacheRequestID))
				/* EBP-21: Assert that this message is cached. */
			}
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }).Should(BeNumerically("==", 3))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 2))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))

			/* NOTE: First AsAvailable cache request should succeed. */
			var cacheResponse3 solace.CacheResponse
			Eventually(firstChannel, "10s").Should(Receive(&cacheResponse3))
			Expect(cacheResponse3).ToNot(BeNil())
			/* EBP-25: Assert cache request ID matches cache response ID. */
			/* EBP-26: Assert CacheRequestOutcome Ok. */
			/* EBP-28: Assert err is nil. */

			for i := 0; i < numExpectedCachedMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeTrue())
				Expect(id).To(BeNumerically("==", firstCacheRequestID))
				/* EBP-21: Assert that this message is cached. */
			}
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 3))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 3))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
		})
		It("a direct receiver that tries to submit more than the maximum number of cache requests should get an IllegalStateError", func() {
			err := receiver.ReceiveAsync(func(message.InboundMessage) {})
			Expect(err).To(BeNil())
			maxCacheRequests := 1024
			/* NOTE: First we will fill the internal buffer, then we will try one more and assert an error */
			numExpectedCachedMessages := 3
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
			cacheName := fmt.Sprintf("MaxMsgs%d", numExpectedCachedMessages)
			cacheRequestConfig := helpers.GetValidCacheRequestConfig(resource.CacheRequestStrategyAsAvailable, cacheName, topic)
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
		It("cache request fails on expired timeout", func() {
			cacheRequestID := message.CacheRequestID(1)
			cacheName := "MaxMsgs3/delay=3500"
			topic := fmt.Sprintf("MaxMsgs3/%s/data1", testcontext.Cache().Vpn)
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyCachedFirst, cacheName, resource.TopicSubscriptionOf(topic), 3000, helpers.ValidMaxCachedMessages, helpers.ValidCachedMessageAge)
			/* NOTE: Chan size 3 in case we get unexpected msgs to avoid hang in termination. */
			receivedMsgChan := make(chan message.InboundMessage, 3)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())
			channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(err).To(BeNil())
			Expect(channel).ToNot(BeNil())
			Consistently(channel, "2.5s").ShouldNot(Receive())
			var cacheResponse solace.CacheResponse
			Eventually(channel, "5s").Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID. */
			/* EBP-26: Assert cache request outcome failed. */
			/* EBP-28: Assert CACHE_TIMEOUT sc and CACHE_INCOMPLETE rc in err. */
			Consistently(receivedMsgChan).ShouldNot(Receive())
			Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 1))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
			Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
		})
		DescribeTable("cache request when there is no cached data available", func(strategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
			cacheRequestID := message.CacheRequestID(1)
			cacheName := "MaxMsgs1"
			topic := fmt.Sprintf("%s/%s/nodata", cacheName, testcontext.Cache().Vpn)
			var cacheRequestConfig resource.CachedMessageSubscriptionRequest
			switch strategy {
			case resource.CacheRequestStrategyAsAvailable:
				cacheRequestConfig = helpers.GetValidCacheRequestStrategyAsAvailableCacheRequestConfig(cacheName, topic)
			case resource.CacheRequestStrategyCachedOnly:
				cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedOnlyCacheRequestConfig(cacheName, topic)
			case resource.CacheRequestStrategyCachedFirst:
				cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedFirstCacheRequestConfig(cacheName, topic)
			case resource.CacheRequestStrategyLiveCancelsCached:
				cacheRequestConfig = helpers.GetValidCacheRequestStrategyLiveCancelsCachedRequestConfig(cacheName, topic)
			default:
				Fail("Got unrecognized cache request strategy")
			}
			/* NOTE: we don't expect to get a message, but having a buffer of 1 will mitigate the risk of this test
			 * hanging on terminate because the receiver callback has to write to the test buffer. The size is 1
			 * because if there is an error we expect MaxMsgs1 to return only 1 message.
			 */
			receivedMsgChan := make(chan message.InboundMessage, 1)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())
			var cacheResponse solace.CacheResponse
			switch cacheResponseProcessStrategy {
			case helpers.ProcessCacheResponseThroughChannel:
				channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
				Expect(err).To(BeNil())
				Expect(channel).ToNot(BeNil())
				Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "10s").Should(BeNumerically("==", 1))
				Eventually(channel, "10s").Should(Receive(&cacheResponse))
			case helpers.ProcessCacheResponseThroughCallback:
				cacheResponseChan := make(chan solace.CacheResponse, 1)
				callback := func(cacheResponse solace.CacheResponse) {
					cacheResponseChan <- cacheResponse
				}
				err := receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, callback)
				Expect(err).To(BeNil())
				Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "10s").Should(BeNumerically("==", 1))
				Eventually(cacheResponseChan, "10s").Should(Receive(&cacheResponse))
			default:
				Fail("Got unrecognized cache response process strategy")
			}
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert response ID matches request ID. */
			/* EBP-26: Assert cache request outcome. */
			/* EBP-28: Assert response err. */
			Consistently(receivedMsgChan).ShouldNot(Receive())
		},
			Entry("with CacheRequestStrategyCachedFirst and channel", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("with CacheRequestStrategyCachedFirst and callback", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughCallback),
			Entry("with CacheRequestStrategyCachedOnly and channel", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughChannel),
			Entry("with CacheRequestStrategyCachedOnly and callback", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughCallback),
			Entry("with CacheRequestStrategyAsAvailable and channel", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughChannel),
			Entry("with CacheRequestStrategyAsAvailable and callback", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughCallback),
			Entry("with CacheRequestStrategyLiveCancelsCached and channel", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("with CacheRequestStrategyLiveCancelsCached and callback", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
		)
		It("a cache request will return the expected number of cached messages based on configured cache message age", func() {
			cacheRequestID := message.CacheRequestID(1)
			cacheName := "MaxMsgs1"
			cacheTopic := fmt.Sprintf("%s/%s/data1", cacheName, testcontext.Cache().Vpn)
			/* NOTE: We're expecting to get 2 messages, 5 gives plenty of buffer in case of errors. */
			receivedMsgChan := make(chan message.InboundMessage, 5)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			/* NOTE: Cache request with max age `0` should retrieve all messages, in this case 1. */
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyAsAvailable, cacheName, resource.TopicSubscriptionOf(cacheTopic), helpers.ValidCacheAccessTimeout, helpers.ValidMaxCachedMessages, int32(0))
			channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(err).To(BeNil())
			Expect(channel).ToNot(BeNil())
			var cacheResponse solace.CacheResponse
			Eventually(channel, "5s").Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert response ID matches request ID. */
			/* EBP-26: Assert response CacheRequestOutcome is Ok. */
			/* EBP-28: Assert response err is nil */
			var msg message.InboundMessage
			Eventually(receivedMsgChan, "5s").Should(Receive(&msg))
			Expect(msg).ToNot(BeNil())
			Expect(msg.GetDestinationName()).To(Equal(cacheTopic))
			id, ok := msg.GetCacheRequestID()
			Expect(ok).To(BeTrue())
			Expect(id).To(BeNumerically("==", cacheRequestID))
			/* EBP-21: Assert this is a cached message. */
			/* NOTE: Because we waited for the cache response, we only need to poll the data message channel
			 * instantaneously.
			 */
			Consistently(receivedMsgChan, "1ms").ShouldNot(Receive())

			/* NOTE: Cache request with max age `1` should receive no messages because we first wait for 2s. This
			 * guarantees that the cache instance was populated at least 2s ago, making its cached messages older
			 * than 1ms. Only messages 1ms or newer should be returned, so none should be returned.
			 */
			time.Sleep(time.Second * 2)
			cacheRequestConfig = resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyAsAvailable, cacheName, resource.TopicSubscriptionOf(cacheTopic), helpers.ValidCacheAccessTimeout, helpers.ValidMaxCachedMessages, int32(1))
			channel, err = receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(err).To(BeNil())
			Expect(channel).ToNot(BeNil())
			Eventually(channel, "5s").Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert response ID matches request ID. */
			/* EBP-26: Assert response CacheRequestOutcome. */
			/* EBP-28: Assert response err. */
			/* NOTE: Because we waited for the cache response, we only need to poll the data message channel
			 * instantaneously.
			 */
			Consistently(receivedMsgChan, "1ms").ShouldNot(Receive())

			/* NOTE: Cache request with max age `10000` should retrieve all messages, in this case 1. */
			cacheRequestConfig = resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyAsAvailable, cacheName, resource.TopicSubscriptionOf(cacheTopic), helpers.ValidCacheAccessTimeout, helpers.ValidMaxCachedMessages, int32(10000))
			channel, err = receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(err).To(BeNil())
			Expect(channel).ToNot(BeNil())
			Eventually(channel, "5s").Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert response ID matches request ID. */
			/* EBP-26: Assert response CacheRequestOutcome is Ok. */
			/* EBP-28: Assert response err is nil */
			Eventually(receivedMsgChan, "5s").Should(Receive(&msg))
			Expect(msg).ToNot(BeNil())
			Expect(msg.GetDestinationName()).To(Equal(cacheTopic))
			id, ok = msg.GetCacheRequestID()
			Expect(ok).To(BeTrue())
			/* EBP-21: Assert this is a cached message. */
			/* NOTE: Because we waited for the cache response, we only need to poll the data message channel
			 * instantaneously.
			 */
			Consistently(receivedMsgChan, "1ms").ShouldNot(Receive())
			Expect(id).To(BeNumerically("==", cacheRequestID))
		})
		DescribeTable("long running cache requests with live data queue and live data to fill", func(cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
			numExpectedCachedMessages := 3
			numExpectedLiveMessages := 100000
			delay := 10000
			numExpectedReceivedMessages := numExpectedCachedMessages + numExpectedLiveMessages
			receivedMsgChan := make(chan message.InboundMessage, numExpectedReceivedMessages)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())
			cacheName := fmt.Sprintf("MaxMsgs%d/delay=%d,msgs=%d", numExpectedCachedMessages, delay, numExpectedLiveMessages)
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
			cacheRequestID := message.CacheRequestID(1)
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyCachedFirst, cacheName, resource.TopicSubscriptionOf(topic), 45000, 0, 50000)
			var cacheResponse solace.CacheResponse
			/* NOTE: We need to wait for longer than usual for the cache response (10s) since the cache response is
			 * given to the application only after all messages related to the cache request have been received by
			 * the API. Since 100000 live messages are being received as a part of the cache response, the cache
			 * response ends up taking a lot longer.
			 */
			switch cacheResponseProcessStrategy {
			case helpers.ProcessCacheResponseThroughCallback:
				channel := make(chan solace.CacheResponse, 1)
				callback := func(cacheResponse solace.CacheResponse) {
					channel <- cacheResponse
				}
				err = receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, callback)
				Expect(err).To(BeNil())
				Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }).Should(BeNumerically("==", 1))
				Consistently(channel, "9.5s").ShouldNot(Receive())
				Eventually(channel, "10s").Should(Receive(&cacheResponse))
			case helpers.ProcessCacheResponseThroughChannel:
				channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
				Expect(err).To(BeNil())
				Expect(channel).ToNot(BeNil())
				Consistently(channel, "9.5s").ShouldNot(Receive(&cacheResponse))
				Eventually(channel, "10s").Should(Receive(&cacheResponse))
			default:
				Fail("Got unexpected cache response process strategy")
			}
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID from response is the same as the request */
			/* EBP-26: Assert cache request Outcome is Ok. */
			/* EBP-28: Assert error from cache response is nil */

			/* NOTE: Check the cached messages first. */
			for i := 0; i < numExpectedCachedMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg), fmt.Sprintf("Timed out waiting to receive message %d of %d", i, numExpectedLiveMessages))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeTrue())
				Expect(id).To(BeNumerically("==", cacheRequestID))
				/* EBP-21: Assert that this message is a cached message. */
			}
			/* NOTE: Check the live messages second. */
			for i := 0; i < numExpectedLiveMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg), fmt.Sprintf("Timed out waiting to receive message %d of %d with %d messages dropped from back pressure", i, numExpectedLiveMessages, messagingService.Metrics().GetValue(metrics.ReceivedMessagesBackpressureDiscarded)))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeFalse())
				Expect(id).To(BeNumerically("==", 0))
				Expect(msg.GetMessageDiscardNotification().HasBrokerDiscardIndication()).To(BeFalse())
				Expect(msg.GetMessageDiscardNotification().HasInternalDiscardIndication()).To(BeFalse())
				/* EBP-21: Assert that this is a live message */
			}
		},
			Entry("with channel", helpers.ProcessCacheResponseThroughChannel),
			Entry("with callback", helpers.ProcessCacheResponseThroughCallback),
		)
		It("requests subsequent to non-wildcard live data are rejected as not supported", func() {
			firstCacheRequestID := message.CacheRequestID(1)
			numExpectedCachedMessages := 3
			cacheName := fmt.Sprintf("MaxMsgs%d/delay=5000", numExpectedCachedMessages)
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyLiveCancelsCached, cacheName, resource.TopicSubscriptionOf(topic), int32(7000), int32(0), int32(0))
			receivedMsgChan := make(chan message.InboundMessage, numExpectedCachedMessages)
			receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			channel, err := receiver.RequestCachedAsync(cacheRequestConfig, firstCacheRequestID)
			Expect(err).To(BeNil())
			Expect(channel).ToNot(BeNil())
			cacheName = fmt.Sprintf("MaxMsgs%d", numExpectedCachedMessages)

			/* NOTE: Subsequent LiveCancelsCached fails. */
			cacheRequestConfig = helpers.GetValidCacheRequestStrategyLiveCancelsCachedRequestConfig(cacheName, topic)
			secondCacheRequestID := message.CacheRequestID(2)
			secondChannel, err := receiver.RequestCachedAsync(cacheRequestConfig, secondCacheRequestID)
			Expect(err).To(BeAssignableToTypeOf(&solace.NativeError{}))
			helpers.ValidateNativeError(err, subcode.CacheAlreadyInProgress)
			Expect(secondChannel).To(BeNil())

			/* NOTE: Subsequent AsAvailable fails. */
			cacheRequestConfig = helpers.GetValidCacheRequestStrategyAsAvailableCacheRequestConfig(cacheName, topic)
			secondCacheRequestID = message.CacheRequestID(3)
			secondChannel, err = receiver.RequestCachedAsync(cacheRequestConfig, secondCacheRequestID)
			Expect(err).To(BeAssignableToTypeOf(&solace.NativeError{}))
			helpers.ValidateNativeError(err, subcode.CacheAlreadyInProgress)
			Expect(secondChannel).To(BeNil())

			/* NOTE: Subsequent CachedFirst fails. */
			cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedFirstCacheRequestConfig(cacheName, topic)
			secondCacheRequestID = message.CacheRequestID(4)
			secondChannel, err = receiver.RequestCachedAsync(cacheRequestConfig, secondCacheRequestID)
			Expect(err).To(BeAssignableToTypeOf(&solace.NativeError{}))
			helpers.ValidateNativeError(err, subcode.CacheAlreadyInProgress)
			Expect(secondChannel).To(BeNil())

			/* NOTE: Subsequent CachedOnly fails. */
			cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedOnlyCacheRequestConfig(cacheName, topic)
			secondCacheRequestID = message.CacheRequestID(5)
			secondChannel, err = receiver.RequestCachedAsync(cacheRequestConfig, secondCacheRequestID)
			Expect(err).To(BeAssignableToTypeOf(&solace.NativeError{}))
			helpers.ValidateNativeError(err, subcode.CacheAlreadyInProgress)
			Expect(secondChannel).To(BeNil())

			var cacheResponse solace.CacheResponse
			Eventually(channel, "10s").Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID matches cache response ID. */
			/* EBP-26: Assert CacheRequestOutcome Ok. */
			/* EBP-28: Assert err is nil. */

			for i := 0; i < numExpectedCachedMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeTrue())
				Expect(id).To(BeNumerically("==", firstCacheRequestID))
				/* EBP-21: Assert that this message is cached. */
			}
		})
		It("cache request requires messages from multiple clusters, but one cluster is shut down", func() {
			cacheRequestID := message.CacheRequestID(1)
			numExpectedMessages := 3
			cacheName := fmt.Sprintf("MaxMsgs%d/inc=badCacheCluster", numExpectedMessages)
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedMessages, testcontext.Cache().Vpn)
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyCachedFirst, cacheName, resource.TopicSubscriptionOf(topic), 10000, helpers.ValidMaxCachedMessages, helpers.ValidCachedMessageAge)
			receivedMsgChan := make(chan message.InboundMessage, 3)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())
			channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
			Expect(err).To(BeNil())
			Expect(channel).ToNot(BeNil())
			Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "10s").Should(BeNumerically("==", 1))
			var cacheResponse solace.CacheResponse
			Eventually(channel, "15s").Should(Receive(&cacheResponse))
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert response ID matched request ID. */
			/* EBP-26: Assert CacheRequestOutcome is Failed. */
			/* EBP-28: Assert err contains CACHE_TIMEOUT sc and CACHE_INCOMPLETE rc. */
			for i := 0; i < numExpectedMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeTrue())
				Expect(id).To(BeNumerically("==", cacheRequestID))
				/* EBP-21: Assert this is a cached message */
			}
		})
		DescribeTable("a cache request should retrieve at most the configured number of maxCachedMessages", func(configuredMaxMessages int32, expectedMessages int, strategy resource.CachedMessageSubscriptionStrategy) {
			/* NOTE: We make a chan twice the size of what we expect is necessary, so that if we do get additional
			 * messages they will immediately be available and not race with the channel read at the end of the
			 * test.
			 */
			if strategy == resource.CacheRequestStrategyCachedOnly {
				fmt.Printf("running CachedOnly test")
			}
			receivedMsgChan := make(chan message.InboundMessage, expectedMessages*2)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			cacheRequestID := message.CacheRequestID(1)
			cacheName := fmt.Sprintf("MaxMsgs%d", expectedMessages)
			cacheTopic := fmt.Sprintf("%s/%s/data1", cacheName, testcontext.Cache().Vpn)
			cacheReqeustConfig := resource.NewCachedMessageSubscriptionRequest(strategy, cacheName, resource.TopicSubscriptionOf(cacheTopic), helpers.ValidCacheAccessTimeout, configuredMaxMessages, helpers.ValidCachedMessageAge)
			cacheResponseChan, err := receiver.RequestCachedAsync(cacheReqeustConfig, cacheRequestID)
			Expect(err).To(BeNil())
			var response solace.CacheResponse
			Eventually(cacheResponseChan, "5s").Should(Receive(&response))
			Expect(response).ToNot(BeNil())
			/* EBP-25: Assert response ID matches request ID. */
			/* EBP-26: Assert CacheRequestOutcome.Ok */
			/* EBP-28: Assert err from response is nil */
			for i := 0; i < expectedMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan, "5s").Should(Receive(&msg))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(cacheTopic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeTrue())
				Expect(id).To(BeNumerically("==", cacheRequestID))
				/* EBP-21: Assert this is a cached message. */
			}
			/* NOTE: Asserts that the channel is empty, that we did not receive more cached messages than expected.
			 * We can assume that if we were going to receive more messages they would already be in the channel
			 * since we already received the cache response, and the cache response is not passed to the application
			 * before the data messages.
			 */
			Consistently(receivedMsgChan, "10ms").ShouldNot(Receive())
		},
			Entry("with maxMessages 1 and strategy AsAvailable", int32(1), 1, resource.CacheRequestStrategyAsAvailable),
			Entry("with maxMessages 3 and strategy AsAvailable", int32(3), 3, resource.CacheRequestStrategyAsAvailable),
			Entry("with maxMessages 10 and strategy AsAvailable", int32(10), 10, resource.CacheRequestStrategyAsAvailable),
			Entry("with maxMessages 0 and strategy AsAvailable", int32(0), 10, resource.CacheRequestStrategyAsAvailable),
			Entry("with maxMessages 1 and strategy CachedFirst", int32(1), 1, resource.CacheRequestStrategyCachedFirst),
			Entry("with maxMessages 3 and strategy CachedFirst", int32(3), 3, resource.CacheRequestStrategyCachedFirst),
			Entry("with maxMessages 10 and strategy CachedFirst", int32(10), 10, resource.CacheRequestStrategyCachedFirst),
			Entry("with maxMessages 0 and strategy CachedFirst", int32(0), 10, resource.CacheRequestStrategyCachedFirst),
			Entry("with maxMessages 1 and strategy CachedOnly", int32(1), 1, resource.CacheRequestStrategyCachedOnly),
			Entry("with maxMessages 3 and strategy CachedOnly", int32(3), 3, resource.CacheRequestStrategyCachedOnly),
			Entry("with maxMessages 10 and strategy CachedOnly", int32(10), 10, resource.CacheRequestStrategyCachedOnly),
			Entry("with maxMessages 0 and strategy CachedOnly", int32(0), 10, resource.CacheRequestStrategyCachedOnly),
			Entry("with maxMessages 1 and strategy LiveCancelsCached", int32(1), 1, resource.CacheRequestStrategyLiveCancelsCached),
			Entry("with maxMessages 3 and strategy LiveCancelsCached", int32(3), 3, resource.CacheRequestStrategyLiveCancelsCached),
			Entry("with maxMessages 10 and strategy LiveCancelsCached", int32(10), 10, resource.CacheRequestStrategyLiveCancelsCached),
			Entry("with maxMessages 0 and strategy LiveCancelsCached", int32(0), 10, resource.CacheRequestStrategyLiveCancelsCached),
		)
		DescribeTable("long running cache requests with live data queue and live data to fill", func(cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
			numExpectedCachedMessages := 3
			numExpectedLiveMessages := 100000
			delay := 10000
			numExpectedReceivedMessages := numExpectedCachedMessages + numExpectedLiveMessages
			receivedMsgChan := make(chan message.InboundMessage, numExpectedReceivedMessages)
			err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
				receivedMsgChan <- msg
			})
			Expect(err).To(BeNil())
			cacheName := fmt.Sprintf("MaxMsgs%d/delay=%d,msgs=%d", numExpectedCachedMessages, delay, numExpectedLiveMessages)
			topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
			cacheRequestID := message.CacheRequestID(1)
			cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyCachedFirst, cacheName, resource.TopicSubscriptionOf(topic), 45000, 0, 50000)
			var cacheResponse solace.CacheResponse
			/* NOTE: We need to wait for longer than usual for the cache response (10s) since the cache response is
			 * given to the application only after all messages related to the cache request have been received by
			 * the API. Since 100000 live messages are being received as a part of the cache response, the cache
			 * response ends up taking a lot longer.
			 */
			switch cacheResponseProcessStrategy {
			case helpers.ProcessCacheResponseThroughCallback:
				channel := make(chan solace.CacheResponse, 1)
				callback := func(cacheResponse solace.CacheResponse) {
					channel <- cacheResponse
				}
				err = receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, callback)
				Expect(err).To(BeNil())
				Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }).Should(BeNumerically("==", 1))
				Consistently(channel, "9.5s").ShouldNot(Receive())
				Eventually(channel, "10s").Should(Receive(&cacheResponse), GetCacheStatsAsString(messagingService))
			case helpers.ProcessCacheResponseThroughChannel:
				channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
				Expect(err).To(BeNil())
				Expect(channel).ToNot(BeNil())
				Consistently(channel, "9.5s").ShouldNot(Receive(&cacheResponse))
				Eventually(channel, "10s").Should(Receive(&cacheResponse), GetCacheStatsAsString(messagingService))
			default:
				Fail("Got unexpected cache response process strategy")
			}
			Expect(cacheResponse).ToNot(BeNil())
			/* EBP-25: Assert cache request ID from response is the same as the request */
			/* EBP-26: Assert cache request Outcome is Ok. */
			/* EBP-28: Assert error from cache response is nil */
			/* NOTE: Check the cached messages first. */
			for i := 0; i < numExpectedCachedMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg), fmt.Sprintf("Timed out waiting to receive message %d of %d", i, numExpectedLiveMessages))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeTrue())
				Expect(id).To(BeNumerically("==", cacheRequestID))
				/* EBP-21: Assert that this message is a cached message. */
			}
			/* NOTE: Check the live messages second. */
			for i := 0; i < numExpectedLiveMessages; i++ {
				var msg message.InboundMessage
				Eventually(receivedMsgChan).Should(Receive(&msg), fmt.Sprintf("Timed out waiting to receive message %d of %d", i, numExpectedLiveMessages))
				Expect(msg).ToNot(BeNil())
				Expect(msg.GetDestinationName()).To(Equal(topic))
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeFalse())
				Expect(id).To(BeNumerically("==", 0))
				/* EBP-21: Assert that this is a live message */
			}
		},
			Entry("with channel", helpers.ProcessCacheResponseThroughChannel),
			Entry("with callback", helpers.ProcessCacheResponseThroughCallback),
		)
		DescribeTable("wildcard request are rejected with error of not live data flow on live data queue",
			func(cacheRequestStrategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
				numExpectedCachedMessages := 3
				cacheRequestID := message.CacheRequestID(1)
				cacheName := fmt.Sprintf("MaxMsgs%d", numExpectedCachedMessages)
				topic := fmt.Sprintf("%s/%s/>", cacheName, testcontext.Cache().Vpn)
				var cacheRequestConfig resource.CachedMessageSubscriptionRequest
				switch cacheRequestStrategy {
				case resource.CacheRequestStrategyLiveCancelsCached:
					cacheRequestConfig = helpers.GetValidCacheRequestStrategyLiveCancelsCachedRequestConfig(cacheName, topic)
				case resource.CacheRequestStrategyCachedFirst:
					cacheRequestConfig = helpers.GetValidCacheRequestStrategyCachedFirstCacheRequestConfig(cacheName, topic)
				default:
					Fail("Got unexpected cacheRequestStrategy")
				}
				switch cacheResponseProcessStrategy {
				case helpers.ProcessCacheResponseThroughChannel:
					channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
					Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
					Expect(channel).To(BeNil())
				case helpers.ProcessCacheResponseThroughCallback:
					err := receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, func(solace.CacheResponse) {})
					Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
				default:
					Fail("Got unexpected cacheResponseProcessStrategy %d", cacheResponseProcessStrategy)
				}
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 0))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 0))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
			},
			Entry("with cache response strategy channel", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("with cache response strategy channel", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("with cache response strategy callback", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
			Entry("with cache response strategy callback", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughCallback),
		)
		DescribeTable("cache requests with wildcard topic with live data flowthrough",
			func(topic string, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
				topic = fmt.Sprintf(topic, testcontext.Cache().Vpn)
				cacheName := "MaxMsgs1"
				var numExpectedCachedMessages int
				if strings.Contains(topic, cacheName) {
					// Includes MaxMsgs1/*/data1(1), MaxMsgs1/*/data2(1)
					numExpectedCachedMessages = 2
				} else {
					// Includes MaxMsgs1/*/data1(1), MaxMsgs3/*/data1(3), MaxMsgs10/*/data1(10)
					numExpectedCachedMessages = 14
				}
				cacheRequestID := message.CacheRequestID(1)
				cacheRequestConfig := helpers.GetValidCacheRequestStrategyAsAvailableCacheRequestConfig(cacheName, topic)
				receivedMsgChan := make(chan message.InboundMessage, numExpectedCachedMessages*10)
				err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
					receivedMsgChan <- msg
				})
				var cacheResponse solace.CacheResponse
				switch cacheResponseProcessStrategy {
				case helpers.ProcessCacheResponseThroughChannel:
					channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
					Expect(err).To(BeNil())
					Expect(channel).ToNot(BeNil())
					Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "5s").Should(BeNumerically("==", 1))
					Eventually(channel, "5s").Should(Receive(&cacheResponse))
				case helpers.ProcessCacheResponseThroughCallback:
					channel := make(chan solace.CacheResponse, 1)
					callback := func(cacheResponse solace.CacheResponse) {
						channel <- cacheResponse
					}
					err = receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, callback)
					Expect(err).To(BeNil())
					Eventually(func() uint64 { return messagingService.Metrics().GetValue(metrics.CacheRequestsSent) }, "5s").Should(BeNumerically("==", 1))
					Eventually(channel, "5s").Should(Receive(&cacheResponse))
				default:
					Fail("Got unrecognized cacheRequestStrategy")
				}
				Expect(cacheResponse).ToNot(BeNil())
				/* EBP-25: Assert that the cache request from the response matches the request. */
				/* EBP-26: Assert that the CacheRequestOutcome is Ok. */
				/* EBP-28: Assert the err from the cache response is nil */
				for i := 0; i < numExpectedCachedMessages; i++ {
					var msg message.InboundMessage
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeTrue())
					Expect(id).To(BeNumerically("==", cacheRequestID))
					/* EBP-21: Assert that this message is a cached message. */
				}
			},
			Entry("wildcard topic 1 with channel", "MaxMsgs*/%s/data1", helpers.ProcessCacheResponseThroughChannel),
			Entry("wildcard topic 1 with callback", "MaxMsgs*/%s/data1", helpers.ProcessCacheResponseThroughCallback),
			Entry("wildcard topic 2 with channel", "MaxMsgs1/%s/*", helpers.ProcessCacheResponseThroughChannel),
			Entry("wildcard topic 2 with callback", "MaxMsgs1/%s/*", helpers.ProcessCacheResponseThroughCallback),
			Entry("wildcard topic 3 with channl", "MaxMsgs1/%s/>", helpers.ProcessCacheResponseThroughChannel),
			Entry("wildcard topic 3 with callback", "MaxMsgs1/%s/>", helpers.ProcessCacheResponseThroughCallback),
		)
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
				case resource.CacheRequestStrategyAsAvailable:
					strategyString = "CacheRequestStrategyAsAvailable"
					numExpectedReceivedMessages += numExpectedCachedMessages
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CacheRequestStrategyLiveCancelsCached:
					strategyString = "CacheRequestStrategyLiveCancelsCached"
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CacheRequestStrategyCachedFirst:
					strategyString = "CacheRequestStrategyCachedFirst"
					numExpectedReceivedMessages += numExpectedCachedMessages
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CacheRequestStrategyCachedOnly:
					strategyString = "CacheRequestStrategyCachedOnly"
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
					Eventually(receivedMsgChan, "10s").Should(Receive(), fmt.Sprintf("Timed out waiting to receive %d of %d messages", i, numExpectedReceivedMessages))
					totalMessagesReceived++
				}
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSent for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSucceeded for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0), fmt.Sprintf("CacheRequestsFailed for %s was wrong", strategyString))
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", numExpectedSentDirectMessages), fmt.Sprintf("DirectMessagesSent for %s was wrong", strategyString))
				Expect(totalMessagesReceived).To(BeNumerically("==", numExpectedReceivedMessages))
			},
			Entry("test cache RR for valid CacheRequestStrategyAsAvailable with channel", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CacheRequestStrategyAsAvailable with callback", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CacheRequestStrategyCachedFirst with channel", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CacheRequestStrategyCachedFirst with callback", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CacheRequestStrategyCachedOnly with channel", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CacheRequestStrategyCachedOnly with callback", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CacheRequestStrategyLiveCancelsCached with channel", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid LivCancelsCached  with callback", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
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
				case resource.CacheRequestStrategyAsAvailable:
					strategyString = "CacheRequestStrategyAsAvailable"
					numExpectedReceivedMessages += numExpectedCachedMessages
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CacheRequestStrategyLiveCancelsCached:
					strategyString = "CacheRequestStrategyLiveCancelsCached"
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CacheRequestStrategyCachedFirst:
					strategyString = "CacheRequestStrategyCachedFirst"
					numExpectedReceivedMessages += numExpectedCachedMessages
					numExpectedReceivedMessages += numExpectedLiveMessages
				case resource.CacheRequestStrategyCachedOnly:
					strategyString = "CacheRequestStrategyCachedOnly"
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
						Eventually(receivedMsgChan, "10s").Should(Receive(&msg), fmt.Sprintf("Timed out waiting for %d of %d messages", i, numExpectedCachedMessages))
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
				case resource.CacheRequestStrategyAsAvailable:
					waitForLiveMessage()
					Consistently(receivedMsgChan, "500ms").ShouldNot(Receive())
					waitForCacheResponses()
					waitForCachedMessages()
				case resource.CacheRequestStrategyLiveCancelsCached:
					waitForLiveMessage()
					waitForCacheResponses()
					/* NOTE: We only need to poll for 1ms, because if the API were going to give us cached
					 * messages, they would already be on the queue by the time we go to this assertion.
					 */
					Consistently(receivedMsgChan, "1ms").ShouldNot(Receive())
				case resource.CacheRequestStrategyCachedFirst:
					/* NOTE: we wait for 1500 ms since the delay is 2000 ms, and we want to allow a bit of room
					 * in the waiter so that we don't wait to long. Waiting past the delay would race with the
					 * reception of the cache response, coinciding with receivedMsgChan receiving
					 * cached data messages. This coincidence would cause the `Consistently` assertion to fail.
					 */
					Consistently(receivedMsgChan, "1500ms").ShouldNot(Receive())
					waitForCacheResponses()
					waitForCachedMessages()
					waitForLiveMessage()
				case resource.CacheRequestStrategyCachedOnly:
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
			Entry("test cache RR for valid CacheRequestStrategyAsAvailable with channel", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CacheRequestStrategyAsAvailable with callback", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CacheRequestStrategyCachedFirst with channel", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CacheRequestStrategyCachedFirst with callback", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CacheRequestStrategyCachedOnly with channel", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CacheRequestStrategyCachedOnly with callback", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughCallback),
			Entry("test cache RR for valid CacheRequestStrategyLiveCancelsCached with channel", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
			Entry("test cache RR for valid CacheRequestStrategyLiveCancelsCached  with callback", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
		)
		Describe("when the cache tests need a publisher", func() {
			var messagePublisher solace.DirectMessagePublisher
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
				messagePublisher, err = messagingService.CreateDirectMessagePublisherBuilder().Build()
				Expect(err).To(BeNil())
				err = messagePublisher.Start()
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
				if messagePublisher.IsRunning() {
					err = messagePublisher.Terminate(0)
					Expect(err).To(BeNil())
				}
				Expect(messagePublisher.IsRunning()).To(BeFalse())
				Expect(messagePublisher.IsTerminated()).To(BeTrue())
				if messagingService.IsConnected() {
					err = messagingService.Disconnect()
					Expect(err).To(BeNil())
				}
				Expect(messagingService.IsConnected()).To(BeFalse())
				if deferredOperation != nil {
					deferredOperation()
				}
			})
			It("a cache request requiring multiple responses from the cache instance results in only one cache response", func() {
				/* NOTE: AFAIK, the response from the cache instance is split every 1Mb, so initializing the
				 * cluster with messages of size 300k char should be a good number to both exceed the limit of what
				 * can be returned to the API from the instance in a single response, and provide an offset so that
				 * we are not always having data returned exactly on the boundary of 1Mb.
				 */
				numExpectedCacheMessages := 28
				payload := strings.Repeat("a", 300000)
				/* WARNING: If a topic subscription is added to one of the cache clusters that causes it to
				 * attract more messages, then this buffer may not be big enough, and receiver termination will
				 * hang.
				 */
				receivedMsgChan := make(chan message.InboundMessage, 30)
				err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
					receivedMsgChan <- msg
				})
				outboundMessage, err := messagingService.MessageBuilder().BuildWithStringPayload(payload)
				Expect(err).To(BeNil())
				Expect(outboundMessage).ToNot(BeNil())
				publishTopic := fmt.Sprintf("MaxMsgs10/%s/data1", testcontext.Cache().Vpn)
				err = receiver.AddSubscription(resource.TopicSubscriptionOf(publishTopic))
				Expect(err).To(BeNil())
				/* NOTE: Intiialize the cache with very large messages. This will be overwritten by the next test,
				 * so we don't need to worry about long messages causing other tests to take longer.
				 */
				numSentMessages := 10
				for i := 0; i < numSentMessages; i++ {
					messagePublisher.Publish(outboundMessage, resource.TopicOf(publishTopic))
				}
				for i := 0; i < numSentMessages; i++ {
					var msg message.InboundMessage
					Eventually(receivedMsgChan, "10s").Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					Expect(msg.GetDestinationName()).To(Equal(publishTopic))
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeFalse())
					Expect(id).To(BeNumerically("==", 0))
					/* EBP-21: Assert this is a live message */
				}
				err = receiver.RemoveSubscription(resource.TopicSubscriptionOf(publishTopic))
				Expect(err).To(BeNil())
				cacheRequestID := message.CacheRequestID(1)
				cacheName := "MaxMsgs10"
				cacheTopic := fmt.Sprintf("MaxMsgs*/%s/>", testcontext.Cache().Vpn)
				cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyAsAvailable, cacheName, resource.TopicSubscriptionOf(cacheTopic), 20000, 0, 0)
				Expect(err).To(BeNil())
				channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
				Expect(err).To(BeNil())
				Expect(channel).ToNot(BeNil())
				var cacheResponse solace.CacheResponse
				Eventually(channel, "30s").Should(Receive(&cacheResponse))
				Expect(cacheResponse).ToNot(BeNil())
				/* EBP-25: Assert cache request ID matches cache response ID. */
				/* EBP-26: Assert CacheRequestOutcome is Ok. */
				/* EBP-28: Assert err from response is nil. */
				var msg message.InboundMessage
				for i := 0; i < numExpectedCacheMessages; i++ {
					Eventually(receivedMsgChan, "5s").Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					/* NOTE: Can't assert topic from received message because of wildcard in cache request. */
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeTrue())
					Expect(id).To(BeNumerically("==", cacheRequestID))
					/* EBP-21: Assert this is a cached message. */
				}
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 1))
				/* NOTE: This metric is incremented by CCSMP, and appears to be incremented for every portion of the
				 * response that is received. Because the messages are very large, their parent response is split
				 * across 6 portions.
				 */
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 6))
				Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
			})
			It("live data that does not match an outstanding asynchronous cache request is delivered immediately", func() {
				directTopic := "nocache/charge-it"
				cacheRequestID := message.CacheRequestID(1)
				numConfiguredCachedMessages := 3
				numExpectedLiveMessages := 1
				numExpectedCachedMessages := numConfiguredCachedMessages
				numExpectedDirectMessages := 1
				numExpectedReceivedMessages := numExpectedLiveMessages + numExpectedDirectMessages + numExpectedCachedMessages
				delay := 2000
				cacheName := fmt.Sprintf("MaxMsgs%d/delay=%d,msgs=%d", numConfiguredCachedMessages, delay, numExpectedLiveMessages)
				cacheTopic := fmt.Sprintf("MaxMsgs%d/%s/data1", numConfiguredCachedMessages, testcontext.Cache().Vpn)
				cacheRequestConfig := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyCachedFirst, cacheName, resource.TopicSubscriptionOf(cacheTopic), int32(delay)*2, 10, 5000)
				outboundMessage, err := messagingService.MessageBuilder().BuildWithStringPayload("this is a direct message")
				Expect(err).To(BeNil())
				err = receiver.AddSubscription(resource.TopicSubscriptionOf(directTopic))
				receivedMsgChan := make(chan message.InboundMessage, numExpectedReceivedMessages)
				err = receiver.ReceiveAsync(func(msg message.InboundMessage) {
					receivedMsgChan <- msg
				})
				Expect(err).To(BeNil())
				channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
				Expect(err).To(BeNil())
				Expect(channel).ToNot(BeNil())
				err = messagePublisher.Publish(outboundMessage, resource.TopicOf(directTopic))
				Expect(err).To(BeNil())
				var msg message.InboundMessage
				/* EBP-21: Assert that this message is a live message */
				Eventually(receivedMsgChan).Should(Receive(&msg))
				Expect(msg).ToNot(BeNil())
				id, ok := msg.GetCacheRequestID()
				Expect(ok).To(BeFalse())
				Expect(id).To(BeNumerically("==", 0))
				Expect(msg.GetDestinationName()).To(Equal(directTopic))
				Consistently(receivedMsgChan, "500ms").ShouldNot(Receive())
				msg = nil
				var cacheResponse solace.CacheResponse
				Eventually(channel, "2s").Should(Receive(&cacheResponse))
				/* EBP-26: Assert that the cache response contains a CacheRequestOutcome.Ok */
				Expect(cacheResponse).ToNot(BeNil())
				for i := 0; i < numExpectedCachedMessages; i++ {
					/* EBP-25: Assert that the cache request ID from these received messages matches the cache request ID received in the cache response.*/
					/* EBP-21: Assert that these messages are cached messages.*/
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					Expect(msg.GetDestinationName()).To(Equal(cacheTopic))
					id, ok = msg.GetCacheRequestID()
					Expect(ok).To(BeTrue())
					Expect(id).To(BeNumerically("==", cacheRequestID))
					msg = nil
				}
				/* NOTE: We expect to get the live data message on the cache topic after the cached messges since we're
				 * using CacheRequestStrategyCachedFirst, but expect to get the live message on the direct topic before the cached messages
				 * because CacheRequestStrategyCachedFirst should not apply to messages not sent on the cacheTopic and the proxy delay
				 * should prevent the cache instance from receiving the cache request for long enough to receive the
				 * direct message.
				 */
				Eventually(receivedMsgChan).Should(Receive(&msg))
				Expect(msg).ToNot(BeNil())
				id, ok = msg.GetCacheRequestID()
				Expect(ok).To(BeFalse())
				Expect(id).To(BeNumerically("==", 0))
				Expect(msg.GetDestinationName()).To(Equal(cacheTopic))
				/* EBP-21: Assert that this message is a live message. */
			})
			DescribeTable("with no subscribe flag set the subscription is not sent before sending the cache request",
				func(cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
					numExpectedCachedMessages := 3
					cacheRequestID := message.CacheRequestID(1)
					cacheName := fmt.Sprintf("MaxMsgs%d", numExpectedCachedMessages)
					topic := fmt.Sprintf("%s/%s/data1", cacheName, testcontext.Cache().Vpn)
					cacheRequestConfig := helpers.GetValidCacheRequestStrategyCachedFirstCacheRequestConfig(cacheName, topic)
					receivedMsgChan := make(chan message.InboundMessage, numExpectedCachedMessages)
					err := receiver.ReceiveAsync(func(msg message.InboundMessage) {
						receivedMsgChan <- msg
					})
					Expect(err).To(BeNil())
					/* NOTE: Check that the subscription for the cache request does not exist before the request is sent */
					outboundMessage, err := messagingService.MessageBuilder().BuildWithStringPayload("string payload")
					Expect(err).To(BeNil())
					Expect(outboundMessage).ToNot(BeNil())
					err = messagePublisher.Publish(outboundMessage, resource.TopicOf(topic))
					Consistently(receivedMsgChan).ShouldNot(Receive())

					var cacheResponse solace.CacheResponse
					switch cacheResponseProcessStrategy {
					case helpers.ProcessCacheResponseThroughChannel:
						channel, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
						Expect(err).To(BeNil())
						Expect(channel).ToNot(BeNil())
						Eventually(channel).Should(Receive(&cacheResponse))
					case helpers.ProcessCacheResponseThroughCallback:
						channel := make(chan solace.CacheResponse, numExpectedCachedMessages)
						callback := func(cacheResponse solace.CacheResponse) { channel <- cacheResponse }
						err := receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, callback)
						Expect(err).To(BeNil())
						Eventually(channel).Should(Receive(&cacheResponse))
					default:
						Fail("Got unexpected cacheResponseStrategy")
					}
					Expect(cacheResponse).ToNot(BeNil())
					/* EBP-25: Assert cache request ID from response matches request. */
					/* EBP-26: Assert CacheRequestOutcome.Ok in response */
					/* EBP-28: Assert err from response is nil. */
					for i := 0; i < numExpectedCachedMessages; i++ {
						var msg message.InboundMessage
						Eventually(receivedMsgChan).Should(Receive(&msg))
						Expect(&msg).ToNot(BeNil())
						Expect(msg.GetDestinationName()).To(Equal(topic))
						id, ok := msg.GetCacheRequestID()
						Expect(ok).To(BeTrue())
						Expect(id).To(BeNumerically("==", cacheRequestID))
						/* EBP-21: Assert this is a cached message */
					}

					/* NOTE: Check that the subscription persists after the cache request has completed. */
					err = messagePublisher.Publish(outboundMessage, resource.TopicOf(topic))
					var msg message.InboundMessage
					Eventually(receivedMsgChan).Should(Receive(&msg))
					Expect(msg).ToNot(BeNil())
					id, ok := msg.GetCacheRequestID()
					Expect(ok).To(BeFalse())
					Expect(id).To(BeNumerically("==", 0))
					/* EBP-21: Assert this is a live message. */

					err = receiver.RemoveSubscription(resource.TopicSubscriptionOf(topic))
					Expect(err).To(BeNil())

					/* NOTE: Check that the subscription added as a part of the cache request can be removed. */
					err = messagePublisher.Publish(outboundMessage, resource.TopicOf(topic))
					Consistently(receivedMsgChan).ShouldNot(Receive())

					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0))
					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", 1))
					Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", 1))
				},
				Entry("with cache response process strategy channel", helpers.ProcessCacheResponseThroughChannel),
				Entry("with cache response process strategy callback", helpers.ProcessCacheResponseThroughCallback),
			)
		})
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
				const strategy resource.CachedMessageSubscriptionStrategy = resource.CacheRequestStrategyAsAvailable
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
					const strategy resource.CachedMessageSubscriptionStrategy = resource.CacheRequestStrategyAsAvailable
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
							case resource.CacheRequestStrategyAsAvailable:
								strategyString = "CacheRequestStrategyAsAvailable"
								numExpectedReceivedMessages += numExpectedCachedMessages
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CacheRequestStrategyLiveCancelsCached:
								strategyString = "CacheRequestStrategyLiveCancelsCached"
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CacheRequestStrategyCachedFirst:
								strategyString = "CacheRequestStrategyCachedFirst"
								numExpectedReceivedMessages += numExpectedCachedMessages
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CacheRequestStrategyCachedOnly:
								strategyString = "CacheRequestStrategyCachedOnly"
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
						Entry("test cache RR for valid CacheRequestStrategyAsAvailable with channel", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughChannel),
						Entry("test cache RR for valid CacheRequestStrategyAsAvailable with callback", resource.CacheRequestStrategyAsAvailable, helpers.ProcessCacheResponseThroughCallback),
						Entry("test cache RR for valid CacheRequestStrategyCachedFirst with channel", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughChannel),
						Entry("test cache RR for valid CacheRequestStrategyCachedFirst with callback", resource.CacheRequestStrategyCachedFirst, helpers.ProcessCacheResponseThroughCallback),
						Entry("test cache RR for valid CacheRequestStrategyCachedOnly with channel", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughChannel),
						Entry("test cache RR for valid CacheRequestStrategyCachedOnly with callback", resource.CacheRequestStrategyCachedOnly, helpers.ProcessCacheResponseThroughCallback),
						Entry("test cache RR for valid CacheRequestStrategyLiveCancelsCached with channel", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
						Entry("test cache RR for valid LivCancelsCached  with callback", resource.CacheRequestStrategyLiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
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
							case resource.CacheRequestStrategyAsAvailable:
								strategyString = "CacheRequestStrategyAsAvailable"
								numExpectedReceivedMessages += numExpectedCachedMessages
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CacheRequestStrategyLiveCancelsCached:
								strategyString = "CacheRequestStrategyLiveCancelsCached"
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CacheRequestStrategyCachedFirst:
								strategyString = "CacheRequestStrategyCachedFirst"
								numExpectedReceivedMessages += numExpectedCachedMessages
								numExpectedReceivedMessages += numExpectedLiveMessages
							case resource.CacheRequestStrategyCachedOnly:
								strategyString = "CacheRequestStrategyCachedOnly"
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
						Entry("test cache RR for valid CacheRequestStrategyAsAvailable with callback", resource.CacheRequestStrategyAsAvailable),
						Entry("test cache RR for valid CacheRequestStrategyCachedFirst with callback", resource.CacheRequestStrategyCachedFirst),
						Entry("test cache RR for valid CacheRequestStrategyCachedOnly with callback", resource.CacheRequestStrategyCachedOnly),
						Entry("test cache RR for valid LivCancelsCached  with callback", resource.CacheRequestStrategyLiveCancelsCached),
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
