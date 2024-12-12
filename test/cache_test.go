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

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/logging"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
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
        var messagingService solace.MessagingService
        Describe("When the cache is available and configured", func() {
                BeforeEach(func() {
                        CheckCache() // skips test with message if cache image is not available
                        helpers.InitAllCacheClustersWithMessages()
                        var err error
                        messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultCacheConfiguration()).Build()
                        Expect(err).To(BeNil())
                })
                AfterEach(func () {
                        err := messagingService.Disconnect()
                        Expect(err).To(BeNil())
                })
                DescribeTable("a direct receiver should be able to submit a valid cache request, receive a response, and terminate",
                /* name test_async_cache_req_with_live_data. */
                        func(strategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
                                numSentCacheRequests := 1
                                numExpectedCacheResponses := numSentCacheRequests
                                numExpectedSentMessages := 0
                                numExpectedReceivedMessages := numExpectedSentMessages
                                numExpectedReceivedDirectMessages := numExpectedCacheResponses + numExpectedReceivedMessages
                                numExpectedSentDirectMessages := numSentCacheRequests + numExpectedSentMessages
                                err := messagingService.Connect()
                                defer func() {
                                        messagingService.Disconnect()
                                }()
                                Expect(err).To(BeNil())
                                receiver, err := messagingService.CreateDirectMessageReceiverBuilder().Build()
                                defer func() {
                                        receiver.Terminate(0)
                                }()
                                Expect(err).To(BeNil())
                                err = receiver.Start()
                                Expect(err).To(BeNil())
                                topic := fmt.Sprintf("MaxMsgs3/%s/data1", testcontext.Cache().Vpn)
                                cacheName := "MaxMsgs3/delay=2000,msgs=1"
                                cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, topic)
                                cacheRequestID := message.CacheRequestID(1)
                                switch cacheResponseProcessStrategy{
                                case helpers.ProcessCacheResponseThroughChannel:
                                        cacheResponseChan, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
                                        Expect(err).To(BeNil())
                                        Eventually(cacheResponseChan, "10s").Should(Receive())
                                        fmt.Printf("Got response for channel\n")
                                case helpers.ProcessCacheResponseThroughCallback:
                                        cacheResponseSignalChan := make(chan solace.CacheResponse, 1)
                                        cacheResponseCallback := func (cacheResponse solace.CacheResponse) {
                                                cacheResponseSignalChan <- cacheResponse
                                        }
                                        err = receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
                                        Expect(err).To(BeNil())
                                        Eventually(cacheResponseSignalChan, "10s").Should(Receive())
                                        fmt.Printf("Got response for callback\n")
                                default:
                                        Fail(fmt.Sprintf("Got unexpected CacheResponseProcessStrategy %d", cacheResponseProcessStrategy))
                                }
                                Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", numExpectedSentDirectMessages))
                                Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(BeNumerically("==", numExpectedReceivedDirectMessages))
                                Expect(messagingService.Metrics().GetValue(metrics.ReceivedMessagesBackpressureDiscarded)).To(BeNumerically("==", numExpectedReceivedMessages))
                                Expect(messagingService.Metrics().GetValue(metrics.ReceivedMessagesTerminationDiscarded)).To(BeNumerically("==", numExpectedReceivedMessages))
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
})
