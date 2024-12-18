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
	//"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/logging"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	//"solace.dev/go/messaging/pkg/solace/subcode"
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
        var receiver solace.DirectMessageReceiver
        Describe("When the cache is available and configured", func() {
                BeforeEach(func() {
                        logging.SetLogLevel(logging.LogLevelDebug)
                        CheckCache() // skips test with message if cache image is not available
                        helpers.InitAllCacheClustersWithMessages()
                        var err error
                        messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultCacheConfiguration()).Build()
                        Expect(err).To(BeNil())
                })
                AfterEach(func () {
                        var err error
                        if messagingService.IsConnected() {
                                err = messagingService.Disconnect()
                                Expect(err).To(BeNil())
                        }
                        Expect(messagingService.IsConnected()).To(BeFalse())
                        if receiver.IsRunning() {
                                err = receiver.Terminate(0)
                                Expect(err).To(BeNil())
                        }
                        Expect(receiver.IsRunning()).To(BeFalse())
                        Expect(receiver.IsTerminated()).To(BeTrue())
                })
                DescribeTable("a direct receiver should be able to submit a valid cache request, receive a response, and terminate",
                /* name test_async_cache_req_with_live_data. */
                        func(strategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
                                logging.SetLogLevel(logging.LogLevelDebug)
                                strategyString := ""
                                switch strategy {
                                case resource.AsAvailable:
                                        strategyString = "AsAvailable"
                                case resource.CachedOnly:
                                        strategyString = "CachedOnly"
                                case resource.CachedFirst:
                                        strategyString = "CachedFirst"
                                case resource.LiveCancelsCached:
                                        strategyString = "LiveCancelsCached"
                                }
                                //time.Sleep(time.Second * 100)
                                numExpectedCachedMessages := 3
                                numExpectedLiveMessages := 1
                                numSentCacheRequests := 1
                                numExpectedCacheResponses := numSentCacheRequests
                                numExpectedSentMessages := 0
                                totalMessagesReceived := 0
                                numExpectedReceivedMessages := numExpectedSentMessages
                                switch strategy {
                                case resource.AsAvailable:
                                        numExpectedReceivedMessages += numExpectedCachedMessages
                                        numExpectedReceivedMessages += numExpectedLiveMessages
                                case resource.LiveCancelsCached:
                                        numExpectedReceivedMessages += numExpectedLiveMessages
                                case resource.CachedFirst:
                                        numExpectedReceivedMessages += numExpectedCachedMessages
                                        numExpectedReceivedMessages += numExpectedLiveMessages
                                case resource.CachedOnly:
                                        numExpectedReceivedMessages += numExpectedCachedMessages
                                }
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
                                topic := fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
                                cacheName := fmt.Sprintf("MaxMsgs%d/delay=2000,msgs=%d", numExpectedCachedMessages, numExpectedLiveMessages)
                                cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, topic)
                                cacheRequestID := message.CacheRequestID(1)
                                receivedMsgChan := make(chan message.InboundMessage, 3)
                                defer close(receivedMsgChan)
                                receiver.ReceiveAsync(func(msg message.InboundMessage) {
                                        fmt.Printf("Received message from test:\n%s\n", msg.String())
                                        receivedMsgChan <- msg
                                })
                                switch cacheResponseProcessStrategy {
                                case helpers.ProcessCacheResponseThroughChannel:
                                        cacheResponseChan, err := receiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
                                        Expect(err).To(BeNil())
                                        for i := 0; i < numExpectedCacheResponses; i++ {
                                                Eventually(cacheResponseChan, "10s").Should(Receive())
                                                fmt.Printf("Got response %d for channel\n", i)
                                        }
                                case helpers.ProcessCacheResponseThroughCallback:
                                        cacheResponseSignalChan := make(chan solace.CacheResponse, 1)
                                        defer close(cacheResponseSignalChan)
                                        cacheResponseCallback := func (cacheResponse solace.CacheResponse) {
                                                cacheResponseSignalChan <- cacheResponse
                                        }
                                        err = receiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
                                        Expect(err).To(BeNil())
                                        for i := 0; i < numExpectedCacheResponses; i++ {
                                                Eventually(cacheResponseSignalChan, "10s").Should(Receive())
                                                fmt.Printf("Got response %d for callback\n", i)
                                        }
                                default:
                                        Fail(fmt.Sprintf("Got unexpected CacheResponseProcessStrategy %d", cacheResponseProcessStrategy))
                                }
                                for i := 0; i < numExpectedReceivedMessages; i ++ {
                                        Eventually(receivedMsgChan, "10s").Should(Receive())
                                        fmt.Printf("Found message\n")
                                        totalMessagesReceived ++
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
        Describe("Lifecycle tests", func() {
                var messagingService solace.MessagingService
                var messageReceiver solace.DirectMessageReceiver
                type terminationContext struct {
                        terminateFunction func(messagingService solace.MessagingService,
                                                messageReceiver solace.DirectMessageReceiver)
                        configuration func() config.ServicePropertyMap
                        cleanupFunc func(messagingService solace.MessagingService)

                }
                terminationCases := map[string]terminationContext {
                        /*
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
                        },
                        "messaging service disconnect async with callback": {
                                 terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        errorChan := make(chan error)
                                        messagingService.DisconnectAsyncWithCallback(func(err error) {
                                                errorChan <- err
                                        })
                                        select {
                                        case err := <-errorChan:
                                                Expect(err).To(BeNil())
                                        case <- time.After(time.Second * 5):
                                                Fail("Timed out waiting for error chan from call to DisconnectAsyncWithCallback")
                                        }
                                        Expect(messagingService.IsConnected()).To(BeFalse())
                                        Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
                                },
                                configuration: func() config.ServicePropertyMap {
                                        return helpers.DefaultCacheConfiguration()
                                },
                        },
                        "management disconnect": {
                                terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        eventChan := make(chan solace.ServiceEvent)
                                        messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
                                                eventChan <- event
                                        })
                                        helpers.ForceDisconnectViaSEMPv2(messagingService)
                                        select {
                                        case event := <- eventChan:
                                                helpers.ValidateNativeError(event.GetCause(), subcode.CommunicationError)
                                        case <- time.After(time.Second * 10):
                                                Fail("Timed out waiting for management disconnect")
                                        }
                                        Expect(messagingService.IsConnected()).To(BeFalse())
                                        Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
                                },
                                configuration: func() config.ServicePropertyMap {
                                        return helpers.DefaultCacheConfiguration()
                                },
                        },
                        "toxic disconnect": {
                                terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        eventChan := make(chan solace.ServiceEvent)
                                        messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
                                                eventChan <- event
                                        })
                                        testcontext.Toxi().SMF().Disable()
                                        select {
                                        case event := <- eventChan:
                                                helpers.ValidateNativeError(event.GetCause(), subcode.CommunicationError)
                                        case <- time.After(time.Second * 10):
                                                Fail("Timed out waiting for toxic disconnect")
                                        }
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
                        },
                        */
                        "receiver terminate": {
                                terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        Expect(messageReceiver.Terminate(0)).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeTrue())
                                },
                                configuration: func() config.ServicePropertyMap {
                                        return helpers.DefaultCacheConfiguration()
                                },
                                cleanupFunc: func(messagingService solace.MessagingService) {
                                        Expect(messagingService.Disconnect()).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeFalse())
                                },
                        },
                        /*
                        "receiver terminate with grace period": {
                                terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        gracePeriod := time.Second * 5
                                        Eventually(messageReceiver.Terminate(gracePeriod), gracePeriod + 1).Should(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeTrue())
                                },
                                configuration: func() config.ServicePropertyMap {
                                        return helpers.DefaultCacheConfiguration()
                                },
                                cleanupFunc: func(messagingService solace.MessagingService) {
                                        Expect(messagingService.Disconnect()).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeFalse())
                                },
                        },
                        "receiver terminate async with channel": {
                                terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        gracePeriod := time.Second * 0
                                        var err error
                                        Eventually(messageReceiver.TerminateAsync(gracePeriod), gracePeriod + 1).Should(Receive(&err))
                                        Expect(err).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeTrue())
                                },
                                configuration: func() config.ServicePropertyMap {
                                        return helpers.DefaultCacheConfiguration()
                                },
                                cleanupFunc: func(messagingService solace.MessagingService) {
                                        Expect(messagingService.Disconnect()).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeFalse())
                                },
                        },
                        "receiver terminate async with grace period with channel": {
                                terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        gracePeriod := time.Second * 5
                                        var err error
                                        Eventually(messageReceiver.TerminateAsync(gracePeriod), gracePeriod + 1).Should(Receive(&err))
                                        Expect(err).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeTrue())
                                },
                                configuration: func() config.ServicePropertyMap {
                                        return helpers.DefaultCacheConfiguration()
                                },
                                cleanupFunc: func(messagingService solace.MessagingService) {
                                        Expect(messagingService.Disconnect()).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeFalse())
                                },
                        },
                        "receiver terminate async with callback": {
                                terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        gracePeriod := time.Second * 0
                                        errChan := make(chan error)
                                        var err error
                                        messageReceiver.TerminateAsyncCallback(gracePeriod, func (err error) {
                                                errChan <- err
                                        })
                                        Eventually(errChan, gracePeriod + 1).Should(Receive(&err))
                                        Expect(err).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeTrue())
                                },
                                configuration: func() config.ServicePropertyMap {
                                        return helpers.DefaultCacheConfiguration()
                                },
                                cleanupFunc: func(messagingService solace.MessagingService) {
                                        Expect(messagingService.Disconnect()).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeFalse())
                                },
                        },
                        "receiver terminate async with grace period with callback": {
                                terminateFunction: func(messagingService solace.MessagingService,
                                                        messageReceiver solace.DirectMessageReceiver) {
                                        gracePeriod := time.Second * 5
                                        errChan := make(chan error)
                                        var err error
                                        messageReceiver.TerminateAsyncCallback(gracePeriod, func (err error) {
                                                errChan <- err
                                        })
                                        Eventually(errChan, gracePeriod + 1).Should(Receive(&err))
                                        Expect(err).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeTrue())
                                },
                                configuration: func() config.ServicePropertyMap {
                                        return helpers.DefaultCacheConfiguration()
                                },
                                cleanupFunc: func(messagingService solace.MessagingService) {
                                        Expect(messagingService.Disconnect()).To(BeNil())
                                        Expect(messagingService.IsConnected()).To(BeFalse())
                                },
                        },
                        */
                }
                cacheRequestID := 0
                for terminationCaseName, terminationContextRef := range terminationCases {
                        terminationConfiguration := terminationContextRef.configuration
                        terminationFunction := terminationContextRef.terminateFunction
                        terminationCleanup := terminationContextRef.cleanupFunc
                        Context("using termination scheme " + terminationCaseName, func() {
                                var terminate func()
                                const numExpectedCachedMessages int = 3
                                const numExpectedLiveMessages int = 1
                                const delay int = 25000
                                var receivedMsgChan chan message.InboundMessage

                                var cacheName string
                                var topic string
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
                                        err = messageReceiver.Start()
                                        Expect(err).To(BeNil())
                                        receivedMsgChan = make(chan message.InboundMessage, 3)
                                        defer close(receivedMsgChan)
                                        messageReceiver.ReceiveAsync(func(msg message.InboundMessage) {
                                                fmt.Printf("Received message from test:\n%s\n", msg.String())
                                                receivedMsgChan <- msg
                                        })

                                        topic = fmt.Sprintf("MaxMsgs%d/%s/data1", numExpectedCachedMessages, testcontext.Cache().Vpn)
                                        cacheName = fmt.Sprintf("MaxMsgs%d/delay=%d,msgs=%d", numExpectedCachedMessages, delay, numExpectedLiveMessages)
                                        terminate = func() {
                                                Expect(messagingService.IsConnected()).To(BeTrue())
                                                Expect(messageReceiver.IsRunning()).To(BeTrue())
                                                terminationFunction(messagingService, messageReceiver)
                                                Eventually(messageReceiver.IsTerminated(), "5s").Should(BeTrue())
                                        }
                                })
                                AfterEach(func () {
                                        if messageReceiver.IsRunning() {
                                                messageReceiver.Terminate(0)
                                        }
                                        if messagingService.IsConnected() {
                                                messagingService.Disconnect()
                                        }
                                        if terminationCleanup != nil {
                                                terminationCleanup(messagingService)
                                        }
                                })

                                DescribeTable("A receiver should be able to terminate gracefully with inflight cache requests",
                                        func(strategy resource.CachedMessageSubscriptionStrategy, cacheResponseProcessStrategy helpers.CacheResponseProcessStrategy) {
                                                fmt.Printf("Got to beginning of DescribeTable iteration\n")
                                                fmt.Printf("strategy is %d and response strategy is %d\n", strategy, cacheResponseProcessStrategy)
                                                logging.SetLogLevel(logging.LogLevelDebug)
                                                strategyString := ""
                                                switch strategy {
                                                case resource.AsAvailable:
                                                        strategyString = "AsAvailable"
                                                case resource.CachedOnly:
                                                        strategyString = "CachedOnly"
                                                case resource.CachedFirst:
                                                        strategyString = "CachedFirst"
                                                case resource.LiveCancelsCached:
                                                        strategyString = "LiveCancelsCached"
                                                }
                                                numSentCacheRequests := 1
                                                numExpectedCacheResponses := numSentCacheRequests
                                                numExpectedSentMessages := 0
                                                totalMessagesReceived := 0
                                                numExpectedReceivedMessages := numExpectedSentMessages
                                                switch strategy {
                                                case resource.AsAvailable:
                                                        numExpectedReceivedMessages += numExpectedCachedMessages
                                                        numExpectedReceivedMessages += numExpectedLiveMessages
                                                case resource.LiveCancelsCached:
                                                        numExpectedReceivedMessages += numExpectedLiveMessages
                                                case resource.CachedFirst:
                                                        numExpectedReceivedMessages += numExpectedCachedMessages
                                                        numExpectedReceivedMessages += numExpectedLiveMessages
                                                case resource.CachedOnly:
                                                        numExpectedReceivedMessages += numExpectedCachedMessages
                                                }
                                                numExpectedSentDirectMessages := numSentCacheRequests + numExpectedSentMessages

                                                cacheRequestConfig := helpers.GetValidCacheRequestConfig(strategy, cacheName, topic)
                                                cacheRequestID := message.CacheRequestID(cacheRequestID)
                                                fmt.Printf("Finished all setup, choosing cache request strat\n")
                                                switch cacheResponseProcessStrategy {
                                                case helpers.ProcessCacheResponseThroughChannel:
                                                        fmt.Printf("About to send cache request")
                                                        cacheResponseChan, err := messageReceiver.RequestCachedAsync(cacheRequestConfig, cacheRequestID)
                                                        Expect(err).To(BeNil())
                                                        fmt.Printf("Sent cache request!")
                                                        terminate()
                                                        for i := 0; i < numExpectedCacheResponses; i++ {
                                                                Eventually(cacheResponseChan, delay * 2).Should(Receive())
                                                                fmt.Printf("Got response %d for channel\n", i)
                                                        }
                                                case helpers.ProcessCacheResponseThroughCallback:
                                                        fmt.Printf("About to send cache request")
                                                        cacheResponseSignalChan := make(chan solace.CacheResponse, numExpectedCacheResponses)
                                                        defer close(cacheResponseSignalChan)
                                                        cacheResponseCallback := func (cacheResponse solace.CacheResponse) {
                                                                cacheResponseSignalChan <- cacheResponse
                                                        }
                                                        err := messageReceiver.RequestCachedAsyncWithCallback(cacheRequestConfig, cacheRequestID, cacheResponseCallback)
                                                        Expect(err).To(BeNil())
                                                        fmt.Printf("Sent cache request!")
                                                        terminate()
                                                        for i := 0; i < numExpectedCacheResponses; i++ {
                                                                Eventually(cacheResponseSignalChan, delay * 2).Should(Receive())
                                                                fmt.Printf("Got response %d for callback\n", i)
                                                        }
                                                default:
                                                        Fail(fmt.Sprintf("Got unexpected CacheResponseProcessStrategy %d", cacheResponseProcessStrategy))
                                                }
                                                /* TODO: Should this even be here? */
                                                for i := 0; i < numExpectedReceivedMessages; i ++ {
                                                        Eventually(receivedMsgChan, "10s").Should(Receive())
                                                        fmt.Printf("Found message\n")
                                                        totalMessagesReceived ++
                                                }
                                                Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSent)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSent for %s was wrong", strategyString))
                                                Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsSucceeded)).To(BeNumerically("==", numSentCacheRequests), fmt.Sprintf("CacheRequestsSucceeded for %s was wrong", strategyString))
                                                Expect(messagingService.Metrics().GetValue(metrics.CacheRequestsFailed)).To(BeNumerically("==", 0), fmt.Sprintf("CacheRequestsFailed for %s was wrong", strategyString))
                                                Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", numExpectedSentDirectMessages), fmt.Sprintf("DirectMessagesSent for %s was wrong", strategyString))
                                                Expect(totalMessagesReceived).To(BeNumerically("==", numExpectedReceivedMessages))
                                               
                                        },
                                        Entry("test cache RR for valid AsAvailable with channel", resource.AsAvailable, helpers.ProcessCacheResponseThroughChannel),
                                        /*
                                        Entry("test cache RR for valid AsAvailable with callback", resource.AsAvailable, helpers.ProcessCacheResponseThroughCallback),
                                        Entry("test cache RR for valid CachedFirst with channel", resource.CachedFirst, helpers.ProcessCacheResponseThroughChannel),
                                        Entry("test cache RR for valid CachedFirst with callback", resource.CachedFirst, helpers.ProcessCacheResponseThroughCallback),
                                        Entry("test cache RR for valid CachedOnly with channel", resource.CachedOnly, helpers.ProcessCacheResponseThroughChannel),
                                        Entry("test cache RR for valid CachedOnly with callback", resource.CachedOnly, helpers.ProcessCacheResponseThroughCallback),
                                        Entry("test cache RR for valid LiveCancelsCached with channel", resource.LiveCancelsCached, helpers.ProcessCacheResponseThroughChannel),
                                        Entry("test cache RR for valid LivCancelsCached  with callback", resource.LiveCancelsCached, helpers.ProcessCacheResponseThroughCallback),
                                        */
                                )
                        })
                        cacheRequestID++
                }
        })
})
})
