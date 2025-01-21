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

package helpers

import (
	"fmt"
	"strconv"
	"strings"

	. "github.com/onsi/gomega"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/test/testcontext"
)

const (
	ValidCachedMessageAge   int32 = 5
	ValidMaxCachedMessages  int32 = 5
	ValidCacheAccessTimeout int32 = 5000
)

const (
	InvalidCacheAccessTimoeout int32 = 1000
)

func DefaultCacheConfiguration() config.ServicePropertyMap {
	connectionDetails := testcontext.Messaging()
	url := fmt.Sprintf("%s:%d", connectionDetails.Host, connectionDetails.MessagingPorts.PlaintextPort)
	config := config.ServicePropertyMap{
		config.ServicePropertyVPNName:                     testcontext.Cache().Vpn,
		config.TransportLayerPropertyHost:                 url,
		config.AuthenticationPropertySchemeBasicUserName:  connectionDetails.Authentication.BasicUsername,
		config.AuthenticationPropertySchemeBasicPassword:  connectionDetails.Authentication.BasicPassword,
		config.TransportLayerPropertyReconnectionAttempts: 0,
	}
	return config
}

func SendMsgsToTopic(topic string, numMessages int) {
	builder := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(DefaultCacheConfiguration())
	messagingService := buildMessagingService(builder, 2)
	defer func() {
		err := messagingService.Disconnect()
		Expect(err).To(BeNil())
	}()
	err := messagingService.Connect()
	Expect(err).To(BeNil())
	receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(resource.TopicSubscriptionOf(topic)).Build()
	Expect(err).To(BeNil())
	defer func() {
		err := receiver.Terminate(0)
		Expect(err).To(BeNil())
	}()
	err = receiver.Start()
	Expect(err).To(BeNil())
	publisher, err := messagingService.CreateDirectMessagePublisherBuilder().OnBackPressureReject(0).Build()
	Expect(err).To(BeNil())
	defer func() {
		err := publisher.Terminate(0)
		Expect(err).To(BeNil())
	}()
	err = publisher.Start()
	Expect(err).To(BeNil())
	receivedMsgs := make(chan message.InboundMessage, numMessages)
	cacheMessageHandlerCallback := func(msg message.InboundMessage) {
		receivedMsgs <- msg
	}
	err = receiver.ReceiveAsync(cacheMessageHandlerCallback)
	Expect(err).To(BeNil())
	counter := 0
	for counter < numMessages {
		msg, err := messagingService.MessageBuilder().BuildWithStringPayload(fmt.Sprintf("message %d", counter))
		Expect(err).To(BeNil())
		err = publisher.Publish(msg, resource.TopicOf(topic))
		Expect(err).To(BeNil())
		counter++
	}
	for i := 0; i < numMessages; i++ {
		var receivedMessage message.InboundMessage
		Eventually(receivedMsgs, "5000ms").Should(Receive(&receivedMessage))
		Expect(receivedMessage.GetDestinationName()).To(Equal(topic))
	}
}

// InitCacheWithPreExistingMessages assumes that `clusterName` is the name of a valid cache cluster.
func InitCacheWithPreExistingMessages(cacheCluster testcontext.CacheClusterConfig) {
	topics := []string{}
	const defaultNumMessages int = 1
	const standardClusterNamePrefix string = "MaxMsgs"
	vpnName := testcontext.Cache().Vpn
	numMessages := defaultNumMessages
	clusterName := cacheCluster.Name
	for _, topic := range cacheCluster.Topics {
		if strings.HasPrefix(topic, fmt.Sprintf("%s/*/data", clusterName)) {
			/* NOTE: Checking the length is greater than the prefix means we can
			 * split the string immediately instead of needing to check that the
			 * slice length is 2. */
			if strings.HasPrefix(clusterName, standardClusterNamePrefix) && (len(clusterName) != len(standardClusterNamePrefix)) {
				if convertedNum, err := strconv.Atoi(strings.Split(clusterName, standardClusterNamePrefix)[1]); err == nil {
					numMessages = convertedNum
				}
			}
			splitString := strings.Split(topic, "*")
			/* NOTE: This should never happen, but we have this check just in case
			 * something goes wrong so we can avoid a panic if we try to go outside
			 * the list size in the next line. */
			Expect(len(splitString)).To(BeNumerically("==", 2))
			topics = append(topics, fmt.Sprintf("%s%s%s", splitString[0], vpnName, splitString[1]))
		}
	}
	for _, topic := range topics {
		SendMsgsToTopic(topic, numMessages)
	}
}

func InitAllCacheClustersWithMessages() {
	for _, distributedCache := range testcontext.Cache().DistributedCaches {
		for _, cacheCluster := range distributedCache.CacheClusters {
			InitCacheWithPreExistingMessages(cacheCluster)
		}
	}
}

func GetValidAsAvailableCacheRequestConfig(cacheName string, topic string) resource.CachedMessageSubscriptionRequest {
	return GetValidCacheRequestConfig(resource.AsAvailable, cacheName, topic)
}

func GetValidCachedOnlyCacheRequestConfig(cacheName string, topic string) resource.CachedMessageSubscriptionRequest {
	return GetValidCacheRequestConfig(resource.CachedOnly, cacheName, topic)
}

func GetValidLiveCancelsCachedRequestConfig(cacheName string, topic string) resource.CachedMessageSubscriptionRequest {
	return GetValidCacheRequestConfig(resource.LiveCancelsCached, cacheName, topic)
}

func GetValidCachedFirstCacheRequestConfig(cacheName string, topic string) resource.CachedMessageSubscriptionRequest {
	return GetValidCacheRequestConfig(resource.CachedFirst, cacheName, topic)
}

func GetValidCacheRequestConfig(strategy resource.CachedMessageSubscriptionStrategy, cacheName string, topic string) resource.CachedMessageSubscriptionRequest {
	return resource.NewCachedMessageSubscriptionRequest(strategy, cacheName, resource.TopicSubscriptionOf(topic), ValidCacheAccessTimeout, ValidMaxCachedMessages, ValidCachedMessageAge)
}

func GetInvalidCacheRequestConfig(strategy resource.CachedMessageSubscriptionStrategy, cacheName string, topic string) resource.CachedMessageSubscriptionRequest {
	return resource.NewCachedMessageSubscriptionRequest(strategy, cacheName, resource.TopicSubscriptionOf(topic), InvalidCacheAccessTimoeout, ValidMaxCachedMessages, ValidCachedMessageAge)
}

type CacheResponseProcessStrategy = int

const (
	ProcessCacheResponseThroughChannel  CacheResponseProcessStrategy = iota
	ProcessCacheResponseThroughCallback CacheResponseProcessStrategy = iota
)

func CacheToxicConfiguration() config.ServicePropertyMap {
	if toxiConfig := ToxicConfiguration(); toxiConfig == nil {
		return nil
	} else {
		toxiConfig[config.ServicePropertyVPNName] = testcontext.Cache().Vpn
		return toxiConfig
	}
}
