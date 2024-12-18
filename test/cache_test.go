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
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/message"
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
	/* The following tests are just demos of how to check for infrastructure.
	   They should be removed once proper integration tests are added. */
	Describe("Demo isolated Cache functionality", func() {
		BeforeEach(func() {
			CheckCache() // skips test with message if cache image is not available
		})
		It("should run this test", func() {
			Expect(1 == 1).To(BeTrue())
		})
	})

	Describe("Demo isolated Cache Proxy functionality", func() {
		BeforeEach(func() {
			CheckCacheProxy() // skips test with message if cache proxy image is not available
		})
		It("should run this test", func() {
			Expect(1 == 1).To(BeTrue())
		})
	})
	Describe("Demo Cache & Cache Proxy functionality", func() {
		BeforeEach(func() {
			CheckCache()
			CheckCacheProxy()
		})
		It("should run this test", func() {
			Expect(1 == 1).To(BeTrue())
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

				Expect(ok).To(BeTrue())                                        // for a CACHE message
				Expect(cacheRequestID).ToNot(Equal(message.CacheRequestID(0))) // for a CACHE message
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

	})

})
