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
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/test/helpers"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const subscriptionString = "some-subscription"

var subscription = resource.TopicSubscriptionOf(subscriptionString)

var _ = Describe("RequestReplyReceiver", func() {
	var messagingService solace.MessagingService
	BeforeEach(func() {
		var err error
		messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration()).Build()
		Expect(err).To(BeNil())
	})

	It("fails to build when given an invalid backpressure type", func() {
		receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().FromConfigurationProvider(config.ReceiverPropertyMap{
			config.ReceiverPropertyDirectBackPressureStrategy: "not a strategy",
		}).Build(subscription)
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
		Expect(receiver).To(BeNil())
	})

	It("fails to build when given an invalid subscription", func() {
		badSubscription := &myCustomRequestReplySubscription{}
		receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(badSubscription)
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
		Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("%T", badSubscription)))
		Expect(receiver).To(BeNil())
	})

	It("fails to start on unconnected messaging service", func() {
		receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(subscription)
		Expect(err).ToNot(HaveOccurred())

		err = receiver.Start()
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
	})

	It("fails to start on disconnected messaging service", func() {
		receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(subscription)
		Expect(err).ToNot(HaveOccurred())

		helpers.ConnectMessagingService(messagingService)
		helpers.DisconnectMessagingService(messagingService)

		err = receiver.Start()
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
	})

	It("fails to receive a message on an unconnected receiver", func() {
		receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().Build(subscription)
		Expect(err).ToNot(HaveOccurred())

		msg, _, err := receiver.ReceiveMessage(-1) // blocking call to receive messages
		Expect(msg).To(BeNil())
		helpers.ValidateError(err, &solace.IllegalStateError{})
	})

	validConfigurations := map[string]config.ReceiverPropertyMap{
		"valid backpressure configuration": {
			config.ReceiverPropertyDirectBackPressureStrategy: config.ReceiverBackPressureStrategyDropLatest,
		},
		"valid backpressure configuration type": {
			config.ReceiverPropertyDirectBackPressureStrategy: config.ReceiverBackPressureStrategyDropOldest,
		},
	}
	for key, val := range validConfigurations {
		validConfiguration := val
		It("succeeds to build with "+key, func() {
			receiver, err := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder().FromConfigurationProvider(validConfiguration).Build(subscription)
			Expect(err).ToNot(HaveOccurred(), "Expected error to not have occurred")
			Expect(receiver).ToNot(BeNil())
			Expect(receiver.IsRunning()).To(BeFalse()) // running state should be false

			expected := &solace.IllegalArgumentError{}
			Expect(err).ToNot(BeAssignableToTypeOf(expected), fmt.Sprintf("Expected error of type %T to not be assignable of type %T", err, expected))
		})
	}
})

type myCustomRequestReplySubscription struct {
}

func (sub *myCustomRequestReplySubscription) GetName() string {
	return "some string"
}

func (sub *myCustomRequestReplySubscription) GetSubscriptionType() string {
	return "some type"
}
