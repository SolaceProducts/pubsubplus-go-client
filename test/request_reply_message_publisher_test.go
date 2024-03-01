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

})
