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

package helpers

import (
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"

	//lint:ignore ST1001 dot import is fine for tests
	. "github.com/onsi/gomega"
)

// NewDirectPublisher function
func NewDirectPublisher(
	messagingService solace.MessagingService,
	configurationProviders ...config.PublisherPropertiesConfigurationProvider,
) solace.DirectMessagePublisher {
	builder := messagingService.CreateDirectMessagePublisherBuilder()
	if len(configurationProviders) > 0 {
		for _, configurationProvider := range configurationProviders {
			builder.FromConfigurationProvider(configurationProvider)
		}
	}
	publisher, err := builder.Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Encountered error while building publisher")
	return publisher
}

// NewPersistentPublisher function
func NewPersistentPublisher(
	messagingService solace.MessagingService,
	configurationProviders ...config.PublisherPropertiesConfigurationProvider,
) solace.PersistentMessagePublisher {
	builder := messagingService.CreatePersistentMessagePublisherBuilder()
	if len(configurationProviders) > 0 {
		for _, configurationProvider := range configurationProviders {
			builder.FromConfigurationProvider(configurationProvider)
		}
	}
	publisher, err := builder.Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Encountered error while building publisher")
	return publisher
}

// NewDirectReceiver function
func NewDirectReceiver(
	messagingService solace.MessagingService,
	configurationProviders ...config.ReceiverPropertiesConfigurationProvider,
) solace.DirectMessageReceiver {
	builder := messagingService.CreateDirectMessageReceiverBuilder()
	if len(configurationProviders) > 0 {
		for _, configurationProvider := range configurationProviders {
			builder.FromConfigurationProvider(configurationProvider)
		}
	}
	receiver, err := builder.Build()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Encountered error while building receiver")
	return receiver
}

// NewPersistentReceiver function
func NewPersistentReceiver(
	messagingService solace.MessagingService,
	queueToBind *resource.Queue,
	configurationProviders ...config.ReceiverPropertiesConfigurationProvider,
) solace.PersistentMessageReceiver {
	builder := messagingService.CreatePersistentMessageReceiverBuilder()
	if len(configurationProviders) > 0 {
		for _, configurationProvider := range configurationProviders {
			builder.FromConfigurationProvider(configurationProvider)
		}
	}
	receiver, err := builder.Build(queueToBind)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Encountered error while building receiver")
	return receiver
}

// NewMessage function
func NewMessage(messagingService solace.MessagingService, payload ...string) message.OutboundMessage {
	builder := messagingService.MessageBuilder()
	var msg message.OutboundMessage
	var err error
	if len(payload) > 0 {
		msg, err = builder.BuildWithStringPayload(payload[0])
	} else {
		msg, err = builder.Build()
	}
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "An error occurred while building a new outbound message")
	return msg
}

// NewRequestReplyMessageReceiver function
func NewRequestReplyMessageReceiver(
	messagingService solace.MessagingService,
	subscription *resource.TopicSubscription,
	configurationProviders ...config.ReceiverPropertiesConfigurationProvider,
) solace.RequestReplyMessageReceiver {
	builder := messagingService.RequestReply().CreateRequestReplyMessageReceiverBuilder()
	if len(configurationProviders) > 0 {
		for _, configurationProvider := range configurationProviders {
			builder.FromConfigurationProvider(configurationProvider)
		}
	}
	receiver, err := builder.Build(subscription)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Encountered error while building receiver")
	return receiver
}

// StartRequestReplyMessageReceiverWithDefault function
func StartRequestReplyMessageReceiverWithDefault(
	messagingService solace.MessagingService,
	receiver solace.RequestReplyMessageReceiver,
) {

	err := receiver.Start()
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Encountered error while starting receiver")
	Expect(receiver.IsRunning()).To(BeTrue()) // running state should be true
	messageHandler := func(message message.InboundMessage, replier solace.Replier) {
		if replier == nil { // the replier is only set when received message is request message that would be replied to
			return
		}
		builder := messagingService.MessageBuilder()
		replyMsg, err := builder.BuildWithStringPayload("hello world reply")
		ExpectWithOffset(1, err).ToNot(HaveOccurred())

		replyErr := replier.Reply(replyMsg)
		ExpectWithOffset(1, replyErr).ToNot(HaveOccurred())
	}
	// have receiver push request messages to request message handler
	regErr := receiver.ReceiveAsync(messageHandler)
	ExpectWithOffset(1, regErr).ToNot(HaveOccurred())
	// return receiver
}
