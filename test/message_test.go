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
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/message/sdt"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	sempconfig "solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/sempclient/monitor"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type fromConfigProviderTestCase struct {
	key    config.MessageProperty
	value  interface{}
	getter func(message.Message) (interface{}, bool)
}

var fromConfigProviderTestCases = []fromConfigProviderTestCase{
	{config.MessagePropertyApplicationMessageType, "my message type", func(m message.Message) (interface{}, bool) { return m.GetApplicationMessageType() }},
	{config.MessagePropertyApplicationMessageID, "my message id", func(m message.Message) (interface{}, bool) { return m.GetApplicationMessageID() }},
	{config.MessagePropertyPriority, 100, func(m message.Message) (interface{}, bool) { return m.GetPriority() }},
	{config.MessagePropertyHTTPContentType, "my content type", func(m message.Message) (interface{}, bool) { return m.GetHTTPContentType() }},
	{config.MessagePropertyHTTPContentEncoding, "my content encoding", func(m message.Message) (interface{}, bool) { return m.GetHTTPContentEncoding() }},
	{config.MessagePropertyCorrelationID, "my correlation id", func(m message.Message) (interface{}, bool) { return m.GetCorrelationID() }},
	{config.MessagePropertyPersistentExpiration, time.Unix(99999999, 0), func(m message.Message) (interface{}, bool) { return m.GetExpiration(), true }},
	{config.MessagePropertySequenceNumber, uint(1038492345), func(m message.Message) (interface{}, bool) { return m.GetSequenceNumber() }},
	{config.MessagePropertyClassOfService, 1, func(m message.Message) (interface{}, bool) { return m.GetClassOfService(), true }},
	// TODO getters?
	// This really just makes sure the code doesn't core or whatever when setting these fields..
	{config.MessagePropertyElidingEligible, true, func(m message.Message) (interface{}, bool) { return true, true }},
	{config.MessagePropertyPersistentTimeToLive, int64(1234), func(m message.Message) (interface{}, bool) { return int64(1234), true }},
	{config.MessagePropertyPersistentDMQEligible, true, func(m message.Message) (interface{}, bool) { return true, true }},
	{config.MessagePropertyPersistentAckImmediately, true, func(m message.Message) (interface{}, bool) { return true, true }},
}

type dummyStruct struct{}

var fromConfigProviderInvalidTestCases = map[config.MessageProperty]interface{}{
	config.MessagePropertyApplicationMessageType:   dummyStruct{},
	config.MessagePropertyApplicationMessageID:     dummyStruct{},
	config.MessagePropertyPriority:                 dummyStruct{},
	config.MessagePropertyHTTPContentType:          dummyStruct{},
	config.MessagePropertyHTTPContentEncoding:      dummyStruct{},
	config.MessagePropertyCorrelationID:            dummyStruct{},
	config.MessagePropertyPersistentExpiration:     dummyStruct{},
	config.MessagePropertyPersistentTimeToLive:     dummyStruct{},
	config.MessagePropertyPersistentDMQEligible:    dummyStruct{},
	config.MessagePropertyPersistentAckImmediately: dummyStruct{},
	config.MessagePropertySequenceNumber:           dummyStruct{},
	config.MessagePropertyElidingEligible:          dummyStruct{},
	config.MessagePropertyClassOfService:           dummyStruct{},
	config.MessagePropertySenderID:                 dummyStruct{},
}

// InboundMessageWithTracingSupport represents a message received by a consumer.
type InboundMessageWithTracingSupport interface {
	// Extend the InboundMessage interface.
	message.InboundMessage

	// GetCreationTraceContext will return the trace context metadata used for distributed message tracing message
	GetCreationTraceContext() (traceID [16]byte, spanID [8]byte, sampled bool, traceState string, ok bool)

	// SetTraceContext will set creation trace context metadata used for distributed message tracing.
	SetCreationTraceContext(traceID [16]byte, spanID [8]byte, sampled bool, traceState *string) (ok bool)

	// GetTransportTraceContext will return the trace context metadata used for distributed message tracing
	GetTransportTraceContext() (traceID [16]byte, spanID [8]byte, sampled bool, traceState string, ok bool)

	// SetTraceContext will set transport trace context metadata used for distributed message tracing.
	SetTransportTraceContext(traceID [16]byte, spanID [8]byte, sampled bool, traceState *string) (ok bool)

	// GetBaggage will return the baggage string associated with the message
	GetBaggage() (baggage string, ok bool)

	// SetBaggage will set the baggage string associated with the message
	SetBaggage(baggage string) error
}

// OutboundMessageWithTracingSupport represents a message received by a consumer.
type OutboundMessageWithTracingSupport interface {
	// Extend the OutboundMessage interface.
	message.OutboundMessage

	// GetCreationTraceContext will return the trace context metadata used for distributed message tracing message
	GetCreationTraceContext() (traceID [16]byte, spanID [8]byte, sampled bool, traceState string, ok bool)

	// SetTraceContext will set creation trace context metadata used for distributed message tracing.
	SetCreationTraceContext(traceID [16]byte, spanID [8]byte, sampled bool, traceState *string) (ok bool)

	// GetTransportTraceContext will return the trace context metadata used for distributed message tracing
	GetTransportTraceContext() (traceID [16]byte, spanID [8]byte, sampled bool, traceState string, ok bool)

	// SetTraceContext will set transport trace context metadata used for distributed message tracing.
	SetTransportTraceContext(traceID [16]byte, spanID [8]byte, sampled bool, traceState *string) (ok bool)

	// GetBaggage will return the baggage string associated with the message
	GetBaggage() (baggage string, ok bool)

	// SetBaggage will set the baggage string associated with the message
	SetBaggage(baggage string) error
}

var _ = Describe("Local MessageBuilder Tests", func() {
	var messageBuilder solace.OutboundMessageBuilder

	// We are intentionally doing this outside of the BeforeEach block in order to not allocate hundreds of messaging services
	// as they are expensive to create and can cause issues with number of open files on some test systems
	messagingService, _ := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(config.ServicePropertyMap{
		config.AuthenticationPropertySchemeBasicUserName: "default",
		config.AuthenticationPropertySchemeBasicPassword: "default",
		config.TransportLayerPropertyHost:                "localhost",
		config.ServicePropertyVPNName:                    "default",
	}).Build()

	BeforeEach(func() {
		messageBuilder = messagingService.MessageBuilder()
	})

	Describe("OutboundMessage setters and getters", func() {
		It("should be able to print a message builder to a string", func() {
			messageBuilderString := fmt.Sprint(messageBuilder)
			Expect(messageBuilderString).To(ContainSubstring(fmt.Sprintf("%p", messageBuilder)))
		})
		const maxStringSize = 10000
		It("should be able to dump a message with byte array payload", func() {
			size := 1000
			payload := make([]byte, size)
			for i := 0; i < size; i++ {
				payload[i] = 'a'
			}
			message, err := messageBuilder.BuildWithByteArrayPayload(payload)
			Expect(err).ToNot(HaveOccurred())
			str := message.String()
			Expect(str).ToNot(Equal(""))
			Expect(len(str)).To(BeNumerically(">", len(payload)))
		})
		It("should be able to dump a message with string payload", func() {
			size := 1000
			payload := make([]byte, size)
			for i := 0; i < size; i++ {
				payload[i] = 'a'
			}
			message, err := messageBuilder.BuildWithStringPayload(string(payload))
			Expect(err).ToNot(HaveOccurred())
			str := message.String()
			Expect(str).ToNot(Equal(""))
			Expect(len(str)).To(BeNumerically(">", len(payload)))
		})
		It("should be able to dump a message with large byte array payload", func() {
			size := 10000000
			payload := make([]byte, size)
			for i := 0; i < size; i++ {
				payload[i] = 'a'
			}
			message, err := messageBuilder.BuildWithByteArrayPayload(payload)
			Expect(err).ToNot(HaveOccurred())
			str := message.String()
			Expect(str).ToNot(Equal(""))
			// max payload size is 10,000
			Expect(len(str)).To(BeNumerically("<=", maxStringSize))
		})
		It("should be able to dump a message with large string payload", func() {
			size := 10000000
			payload := make([]byte, size)
			for i := 0; i < size; i++ {
				payload[i] = 'a'
			}
			message, err := messageBuilder.BuildWithStringPayload(string(payload))
			Expect(err).ToNot(HaveOccurred())
			str := message.String()
			Expect(str).ToNot(Equal(""))
			Expect(len(str)).To(BeNumerically("<=", maxStringSize))
		})
		It("should be able to dump a message with no payload", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			messageStringer, ok := message.(fmt.Stringer)
			Expect(ok).To(BeTrue())
			Expect(messageStringer.String()).ToNot(Equal(""))
		})
		It("should be able to dump a message with large user property payload", func() {
			size := 10000000
			payload := make([]byte, size)
			for i := 0; i < size; i++ {
				payload[i] = 'a'
			}
			message, err := messageBuilder.WithProperty("some property name", payload).Build()
			Expect(err).ToNot(HaveOccurred())
			messageStringer := message.(fmt.Stringer)
			str := messageStringer.String()
			Expect(str).ToNot(Equal(""))
			// we don't account for large user properties, so this will likely be truncated
			Expect(str).To(ContainSubstring("aaaaaaaa"))
		})
		It("should be able to build/retrieve string payloads", func() {
			payload := "hello world"
			message, err := messageBuilder.BuildWithStringPayload(payload)
			Expect(err).ToNot(HaveOccurred())
			content, ok := message.GetPayloadAsString()
			Expect(ok).To(BeTrue())
			Expect(content).To(Equal(payload))
		})
		It("should be able to build/retrieve byte array payloads", func() {
			payload := []byte("hello world")
			message, err := messageBuilder.BuildWithByteArrayPayload(payload)
			Expect(err).ToNot(HaveOccurred())
			content, ok := message.GetPayloadAsBytes()
			Expect(ok).To(BeTrue())
			Expect(content).To(Equal(payload))
		})
		It("should be able to build/retrieve a nil byte array payload", func() {
			message, err := messageBuilder.BuildWithByteArrayPayload(nil)
			Expect(err).ToNot(HaveOccurred())
			content, ok := message.GetPayloadAsBytes()
			// nil byte array does not set the byte array
			Expect(ok).To(BeFalse())
			Expect(content).To(BeNil())
		})
		It("should be able to build/retrieve a stream payload", func() {
			stream := sdt.Stream{"hello world"}
			message, err := messageBuilder.BuildWithStreamPayload(stream)
			Expect(err).ToNot(HaveOccurred())
			content, ok := message.GetPayloadAsStream()
			Expect(ok).To(BeTrue())
			Expect(content).To(Equal(stream))
		})
		It("should be able to build/retrieve a nil stream payload", func() {
			message, err := messageBuilder.BuildWithStreamPayload(nil)
			Expect(err).ToNot(HaveOccurred())
			content, ok := message.GetPayloadAsStream()
			Expect(ok).To(BeTrue())
			Expect(content).To(Equal(sdt.Stream{}))
		})
		It("should be able to build/retrieve a map payload", func() {
			mapPayload := sdt.Map{"hello": "world"}
			message, err := messageBuilder.BuildWithMapPayload(mapPayload)
			Expect(err).ToNot(HaveOccurred())
			content, ok := message.GetPayloadAsMap()
			Expect(ok).To(BeTrue())
			Expect(content).To(Equal(mapPayload))
		})
		It("should be able to build/retrieve a nil map payload", func() {
			message, err := messageBuilder.BuildWithMapPayload(nil)
			Expect(err).ToNot(HaveOccurred())
			content, ok := message.GetPayloadAsMap()
			Expect(ok).To(BeTrue())
			Expect(content).To(Equal(sdt.Map{}))
		})
		It("should be able to build/retrieve a message with a property overridden in build", func() {
			priority := 1
			messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyPriority: priority,
			})
			overridePriority := 2
			message, err := messageBuilder.Build(config.MessagePropertyMap{
				config.MessagePropertyPriority: overridePriority,
			})
			Expect(err).ToNot(HaveOccurred())
			retrievedPriority, ok := message.GetPriority()
			Expect(ok).To(BeTrue())
			Expect(retrievedPriority).To(Equal(overridePriority))
			message, err = messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedPriority, ok = message.GetPriority()
			Expect(ok).To(BeTrue())
			Expect(retrievedPriority).To(Equal(priority))
		})
		It("should be able to build/retrieve a message with an HTTP content header", func() {
			httpContentType := " HTTP Content Type"
			httpContentEncoding := " HTTP Content Encoding"
			message, err := messageBuilder.WithHTTPContentHeader(httpContentType, httpContentEncoding).Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedHTTPContentType, contentTypeOk := message.GetHTTPContentType()
			Expect(contentTypeOk).To(BeTrue())
			Expect(retrievedHTTPContentType).To(Equal(httpContentType))
			retrievedHTTPContentEncoding, contentEncodingOk := message.GetHTTPContentEncoding()
			Expect(contentEncodingOk).To(BeTrue())
			Expect(retrievedHTTPContentEncoding).To(Equal(httpContentEncoding))
		})
		It("should be able to build/retrieve a message with a priority", func() {
			priority := 1
			message, err := messageBuilder.WithPriority(priority).Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedPriority, ok := message.GetPriority()
			Expect(ok).To(BeTrue())
			Expect(retrievedPriority).To(Equal(priority))
		})
		It("should fail when building a message with a priority greater than 255 (valid 0-255)", func() {
			priority := 256
			message, err := messageBuilder.WithPriority(priority).Build()
			Expect(err).To(HaveOccurred())
			Expect(message).To(BeNil())
		})
		It("should fail when building a message with a class of service greater than 2", func() {
			cos := 3
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyClassOfService: cos,
			}).Build()
			Expect(err).To(HaveOccurred())
			Expect(message).To(BeNil())
		})
		It("should fail when building a message with a class of service less than 0", func() {
			cos := -1
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyClassOfService: cos,
			}).Build()
			Expect(err).To(HaveOccurred())
			Expect(message).To(BeNil())
		})
		It("should be able to build/retrieve a message with a CorrelationID", func() {
			correlationID := "TheCorrelationId"
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyCorrelationID: correlationID,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedCorrelationID, ok := message.GetCorrelationID()
			Expect(ok).To(BeTrue())
			Expect(retrievedCorrelationID).To(Equal(correlationID))
		})
		It("should be able to build/retrieve a message with a Sequence Number", func() {
			var sequenceNumber = 10
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertySequenceNumber: sequenceNumber,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedSequenceNumber, ok := message.GetSequenceNumber()
			Expect(ok).To(BeTrue())
			Expect(retrievedSequenceNumber).To(Equal(int64(sequenceNumber)))
		})
		It("should be able to build/retrieve a message with a Expiration", func() {
			t := time.Unix(99999999, 0)
			message, err := messageBuilder.WithExpiration(t).Build()
			Expect(err).ToNot(HaveOccurred())
			expiration := message.GetExpiration()
			Expect(expiration).To(Equal(t))
		})

		It("should be able to build with an unknown property", func() {
			_, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessageProperty("this isn't a real property"): "some value",
			}).Build()
			Expect(err).ToNot(HaveOccurred())
		})

		It("should build a message with a nil ApplicationMsgType", func() {
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyApplicationMessageType: nil,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedAppMsgType, ok := message.GetApplicationMessageType()
			Expect(ok).To(BeFalse())
			Expect(retrievedAppMsgType).To(Equal(""))
		})
		It("should build a message with a nil ApplicationMsgId", func() {
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyApplicationMessageID: nil,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedAppMsgID, ok := message.GetApplicationMessageID()
			Expect(ok).To(BeFalse())
			Expect(retrievedAppMsgID).To(Equal(""))
		})
		It("should be able to check for nil correlationID", func() {
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyCorrelationID: nil,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			correlationID, ok := message.GetCorrelationID()
			Expect(ok).To(BeFalse())
			Expect(correlationID).To(Equal(""))
		})
		It("should be able to check for nil sequence number", func() {
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertySequenceNumber: nil,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			sequenceNumber, ok := message.GetSequenceNumber()
			Expect(ok).To(BeFalse())
			Expect(sequenceNumber).To(Equal(int64(0)))
		})
		It("should be able to check for nil content encoding", func() {
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyHTTPContentEncoding: nil,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			contentEncoding, ok := message.GetHTTPContentEncoding()
			Expect(ok).To(BeFalse())
			Expect(contentEncoding).To(Equal(""))
		})
		It("should be able to check for nil content type", func() {
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyHTTPContentType: nil,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			contentType, ok := message.GetHTTPContentType()
			Expect(ok).To(BeFalse())
			Expect(contentType).To(Equal(""))
		})
		It("should be able to check for nil priority", func() {
			message, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyPriority: nil,
			}).Build()
			Expect(err).ToNot(HaveOccurred())
			priority, ok := message.GetPriority()
			Expect(ok).To(BeFalse())
			Expect(priority).To(Equal(-1))
		})

		It("should build a message with a unset ApplicationMsgType", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedAppMsgType, ok := message.GetApplicationMessageType()
			Expect(ok).To(BeFalse())
			Expect(retrievedAppMsgType).To(Equal(""))
		})
		It("should build a message with a unset ApplicationMsgId", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			retrievedAppMsgID, ok := message.GetApplicationMessageID()
			Expect(ok).To(BeFalse())
			Expect(retrievedAppMsgID).To(Equal(""))
		})
		It("should be able to check for unset correlationID", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			correlationID, ok := message.GetCorrelationID()
			Expect(ok).To(BeFalse())
			Expect(correlationID).To(Equal(""))
		})
		It("should be able to check for unset sequence number", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			sequenceNumber, ok := message.GetSequenceNumber()
			Expect(ok).To(BeFalse())
			Expect(sequenceNumber).To(Equal(int64(0)))
		})
		It("should be able to check for unset content encoding", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			contentEncoding, ok := message.GetHTTPContentEncoding()
			Expect(ok).To(BeFalse())
			Expect(contentEncoding).To(Equal(""))
		})
		It("should be able to check for unset content type", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			contentType, ok := message.GetHTTPContentType()
			Expect(ok).To(BeFalse())
			Expect(contentType).To(Equal(""))
		})
		It("should be able to check for unset priority", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())
			priority, ok := message.GetPriority()
			Expect(ok).To(BeFalse())
			Expect(priority).To(Equal(-1))
		})

		It("should be able to dump messages with string payloads", func() {
			payload := "hello world"
			message, err := messageBuilder.BuildWithStringPayload(payload)
			Expect(err).ToNot(HaveOccurred())
			content := fmt.Sprint(message)
			// The string exported in msg dump is formatted in blocks of 8
			Expect(content).To(ContainSubstring("hello wo   rld"))
		})

		It("should be able to set Eliding Eligible to true", func() {
			message, err := messageBuilder.WithProperty(config.MessagePropertyElidingEligible, true).Build()
			Expect(err).ToNot(HaveOccurred())
			Expect(fmt.Sprint(message)).To(ContainSubstring("Eliding Eligible"))
		})

		It("should be able to set Eliding Eligible to false", func() {
			message, err := messageBuilder.WithProperty(config.MessagePropertyElidingEligible, false).Build()
			Expect(err).ToNot(HaveOccurred())
			Expect(fmt.Sprint(message)).ToNot(ContainSubstring("Eliding Eligible"))
		})

		var buildFunctions = map[string]func(solace.OutboundMessageBuilder, ...config.MessagePropertiesConfigurationProvider) (message.OutboundMessage, error){
			"empty build": func(omb solace.OutboundMessageBuilder, additionalConfig ...config.MessagePropertiesConfigurationProvider) (message.OutboundMessage, error) {
				return omb.Build(additionalConfig...)
			},
			"array build": func(omb solace.OutboundMessageBuilder, additionalConfig ...config.MessagePropertiesConfigurationProvider) (message.OutboundMessage, error) {
				payload := []byte{1, 2, 3, 4, 5}
				msg, err := omb.BuildWithByteArrayPayload(payload, additionalConfig...)
				if err == nil {
					actual, ok := msg.GetPayloadAsBytes()
					Expect(ok).To(BeTrue())
					Expect(actual).To(Equal(payload))
				}
				return msg, err
			},
			"string build": func(omb solace.OutboundMessageBuilder, additionalConfig ...config.MessagePropertiesConfigurationProvider) (message.OutboundMessage, error) {
				payload := "Hello World"
				msg, err := omb.BuildWithStringPayload(payload, additionalConfig...)
				if err == nil {
					actual, ok := msg.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(actual).To(Equal(payload))
				}
				return msg, err
			},
			"stream build": func(omb solace.OutboundMessageBuilder, additionalConfig ...config.MessagePropertiesConfigurationProvider) (message.OutboundMessage, error) {
				payload := sdt.Stream{"Hello World"}
				msg, err := omb.BuildWithStreamPayload(payload, additionalConfig...)
				if err == nil {
					actual, ok := msg.GetPayloadAsStream()
					Expect(ok).To(BeTrue())
					Expect(actual).To(HaveLen(len(payload)))
					Expect(actual[0]).To(Equal(payload[0]))
				}
				return msg, err
			},
			"map build": func(omb solace.OutboundMessageBuilder, additionalConfig ...config.MessagePropertiesConfigurationProvider) (message.OutboundMessage, error) {
				key := "someKey"
				payload := sdt.Map{key: "Hello World"}
				msg, err := omb.BuildWithMapPayload(payload, additionalConfig...)
				if err == nil {
					actual, ok := msg.GetPayloadAsMap()
					Expect(ok).To(BeTrue())
					Expect(actual).To(HaveLen(len(payload)))
					Expect(actual[key]).To(Equal(payload[key]))
				}
				return msg, err
			},
		}

		for builderName, builderFuncRef := range buildFunctions {
			builderFunc := builderFuncRef
			Context("using build "+builderName, func() {
				It("can set and retrieve all message options", func() {
					for _, configProvider := range fromConfigProviderTestCases {
						messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{configProvider.key: configProvider.value})
					}
					msg, err := builderFunc(messageBuilder)
					Expect(err).ToNot(HaveOccurred())
					for _, configProvider := range fromConfigProviderTestCases {
						retrieved, ok := configProvider.getter(msg)
						Expect(ok).To(BeTrue())
						Expect(retrieved).To(BeEquivalentTo(configProvider.value))
					}
					msg.Dispose()
				})

				for _, testCase := range fromConfigProviderTestCases {
					key := testCase.key
					value := testCase.value
					getter := testCase.getter
					Context("with property "+string(key), func() {

						It("should be able to build a message with property set from a configuration provider", func() {
							msg, err := builderFunc(messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{key: value}))
							Expect(err).ToNot(HaveOccurred())
							retrieved, ok := getter(msg)
							Expect(ok).To(BeTrue())
							Expect(retrieved).To(BeEquivalentTo(value))
							msg.Dispose()
						})

						It("should be able to provide additional configuration at build time", func() {
							msg, err := builderFunc(messageBuilder, config.MessagePropertyMap{key: value})
							Expect(err).ToNot(HaveOccurred())
							retrieved, ok := getter(msg)
							Expect(ok).To(BeTrue())
							Expect(retrieved).To(BeEquivalentTo(value))
							msg.Dispose()
						})

						It("should be able to provide multiple additional configurations at build time", func() {
							msg, err := builderFunc(messageBuilder, config.MessagePropertyMap{key: nil},
								config.MessagePropertyMap{key: "some invalid value"},
								nil,
								config.MessagePropertyMap{key: value})
							Expect(err).ToNot(HaveOccurred())
							retrieved, ok := getter(msg)
							Expect(ok).To(BeTrue())
							Expect(retrieved).To(BeEquivalentTo(value))
							msg.Dispose()
						})

						It("should be able to build with the value set to nil", func() {
							msg, err := builderFunc(messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
								key: nil,
							}))
							Expect(err).ToNot(HaveOccurred())
							// TODO check default? no way to reliably do so
							msg.Dispose()
						})
					})
				}

				for testKey, testVal := range fromConfigProviderInvalidTestCases {
					key := testKey
					value := testVal
					Context("with invalid key value for key "+string(key), func() {
						It("should fail to build a message with property set from a configuration provider to an invalid value", func() {
							messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{key: value})
							// multiple builds expecting them all to fail
							for i := 0; i < 2; i++ {
								msg, err := builderFunc(messageBuilder)
								Expect(err).To(HaveOccurred())
								Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
								Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("%T", value)))
								Expect(msg).To(BeNil())
							}
						})
						It("should not fail to build subsequent messages when passing invalid value to build", func() {
							msg, err := builderFunc(messageBuilder, config.MessagePropertyMap{key: value})
							Expect(err).To(HaveOccurred())
							Expect(err).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
							Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("%T", value)))
							Expect(msg).To(BeNil())
							msg, err = builderFunc(messageBuilder)
							Expect(err).ToNot(HaveOccurred())
							msg.Dispose()
						})
					})
				}
			})
		}

		It("should be able to dispose of an outbound message with string paylaod", func() {
			str := "hello world"
			msg, err := messageBuilder.BuildWithStringPayload(str)
			Expect(err).ToNot(HaveOccurred())
			msg.Dispose()
			payload, ok := msg.GetPayloadAsString()
			Expect(ok).To(BeFalse())
			Expect(payload).To(Equal(""))
		})

		It("should be able to dispose of an outbound message with byte array paylaod", func() {
			bytes := []byte("hello world")
			msg, err := messageBuilder.BuildWithByteArrayPayload(bytes)
			Expect(err).ToNot(HaveOccurred())
			msg.Dispose()
			payload, ok := msg.GetPayloadAsBytes()
			Expect(ok).To(BeFalse())
			Expect(payload).To(BeNil())
		})

		messageGetterList := map[string](func(msg message.Message) func()){
			"GetApplicationMessageID":   func(msg message.Message) func() { return func() { msg.GetApplicationMessageID() } },
			"GetApplicationMessageType": func(msg message.Message) func() { return func() { msg.GetApplicationMessageType() } },
			"GetCorrelationID":          func(msg message.Message) func() { return func() { msg.GetCorrelationID() } },
			"GetExpiration":             func(msg message.Message) func() { return func() { msg.GetExpiration() } },
			"GetHTTPContentEncoding":    func(msg message.Message) func() { return func() { msg.GetHTTPContentEncoding() } },
			"GetHTTPContentType":        func(msg message.Message) func() { return func() { msg.GetHTTPContentType() } },
			"GetPriority":               func(msg message.Message) func() { return func() { msg.GetPriority() } },
			"GetProperties":             func(msg message.Message) func() { return func() { msg.GetProperties() } },
			"GetSequenceNumber":         func(msg message.Message) func() { return func() { msg.GetSequenceNumber() } },
		}

		for functionName, getter := range messageGetterList {
			function := getter
			It("does not panic on freed message when calling "+functionName, func() {
				msg, err := messageBuilder.Build()
				Expect(err).ToNot(HaveOccurred())
				msg.Dispose()
				Expect(function(msg)).ToNot(Panic())
			})
		}

	})

})

var _ = Describe("Remote Message Tests", func() {

	const topic = "remote-message-tests"

	var messagingService solace.MessagingService
	var messageBuilder solace.OutboundMessageBuilder

	BeforeEach(func() {
		builder := messaging.NewMessagingServiceBuilder().
			FromConfigurationProvider(helpers.DefaultConfiguration()).
			FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyGenerateSendTimestamps:    true,
				config.ServicePropertyGenerateReceiveTimestamps: true,
				config.ServicePropertyGenerateSenderID:          true,
			})

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
			receiver, err = messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(resource.TopicSubscriptionOf(topic)).Build()
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

		It("should be able to publish/receive a message with no payload", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsString()
				Expect(ok).To(BeFalse())
				Expect(content).To(Equal(""))
				byteContent, ok := message.GetPayloadAsBytes()
				Expect(ok).To(BeFalse())
				Expect(byteContent).To(BeNil())
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a string payload", func() {
			payload := "hello world"
			message, err := messageBuilder.BuildWithStringPayload(payload)
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(content).To(Equal(payload))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a string payload using PublishString", func() {
			payload := "hello world"

			err := publisher.PublishString(payload, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(content).To(Equal(payload))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with an empty string payload", func() {
			payload := ""
			message, err := messageBuilder.BuildWithStringPayload(payload)
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(content).To(Equal(payload))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with an empty string payload using PublishString", func() {
			payload := ""

			err := publisher.PublishString(payload, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(content).To(Equal(payload))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a byte array payload", func() {
			payload := []byte("hello world")
			message, err := messageBuilder.BuildWithByteArrayPayload(payload)
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsBytes()
				Expect(ok).To(BeTrue())
				Expect(content).To(Equal(payload))
				stringContent, ok := message.GetPayloadAsString()
				Expect(ok).To(BeFalse())
				Expect(stringContent).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a byte array payload using PublishBytes", func() {
			payload := []byte("hello world")

			err := publisher.PublishBytes(payload, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsBytes()
				Expect(ok).To(BeTrue())
				Expect(content).To(Equal(payload))
				stringContent, ok := message.GetPayloadAsString()
				Expect(ok).To(BeFalse())
				Expect(stringContent).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a zero length byte array payload", func() {
			message, err := messageBuilder.BuildWithByteArrayPayload([]byte{})
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsBytes()
				Expect(ok).To(BeFalse())
				Expect(content).To(BeNil())
				stringContent, ok := message.GetPayloadAsString()
				Expect(ok).To(BeFalse())
				Expect(stringContent).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a zero length byte array payload using PublishBytes", func() {
			err := publisher.PublishBytes([]byte{}, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsBytes()
				Expect(ok).To(BeFalse())
				Expect(content).To(BeNil())
				stringContent, ok := message.GetPayloadAsString()
				Expect(ok).To(BeFalse())
				Expect(stringContent).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a nil byte array payload", func() {
			message, err := messageBuilder.BuildWithByteArrayPayload(nil)
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsBytes()
				Expect(ok).To(BeFalse())
				Expect(content).To(BeNil())
				stringContent, ok := message.GetPayloadAsString()
				Expect(ok).To(BeFalse())
				Expect(stringContent).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a nil byte array payload using PublishBytes", func() {
			err := publisher.PublishBytes(nil, resource.TopicOf(topic))
			Expect(err).ToNot(HaveOccurred())

			select {
			case message := <-inboundMessageChannel:
				content, ok := message.GetPayloadAsBytes()
				Expect(ok).To(BeFalse())
				Expect(content).To(BeNil())
				stringContent, ok := message.GetPayloadAsString()
				Expect(ok).To(BeFalse())
				Expect(stringContent).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with an HTTP content header", func() {
			httpContentType := " HTTP Content Type"
			httpContentEncoding := " HTTP Content Encoding"
			message, err := messageBuilder.WithHTTPContentHeader(httpContentType, httpContentEncoding).Build()
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				retrievedHTTPContentType, contentTypeOk := message.GetHTTPContentType()
				Expect(contentTypeOk).To(BeTrue())
				Expect(retrievedHTTPContentType).To(Equal(httpContentType))
				retrievedHTTPContentEncoding, contentEncodingOk := message.GetHTTPContentEncoding()
				Expect(contentEncodingOk).To(BeTrue())
				Expect(retrievedHTTPContentEncoding).To(Equal(httpContentEncoding))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a priority", func() {
			priority := 1
			message, err := messageBuilder.WithPriority(priority).Build()
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				retrievedPriority, ok := message.GetPriority()
				Expect(ok).To(BeTrue())
				Expect(retrievedPriority).To(Equal(priority))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with an expiration", func() {
			t := time.Unix(99999999, 0)
			message, err := messageBuilder.WithExpiration(t).Build()
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				expiration := message.GetExpiration()
				Expect(expiration).To(Equal(t))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		// Regression for SOL-64507
		It("should be able to print a discard notification to string and not receive a message", func() {
			outboundMsg, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(outboundMsg, resource.TopicOf(topic))

			var inboundMsg message.InboundMessage
			Eventually(inboundMessageChannel).Should(Receive(&inboundMsg))
			discardNotification := inboundMsg.GetMessageDiscardNotification()
			Expect(fmt.Sprint(discardNotification)).ToNot(Equal(fmt.Sprint(inboundMsg)))
		})

		It("should be able to set both solace properties and user properties via a message builder", func() {
			key := "myUserProperty"
			value := "Some Value"
			t := time.Unix(99999999, 0)
			outboundMsg, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{
				config.MessagePropertyPersistentExpiration: t,
				config.MessageProperty(key):                value,
			}).Build()
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(outboundMsg, resource.TopicOf(topic))

			var inboundMsg message.InboundMessage
			Eventually(inboundMessageChannel).Should(Receive(&inboundMsg))
			Expect(inboundMsg.GetExpiration()).To(Equal(t))
			actualVal, ok := inboundMsg.GetProperty(key)
			Expect(ok).To(BeTrue())
			Expect(actualVal).To(Equal(value))
		})

		It("should be able to set both solace properties and user properties via WithProperty", func() {
			key := "myUserProperty"
			value := "Some Value"
			t := time.Unix(99999999, 0)
			outboundMsg, err := messageBuilder.
				WithProperty(config.MessagePropertyPersistentExpiration, t).
				WithProperty(config.MessageProperty(key), value).
				Build()
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(outboundMsg, resource.TopicOf(topic))

			var inboundMsg message.InboundMessage
			Eventually(inboundMessageChannel).Should(Receive(&inboundMsg))
			Expect(inboundMsg.GetExpiration()).To(Equal(t))
			actualVal, ok := inboundMsg.GetProperty(key)
			Expect(ok).To(BeTrue())
			Expect(actualVal).To(Equal(value))
		})

		It("should be able to set both solace properties and user properties at Publish time with PublishWithProperties", func() {
			key := "myUserProperty"
			value := "Some Value"
			t := time.Unix(99999999, 0)
			outboundMsg, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			publisher.PublishWithProperties(outboundMsg, resource.TopicOf(topic), config.MessagePropertyMap{
				config.MessagePropertyPersistentExpiration: t,
				config.MessageProperty(key):                value,
			})

			var inboundMsg message.InboundMessage
			Eventually(inboundMessageChannel).Should(Receive(&inboundMsg))
			Expect(inboundMsg.GetExpiration()).To(Equal(t))
			actualVal, ok := inboundMsg.GetProperty(key)
			Expect(ok).To(BeTrue())
			Expect(actualVal).To(Equal(value))
		})

		for _, testCase := range fromConfigProviderTestCases {
			key := testCase.key
			value := testCase.value
			getter := testCase.getter
			It("should be able to publish a message with "+string(key)+" set from a configuration provider", func() {
				outboundMessage, err := messageBuilder.FromConfigurationProvider(config.MessagePropertyMap{key: value}).Build()
				Expect(err).ToNot(HaveOccurred())

				publisher.Publish(outboundMessage, resource.TopicOf(topic))

				select {
				case inboundMessage := <-inboundMessageChannel:
					retrieved, ok := getter(inboundMessage)
					Expect(ok).To(BeTrue())
					Expect(retrieved).To(BeEquivalentTo(value))
				case <-time.After(1 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})
			It("should be able to publish a message with "+string(key)+" set from a configuration provider at send time", func() {
				outboundMessage, err := messageBuilder.Build()
				Expect(err).ToNot(HaveOccurred())

				publisher.PublishWithProperties(outboundMessage, resource.TopicOf(topic), config.MessagePropertyMap{key: value})

				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				}, 10*time.Second).Should(BeNumerically("==", uint64(1)))

				select {
				case inboundMessage := <-inboundMessageChannel:
					retrieved, ok := getter(inboundMessage)
					Expect(ok).To(BeTrue())
					Expect(retrieved).To(BeEquivalentTo(value))
				case <-time.After(5 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})
		}

		It("should be able to get the destination name from a received message", func() {
			msg, err := messageBuilder.BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				destination := inboundMessage.GetDestinationName()
				Expect(destination).To(Equal(topic))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the rx timestamp from a received message", func() {
			msg, err := messageBuilder.BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			startTime := time.Now()
			// we need to make sure the sender timestamp is slightly after the start time
			<-time.After(10 * time.Millisecond)
			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				endTime := time.Now()
				rxTimestamp, ok := inboundMessage.GetTimeStamp()
				Expect(ok).To(BeTrue())
				Expect(rxTimestamp).To(BeTemporally(">=", startTime))
				Expect(rxTimestamp).To(BeTemporally("<=", endTime))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the sender timestamp from a received message", func() {
			msg, err := messageBuilder.BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			startTime := time.Now()
			// we need to make sure the sender timestamp is slightly after the start time
			<-time.After(10 * time.Millisecond)
			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				endTime := time.Now()
				senderTimestamp, ok := inboundMessage.GetSenderTimestamp()
				Expect(ok).To(BeTrue())
				Expect(senderTimestamp).To(BeTemporally(">=", startTime))
				Expect(senderTimestamp).To(BeTemporally("<=", endTime))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the sender ID from a received message", func() {
			msg, err := messageBuilder.BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				senderID, ok := inboundMessage.GetSenderID()
				Expect(ok).To(BeTrue())
				Expect(senderID).To(Equal(messagingService.GetApplicationID()))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the sender ID from a received message when overridden", func() {
			expectedSenderID := "someSenderID"
			msg, err := messageBuilder.FromConfigurationProvider(
				config.MessagePropertyMap{
					config.MessagePropertySenderID: expectedSenderID,
				},
			).BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				senderID, ok := inboundMessage.GetSenderID()
				Expect(ok).To(BeTrue())
				Expect(senderID).To(Equal(expectedSenderID))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the sender ID from a received message when overridden via builder function", func() {
			expectedSenderID := "someSenderID"
			msg, err := messageBuilder.WithSenderID(expectedSenderID).BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				senderID, ok := inboundMessage.GetSenderID()
				Expect(ok).To(BeTrue())
				Expect(senderID).To(Equal(expectedSenderID))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the application message ID from a received message when set via builder function", func() {
			expectedMessageID := "someSenderID"
			msg, err := messageBuilder.WithApplicationMessageID(expectedMessageID).BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				messageID, ok := inboundMessage.GetApplicationMessageID()
				Expect(ok).To(BeTrue())
				Expect(messageID).To(Equal(expectedMessageID))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the application message type from a received message when set via builder function", func() {
			applicationMessageType := "someApplicationMessageType"
			msg, err := messageBuilder.WithApplicationMessageType(applicationMessageType).BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				messageType, ok := inboundMessage.GetApplicationMessageType()
				Expect(ok).To(BeTrue())
				Expect(messageType).To(Equal(applicationMessageType))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the sender ID from a received message when set via builder function", func() {
			expectedSequenceNumber := uint64(20)
			msg, err := messageBuilder.WithSequenceNumber(expectedSequenceNumber).BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				sequenceNumber, ok := inboundMessage.GetSequenceNumber()
				Expect(ok).To(BeTrue())
				Expect(sequenceNumber).To(Equal(int64(expectedSequenceNumber)))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the sender ID from a received message when set via builder function", func() {
			expectedCorrelationID := "someSenderID"
			msg, err := messageBuilder.WithCorrelationID(expectedCorrelationID).BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				correlationID, ok := inboundMessage.GetCorrelationID()
				Expect(ok).To(BeTrue())
				Expect(correlationID).To(Equal(expectedCorrelationID))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the RMID from a received message and expect it to be not set", func() {
			msg, err := messageBuilder.BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				rmid, ok := inboundMessage.GetReplicationGroupMessageID()
				Expect(ok).To(BeFalse())
				Expect(rmid).To(BeNil())
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to get the broker discard notification from a received message", func() {
			msg, err := messageBuilder.BuildWithStringPayload("hello world")
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				discardNotification := inboundMessage.GetMessageDiscardNotification()
				Expect(discardNotification.HasBrokerDiscardIndication()).To(BeFalse())
				Expect(discardNotification.HasInternalDiscardIndication()).To(BeFalse())
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to dispose of an inbound message with string paylaod", func() {
			str := "hello world"
			msg, err := messageBuilder.BuildWithStringPayload(str)
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				Expect(inboundMessage.IsDisposed()).To(BeFalse())
				inboundMessage.Dispose()
				Expect(inboundMessage.IsDisposed()).To(BeTrue())
				payload, ok := inboundMessage.GetPayloadAsString()
				Expect(ok).To(BeFalse())
				Expect(payload).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to dispose of an inbound message with byte array paylaod", func() {
			bytes := []byte("hello world")
			msg, err := messageBuilder.BuildWithByteArrayPayload(bytes)
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				Expect(inboundMessage.IsDisposed()).To(BeFalse())
				inboundMessage.Dispose()
				Expect(inboundMessage.IsDisposed()).To(BeTrue())
				payload, ok := inboundMessage.GetPayloadAsBytes()
				Expect(ok).To(BeFalse())
				Expect(payload).To(BeNil())
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to dispose of an inbound message with stream paylaod", func() {
			str := "hello world"
			msg, err := messageBuilder.BuildWithStreamPayload(sdt.Stream{str})
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				Expect(inboundMessage.IsDisposed()).To(BeFalse())
				inboundMessage.Dispose()
				Expect(inboundMessage.IsDisposed()).To(BeTrue())
				payload, ok := inboundMessage.GetPayloadAsStream()
				Expect(ok).To(BeFalse())
				Expect(payload).To(BeNil())
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to dispose of an inbound message with map paylaod", func() {
			bytes := []byte("hello world")
			msg, err := messageBuilder.BuildWithMapPayload(sdt.Map{"key": bytes})
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(msg, resource.TopicOf(topic))

			select {
			case inboundMessage := <-inboundMessageChannel:
				Expect(inboundMessage.IsDisposed()).To(BeFalse())
				inboundMessage.Dispose()
				Expect(inboundMessage.IsDisposed()).To(BeTrue())
				payload, ok := inboundMessage.GetPayloadAsMap()
				Expect(ok).To(BeFalse())
				Expect(payload).To(BeNil())
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		inboundMessageGetterList := map[string](func(msg message.InboundMessage) func()){
			"GetApplicationMessageID":   func(msg message.InboundMessage) func() { return func() { msg.GetApplicationMessageID() } },
			"GetApplicationMessageType": func(msg message.InboundMessage) func() { return func() { msg.GetApplicationMessageType() } },
			"GetCorrelationID":          func(msg message.InboundMessage) func() { return func() { msg.GetCorrelationID() } },
			"GetExpiration":             func(msg message.InboundMessage) func() { return func() { msg.GetExpiration() } },
			"GetHTTPContentEncoding":    func(msg message.InboundMessage) func() { return func() { msg.GetHTTPContentEncoding() } },
			"GetHTTPContentType":        func(msg message.InboundMessage) func() { return func() { msg.GetHTTPContentType() } },
			"GetPriority":               func(msg message.InboundMessage) func() { return func() { msg.GetPriority() } },
			"GetProperties":             func(msg message.InboundMessage) func() { return func() { msg.GetProperties() } },
			"GetSequenceNumber":         func(msg message.InboundMessage) func() { return func() { msg.GetSequenceNumber() } },
			"ClassOfService":            func(msg message.InboundMessage) func() { return func() { msg.GetClassOfService() } },

			"GetDestinationName": func(msg message.InboundMessage) func() { return func() { msg.GetDestinationName() } },
			"GetSenderTimestamp": func(msg message.InboundMessage) func() { return func() { msg.GetSenderTimestamp() } },
			"GetTimestamp":       func(msg message.InboundMessage) func() { return func() { msg.GetTimeStamp() } },

			"GetSenderID":                  func(msg message.InboundMessage) func() { return func() { msg.GetSenderID() } },
			"GetReplicationGroupMessageID": func(msg message.InboundMessage) func() { return func() { msg.GetReplicationGroupMessageID() } },

			"GetMessageDiscardNotification": func(msg message.InboundMessage) func() {
				return func() {
					discardNotification := msg.GetMessageDiscardNotification()
					Expect(discardNotification).To(BeNil())
				}
			},
		}

		for functionName, getter := range inboundMessageGetterList {
			function := getter
			It("does not panic on freed message when calling "+functionName, func() {
				msg, err := messageBuilder.Build()
				Expect(err).ToNot(HaveOccurred())

				publisher.Publish(msg, resource.TopicOf(topic))

				select {
				case inboundMessage := <-inboundMessageChannel:
					inboundMessage.Dispose()
					Expect(function(inboundMessage)).ToNot(Panic())
				case <-time.After(1 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})
		}

		// SDT Tests
		Describe("SDT Tests", func() {

			// If a type is mapped to another type for outputs, it is captured here
			var sdtOutputMappings = map[reflect.Type]reflect.Type{
				reflect.TypeOf(int(0)):  reflect.TypeOf(int64(0)),
				reflect.TypeOf(uint(0)): reflect.TypeOf(uint64(0)),
			}

			// This list captures valid test cases
			var supportedSDTData = []sdt.Data{
				false,
				true,
				int(44),
				uint(123),
				uint8(12),
				int8(22),
				uint16(123),
				int16(234),
				uint32(10202),
				int32(20301),
				uint64(102003),
				int64(200490),
				"",
				"Hello World",
				[]byte{1, 4, 7, 7, 2},
				[]byte{},
				float32(123.45),
				float64(123456.6789),
				sdt.WChar(55),
				sdt.Map{
					"some key": "some value",
				},
				sdt.Stream{"hello world"},
				sdt.Map{},
				sdt.Stream{},
				sdt.Map{
					"nested_map": sdt.Map{
						"some more data": "Hello World",
					},
					"nested_stream": sdt.Stream{
						"some more data!",
					},
				},
				sdt.Stream{
					sdt.Map{
						"this map is nested": "nested!",
					},
					sdt.Stream{
						"this stream is nested",
					},
				},
				resource.QueueDurableExclusive("someQueueName"),
				resource.TopicOf("hello/world"),
			}

			var unsupportedSDTData = []interface{}{
				dummyStruct{},
				&dummyStruct{},
				complex(1, 1),
				uintptr(10202030),
				sdt.Stream{dummyStruct{}},
				sdt.Map{
					"some key": complex(1, 1),
				},
				sdt.Stream{sdt.Map{
					"some key": dummyStruct{},
				}},
				sdt.Map{
					"some key": sdt.Stream{dummyStruct{}},
				},
			}

			for _, testCase := range supportedSDTData {
				valueString := fmt.Sprintf("%T", testCase)
				value := testCase
				It("can add a user property of type "+valueString, func() {
					propKey := "myCustomProp"
					msg, err := messageBuilder.WithProperty(config.MessageProperty(propKey), value).Build()
					Expect(err).ToNot(HaveOccurred())

					validateUserProp := func(msg message.Message) {
						expectedOutputType := reflect.TypeOf(value)
						var expectedOutput interface{} = value
						if newType, ok := sdtOutputMappings[expectedOutputType]; ok {
							expectedOutputType = newType
							expectedOutput = reflect.ValueOf(value).Convert(expectedOutputType).Interface()
						}

						// Check has property
						present := msg.HasProperty(propKey)
						Expect(present).To(BeTrue())
						// Check get property
						retrieved, ok := msg.GetProperty(propKey)
						Expect(ok).To(BeTrue())
						Expect(retrieved).To(Equal(expectedOutput))
						// Check property map
						propertyMap := msg.GetProperties()
						Expect(propertyMap).ToNot(BeNil())
						retrieved, ok = propertyMap[propKey]
						Expect(ok).To(BeTrue())
						Expect(retrieved).To(Equal(expectedOutput))
						// Check the getter function
						mapType := reflect.TypeOf(propertyMap)
						var method *reflect.Method
						// Find the method
						for i := 0; i < mapType.NumMethod(); i++ {
							m := mapType.Method(i)
							mt := m.Type
							if mt.NumIn() == 2 && mt.NumOut() == 2 &&
								mt.Out(0) == expectedOutputType && mt.In(1) == reflect.TypeOf("string") {
								method = &m
								break
							}
						}
						Expect(method).ToNot(BeNil())
						results := method.Func.Call([]reflect.Value{reflect.ValueOf(propertyMap), reflect.ValueOf(propKey)})
						Expect(results[0].Interface()).To(BeEquivalentTo(value))
						Expect(results[1].Interface()).To(BeNil())
					}
					validateUserProp(msg)

					publisher.Publish(msg, resource.TopicOf(topic))
					var inboundMsg message.InboundMessage
					Eventually(inboundMessageChannel).Should(Receive(&inboundMsg))
					validateUserProp(inboundMsg)
				})

				It("can add "+valueString+" to an SDT Map and send as payload", func() {
					propKey := "myCustomData"
					msg, err := messageBuilder.BuildWithMapPayload(sdt.Map{
						propKey: value,
					})
					Expect(err).ToNot(HaveOccurred())

					validatePayloadProperty := func(msg message.Message) {
						expectedOutputType := reflect.TypeOf(value)
						var expectedOutput interface{} = value
						if newType, ok := sdtOutputMappings[expectedOutputType]; ok {
							expectedOutputType = newType
							expectedOutput = reflect.ValueOf(value).Convert(expectedOutputType).Interface()
						}
						// Check property map
						propertyMap, ok := msg.GetPayloadAsMap()
						Expect(ok).To(BeTrue())
						Expect(propertyMap).ToNot(BeNil())
						retrieved, ok := propertyMap[propKey]
						Expect(ok).To(BeTrue())
						Expect(retrieved).To(Equal(expectedOutput))
						// Check the getter function
						mapType := reflect.TypeOf(propertyMap)
						var method *reflect.Method
						// Find the method
						for i := 0; i < mapType.NumMethod(); i++ {
							m := mapType.Method(i)
							mt := m.Type
							if mt.NumIn() == 2 && mt.NumOut() == 2 &&
								mt.Out(0) == expectedOutputType && mt.In(1) == reflect.TypeOf("string") {
								method = &m
								break
							}
						}
						Expect(method).ToNot(BeNil())
						results := method.Func.Call([]reflect.Value{reflect.ValueOf(propertyMap), reflect.ValueOf(propKey)})
						Expect(results[0].Interface()).To(BeEquivalentTo(value))
						Expect(results[1].Interface()).To(BeNil())
					}
					validatePayloadProperty(msg)

					publisher.Publish(msg, resource.TopicOf(topic))
					var inboundMsg message.InboundMessage
					Eventually(inboundMessageChannel).Should(Receive(&inboundMsg))
					validatePayloadProperty(inboundMsg)
				})

				It("can add "+valueString+" to an SDT Stream and send as payload", func() {
					msg, err := messageBuilder.BuildWithStreamPayload(sdt.Stream{
						value,
					})
					Expect(err).ToNot(HaveOccurred())

					validatePayloadProperty := func(msg message.Message) {
						expectedOutputType := reflect.TypeOf(value)
						var expectedOutput interface{} = value
						if newType, ok := sdtOutputMappings[expectedOutputType]; ok {
							expectedOutputType = newType
							expectedOutput = reflect.ValueOf(value).Convert(expectedOutputType).Interface()
						}
						// Check property map
						propertyStream, ok := msg.GetPayloadAsStream()
						Expect(ok).To(BeTrue())
						Expect(propertyStream).ToNot(BeNil())
						Expect(len(propertyStream)).To(Equal(1))
						retrieved := propertyStream[0]
						Expect(retrieved).To(Equal(expectedOutput))
						// Check the getter function
						mapType := reflect.TypeOf(propertyStream)
						var method *reflect.Method
						// Find the method
						for i := 0; i < mapType.NumMethod(); i++ {
							m := mapType.Method(i)
							mt := m.Type
							if mt.NumIn() == 2 && mt.NumOut() == 2 &&
								mt.Out(0) == expectedOutputType && mt.In(1) == reflect.TypeOf(int(0)) {
								method = &m
								break
							}
						}
						Expect(method).ToNot(BeNil())
						results := method.Func.Call([]reflect.Value{reflect.ValueOf(propertyStream), reflect.ValueOf(int(0))})
						Expect(results[0].Interface()).To(BeEquivalentTo(value))
						Expect(results[1].Interface()).To(BeNil())
					}
					validatePayloadProperty(msg)

					publisher.Publish(msg, resource.TopicOf(topic))
					var inboundMsg message.InboundMessage
					Eventually(inboundMessageChannel).Should(Receive(&inboundMsg))
					validatePayloadProperty(inboundMsg)
				})
			}

			for _, testCase := range unsupportedSDTData {
				valueString := fmt.Sprintf("%T", testCase)
				value := testCase

				var validateError = func(value interface{}, err error) {
					// if we are nested it will be a different value
					var drillDown func(val interface{}) interface{}
					drillDown = func(val interface{}) interface{} {
						if m, ok := val.(sdt.Map); ok {
							for _, nestedVal := range m {
								return drillDown(nestedVal)
							}
						} else if s, ok := val.(sdt.Stream); ok {
							for _, nestedVal := range s {
								return drillDown(nestedVal)
							}
						}
						return val
					}

					Expect(err).To(HaveOccurred())
					Expect(err).To(BeAssignableToTypeOf(&sdt.IllegalTypeError{}))
					expectedValue := drillDown(value)
					Expect((err.(*sdt.IllegalTypeError)).Data).To(Equal(expectedValue))
					// Assert error message is helpful and describes the unsupported type
					Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("%T", expectedValue)))
				}

				It("returns an error when adding a user property of type "+valueString, func() {
					propKey := "myCustomProp"
					msg, err := messageBuilder.WithProperty(config.MessageProperty(propKey), value).Build()
					Expect(msg).To(BeNil())
					validateError(value, err)
				})

				It("returns an error when adding "+valueString+" to an SDTMap payload", func() {
					propKey := "myCustomProp"
					msg, err := messageBuilder.BuildWithMapPayload(sdt.Map{
						propKey: value,
					})
					Expect(msg).To(BeNil())
					validateError(value, err)
				})

				It("returns an error when adding "+valueString+" to an SDTStream payload", func() {
					msg, err := messageBuilder.BuildWithStreamPayload(sdt.Stream{
						value,
					})
					Expect(msg).To(BeNil())
					validateError(value, err)
				})
			}

			It("can retrieve a user property multiple times", func() {
				key := "somekey"
				val := "somevalue"
				msg, err := messageBuilder.WithProperty(config.MessageProperty(key), val).Build()
				Expect(err).ToNot(HaveOccurred())
				Expect(msg.HasProperty(key)).To(BeTrue())
				actualVal, ok := msg.GetProperty(key)
				Expect(ok).To(BeTrue())
				Expect(actualVal).To(Equal(val))
				// try it again
				Expect(msg.HasProperty(key)).To(BeTrue())
				actualVal, ok = msg.GetProperty(key)
				Expect(ok).To(BeTrue())
				Expect(actualVal).To(Equal(val))
			})

			It("can retrieve the user property map multiple times", func() {
				key := "somekey"
				val := "somevalue"
				msg, err := messageBuilder.WithProperty(config.MessageProperty(key), val).Build()
				Expect(err).ToNot(HaveOccurred())
				m := msg.GetProperties()
				Expect(m).ToNot(BeNil())
				actualVal, ok := m[key]
				Expect(ok).To(BeTrue())
				Expect(actualVal).To(Equal(val))

				m = msg.GetProperties()
				Expect(m).ToNot(BeNil())
				actualVal, ok = m[key]
				Expect(ok).To(BeTrue())
				Expect(actualVal).To(Equal(val))
				// try it again
				Expect(msg.HasProperty(key)).To(BeTrue())
				actualVal, ok = msg.GetProperty(key)
				Expect(ok).To(BeTrue())
				Expect(actualVal).To(Equal(val))
			})

			It("can retrieve the payload as map multiple times", func() {
				myMap := sdt.Map{"someKey": "someVal"}
				msg, err := messageBuilder.BuildWithMapPayload(myMap)
				Expect(err).ToNot(HaveOccurred())
				payload, ok := msg.GetPayloadAsMap()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal(myMap))
				// do it again
				payload, ok = msg.GetPayloadAsMap()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal(myMap))
			})

			It("can retrieve the payload as stream multiple times", func() {
				myStream := sdt.Stream{"someVal"}
				msg, err := messageBuilder.BuildWithStreamPayload(myStream)
				Expect(err).ToNot(HaveOccurred())
				payload, ok := msg.GetPayloadAsStream()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal(myStream))
				// do it again
				payload, ok = msg.GetPayloadAsStream()
				Expect(ok).To(BeTrue())
				Expect(payload).To(Equal(myStream))
			})

			It("can attempt to retrieve a missing property", func() {
				msg, err := messageBuilder.Build()
				Expect(err).ToNot(HaveOccurred())
				sdtMap := msg.GetProperties()
				Expect(sdtMap).To(BeNil())
				nonexistentKey := "somekey"
				Expect(msg.HasProperty(nonexistentKey)).To(BeFalse())
				val, ok := msg.GetProperty(nonexistentKey)
				Expect(ok).To(BeFalse())
				Expect(val).To(BeNil())
			})

			It("can attempt to retrieve a missing property when other properties are present", func() {
				msg, err := messageBuilder.WithProperty("notretrieved", "somevalue").Build()
				Expect(err).ToNot(HaveOccurred())
				nonexistentKey := "somekey"
				Expect(msg.HasProperty(nonexistentKey)).To(BeFalse())
				val, ok := msg.GetProperty(nonexistentKey)
				Expect(ok).To(BeFalse())
				Expect(val).To(BeNil())
			})

			It("fails to retrieve an SDTMap payload when not set", func() {
				validate := func(msg message.Message, err error) {
					Expect(err).ToNot(HaveOccurred())
					payload, ok := msg.GetPayloadAsMap()
					Expect(ok).To(BeFalse())
					Expect(payload).To(BeNil())
				}
				msg, err := messageBuilder.Build()
				validate(msg, err)
				msg, err = messageBuilder.BuildWithStringPayload("hello")
				validate(msg, err)
				msg, err = messageBuilder.BuildWithByteArrayPayload([]byte("hello"))
				validate(msg, err)
				msg, err = messageBuilder.BuildWithStreamPayload(sdt.Stream{"hello"})
				validate(msg, err)
			})

			It("fails to retrieve an SDTStream payload when not set", func() {
				validate := func(msg message.Message, err error) {
					Expect(err).ToNot(HaveOccurred())
					payload, ok := msg.GetPayloadAsStream()
					Expect(ok).To(BeFalse())
					Expect(payload).To(BeNil())
				}
				msg, err := messageBuilder.Build()
				validate(msg, err)
				msg, err = messageBuilder.BuildWithStringPayload("hello")
				validate(msg, err)
				msg, err = messageBuilder.BuildWithByteArrayPayload([]byte("hello"))
				validate(msg, err)
				msg, err = messageBuilder.BuildWithMapPayload(sdt.Map{"hello": "world"})
				validate(msg, err)
			})

			It("can add a user property of type nil", func() {
				propKey := "myCustomProp"
				msg, err := messageBuilder.WithProperty(config.MessageProperty(propKey), nil).Build()
				Expect(err).ToNot(HaveOccurred())
				retrieved, ok := msg.GetProperty(propKey)
				Expect(ok).To(BeTrue())
				// Check that the type was matched too
				Expect(retrieved).To(BeNil())
			})
		})
	})

	Describe("Persistent message properties", func() {

		var queueName = "persistent_message_test"
		var topic = "persistent_msg_test_topic"
		var payload = "hello world"

		BeforeEach(func() {
			helpers.ConnectMessagingService(messagingService)
			helpers.CreateQueue(queueName, topic)
		})
		AfterEach(func() {
			helpers.DisconnectMessagingService(messagingService)
			helpers.DeleteQueue(queueName)
		})

		Context("with a started receiver", func() {
			var receiver solace.PersistentMessageReceiver
			BeforeEach(func() {
				receiver = helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
				Expect(receiver.Start()).ToNot(HaveOccurred())
			})
			AfterEach(func() {
				Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			})

			It("can retrieve the message's RMID", func() {
				helpers.PublishOnePersistentMessage(messagingService, topic, payload)
				message, err := receiver.ReceiveMessage(10 * time.Second)
				Expect(err).ToNot(HaveOccurred())
				rmid, ok := message.GetReplicationGroupMessageID()
				Expect(ok).To(BeTrue())
				Expect(rmid.String()).To(HavePrefix("rmid1:"))
			})

			It("can check message redelivery", func() {
				helpers.PublishOnePersistentMessage(messagingService, topic, payload)
				message, err := receiver.ReceiveMessage(10 * time.Second)
				Expect(err).ToNot(HaveOccurred())
				Expect(message.IsRedelivered()).To(BeFalse())
				receivedPayload, ok := message.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(receivedPayload).To(Equal(payload))
				// do not acknowledge
				Expect(receiver.Terminate(0)).ToNot(HaveOccurred())
				receiver = helpers.NewPersistentReceiver(messagingService, resource.QueueDurableExclusive(queueName))
				Expect(receiver.Start()).ToNot(HaveOccurred())
				message, err = receiver.ReceiveMessage(10 * time.Second)
				Expect(err).ToNot(HaveOccurred())
				Expect(message.IsRedelivered()).To(BeTrue())
				receivedPayload, ok = message.GetPayloadAsString()
				Expect(ok).To(BeTrue())
				Expect(receivedPayload).To(Equal(payload))
				Expect(receiver.Ack(message)).ToNot(HaveOccurred())
			})
		})

		Context("with a dead message queue and respect ttl", func() {
			const dmqName = "persistent_message_test_dmq"
			BeforeEach(func() {
				helpers.CreateQueue(dmqName)
				testcontext.SEMP().Config().QueueApi.UpdateMsgVpnQueue(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnQueue{
						DeadMsgQueue:      dmqName,
						RespectTtlEnabled: helpers.True,
					},
					testcontext.Messaging().VPN,
					queueName,
					nil,
				)
			})
			AfterEach(func() {
				helpers.DeleteQueue(dmqName)
			})

			publishMsg := func(msg message.OutboundMessage) {
				publisher := helpers.NewPersistentPublisher(messagingService)
				Expect(publisher.Start()).ToNot(HaveOccurred())
				publisher.Publish(msg, resource.TopicOf(topic), nil, nil)
				Expect(publisher.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			}

			validateQueueContents := func(dmqEligible bool) {
				Eventually(func() []monitor.MsgVpnQueueMsg {
					return helpers.GetQueueMessages(queueName)
				}, 2*time.Second).Should(HaveLen(0))
				var dmqCount = 0
				if dmqEligible == true {
					dmqCount = 1
				}
				Eventually(func() []monitor.MsgVpnQueueMsg {
					return helpers.GetQueueMessages(dmqName)
				}, 2*time.Second).Should(HaveLen(dmqCount))
				// In cases where dmq count is 0, we want to make sure it stays that way
				if !dmqEligible {
					Consistently(func() []monitor.MsgVpnQueueMsg {
						return helpers.GetQueueMessages(dmqName)
					}, 1*time.Second).Should(HaveLen(dmqCount))
				}
			}

			DescribeTable("message timeout tests",
				func(dmqEligible bool) {
					msg, err := messagingService.MessageBuilder().FromConfigurationProvider(config.MessagePropertyMap{
						config.MessagePropertyPersistentDMQEligible: dmqEligible,
						config.MessagePropertyPersistentTimeToLive:  1000,
					}).Build()
					Expect(err).ToNot(HaveOccurred())
					publishMsg(msg)
					validateQueueContents(dmqEligible)
				},
				Entry("with DMQ eligible false", false),
				Entry("with DMQ eligible true", true),
			)

			It("should default DMQ eligible to true", func() {
				msg, err := messagingService.MessageBuilder().FromConfigurationProvider(config.MessagePropertyMap{
					config.MessagePropertyPersistentTimeToLive: 1000,
				}).Build()
				Expect(err).ToNot(HaveOccurred())
				publishMsg(msg)
				validateQueueContents(true)
			})
		})
	})

	Describe("Published and received message with Distributed Tracing support", func() {
		var publisher solace.DirectMessagePublisher
		var receiver solace.DirectMessageReceiver
		var inboundMessageChannel chan InboundMessageWithTracingSupport

		BeforeEach(func() {
			var err error
			err = messagingService.Connect()
			Expect(err).ToNot(HaveOccurred())

			publisher, err = messagingService.CreateDirectMessagePublisherBuilder().Build()
			Expect(err).ToNot(HaveOccurred())
			receiver, err = messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(resource.TopicSubscriptionOf(topic)).Build()
			Expect(err).ToNot(HaveOccurred())

			err = publisher.Start()
			Expect(err).ToNot(HaveOccurred())

			inboundMessageChannel = make(chan InboundMessageWithTracingSupport)
			receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
				inboundMessageChannel <- inboundMessage.(InboundMessageWithTracingSupport)
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

		It("should be able to publish/receive a message with no creation context", func() {
			message, err := messageBuilder.Build() // no creation context is set on message
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				traceID, spanID, sampled, traceState, ok := message.GetCreationTraceContext()
				Expect(ok).To(BeFalse())
				Expect(traceID).To(Equal([16]byte{})) // empty
				Expect(spanID).To(Equal([8]byte{}))   // empty
				Expect(sampled).To(BeFalse())
				Expect(traceState).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a valid creation context", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			// cast the message to the extended interface that has message tracing support
			messageWithDT := message.(OutboundMessageWithTracingSupport)

			// set creation context on message
			creationCtxTraceID, _ := hex.DecodeString("79f90916c9a3dad1eb4b328e00469e45")
			creationCtxSpanID, _ := hex.DecodeString("3b364712c4e1f17f")
			sampledValue := true
			traceStateValue := "sometrace1=Example1"

			var creationCtxTraceID16 [16]byte
			var creationCtxSpanID8 [8]byte
			copy(creationCtxTraceID16[:], creationCtxTraceID)
			copy(creationCtxSpanID8[:], creationCtxSpanID)
			ok := messageWithDT.SetCreationTraceContext(creationCtxTraceID16, creationCtxSpanID8, sampledValue, &traceStateValue)
			Expect(ok).To(BeTrue())

			publisher.Publish(messageWithDT, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				traceID, spanID, sampled, traceState, ok := message.GetCreationTraceContext()
				Expect(ok).To(BeTrue())
				Expect(traceID).To(Equal(creationCtxTraceID16)) // should be equal
				Expect(spanID).To(Equal(creationCtxSpanID8))    // should be equal
				Expect(sampled).To(Equal(sampledValue))
				Expect(traceState).To(Equal(traceStateValue))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a valid creation context without trace state", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			// cast the message to the extended interface that has message tracing support
			messageWithDT := message.(OutboundMessageWithTracingSupport)

			// set creation context on message
			creationCtxTraceID, _ := hex.DecodeString("79f90916c9a3dad1eb4b328e00469e45")
			creationCtxSpanID, _ := hex.DecodeString("3b364712c4e1f17f")
			sampledValue := true

			var creationCtxTraceID16 [16]byte
			var creationCtxSpanID8 [8]byte
			copy(creationCtxTraceID16[:], creationCtxTraceID)
			copy(creationCtxSpanID8[:], creationCtxSpanID)
			ok := messageWithDT.SetCreationTraceContext(creationCtxTraceID16, creationCtxSpanID8, sampledValue, nil) // no trace state
			Expect(ok).To(BeTrue())

			publisher.Publish(messageWithDT, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				traceID, spanID, sampled, traceState, ok := message.GetCreationTraceContext()
				Expect(ok).To(BeTrue())
				Expect(traceID).To(Equal(creationCtxTraceID16)) // should be equal
				Expect(spanID).To(Equal(creationCtxSpanID8))    // should be equal
				Expect(sampled).To(Equal(sampledValue))
				Expect(traceState).To(Equal("")) // should be empty
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with no tranport context", func() {
			message, err := messageBuilder.Build() // no creation context is set on message
			Expect(err).ToNot(HaveOccurred())

			publisher.Publish(message, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				traceID, spanID, sampled, traceState, ok := message.GetTransportTraceContext()
				Expect(ok).To(BeFalse())
				Expect(traceID).To(Equal([16]byte{})) // empty
				Expect(spanID).To(Equal([8]byte{}))   // empty
				Expect(sampled).To(BeFalse())
				Expect(traceState).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a valid transport context", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			// cast the message to the extended interface that has message tracing support
			messageWithDT := message.(OutboundMessageWithTracingSupport)

			// set transport context on message
			transportCtxTraceID, _ := hex.DecodeString("55d30916c9a3dad1eb4b328e00469e45")
			transportCtxSpanID, _ := hex.DecodeString("a7164712c4e1f17f")

			sampledValue := true
			traceStateValue := "trace=Sample"

			var transportCtxTraceID16 [16]byte
			var transportCtxSpanID8 [8]byte
			copy(transportCtxTraceID16[:], transportCtxTraceID)
			copy(transportCtxSpanID8[:], transportCtxSpanID)
			ok := messageWithDT.SetTransportTraceContext(transportCtxTraceID16, transportCtxSpanID8, sampledValue, &traceStateValue)
			Expect(ok).To(BeTrue())

			publisher.Publish(messageWithDT, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				traceID, spanID, sampled, traceState, ok := message.GetTransportTraceContext()
				Expect(ok).To(BeTrue())
				Expect(traceID).To(Equal(transportCtxTraceID16)) // should be equal
				Expect(spanID).To(Equal(transportCtxSpanID8))    // should be equal
				Expect(sampled).To(BeTrue())
				Expect(traceState).To(Equal(traceStateValue))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a valid transport context without trace state", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			// cast the message to the extended interface that has message tracing support
			messageWithDT := message.(OutboundMessageWithTracingSupport)

			// set transport context on message
			transportCtxTraceID, _ := hex.DecodeString("55d30916c9a3dad1eb4b328e00469e45")
			transportCtxSpanID, _ := hex.DecodeString("a7164712c4e1f17f")
			sampledValue := true

			var transportCtxTraceID16 [16]byte
			var transportCtxSpanID8 [8]byte
			copy(transportCtxTraceID16[:], transportCtxTraceID)
			copy(transportCtxSpanID8[:], transportCtxSpanID)
			ok := messageWithDT.SetTransportTraceContext(transportCtxTraceID16, transportCtxSpanID8, sampledValue, nil)
			Expect(ok).To(BeTrue())

			publisher.Publish(messageWithDT, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				traceID, spanID, sampled, traceState, ok := message.GetTransportTraceContext()
				Expect(ok).To(BeTrue())
				Expect(traceID).To(Equal(transportCtxTraceID16)) // should be equal
				Expect(spanID).To(Equal(transportCtxSpanID8))    // should be equal
				Expect(sampled).To(BeTrue())
				Expect(traceState).To(Equal("")) // should be empty
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a valid creation context and no transport context", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			// cast the message to the extended interface that has message tracing support
			messageWithDT := message.(OutboundMessageWithTracingSupport)

			// set creation context on message
			creationCtxTraceID, _ := hex.DecodeString("79f90916c9a3dad1eb4b328e00469e45")
			creationCtxSpanID, _ := hex.DecodeString("3b364712c4e1f17f")
			sampledValue := true
			traceStateValue := "sometrace=Example"

			var creationCtxTraceID16 [16]byte
			var creationCtxSpanID8 [8]byte
			copy(creationCtxTraceID16[:], creationCtxTraceID)
			copy(creationCtxSpanID8[:], creationCtxSpanID)
			ok := messageWithDT.SetCreationTraceContext(creationCtxTraceID16, creationCtxSpanID8, sampledValue, &traceStateValue)
			Expect(ok).To(BeTrue())

			publisher.Publish(messageWithDT, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				creationTraceID, creationSpanID, creationSampled, creationTraceState, creationOk := message.GetCreationTraceContext()
				transportTraceID, transportSpanID, transportSampled, transportTraceState, transportOk := message.GetTransportTraceContext()

				Expect(creationOk).To(BeTrue())
				Expect(creationTraceID).To(Equal(creationCtxTraceID16)) // should be equal
				Expect(creationSpanID).To(Equal(creationCtxSpanID8))    // should be equal
				Expect(creationSampled).To(BeTrue())
				Expect(creationTraceState).To(Equal(traceStateValue))

				Expect(transportOk).To(BeFalse())
				Expect(transportTraceID).To(Equal([16]byte{})) // empty
				Expect(transportSpanID).To(Equal([8]byte{}))   // empty
				Expect(transportSampled).To(BeFalse())
				Expect(transportTraceState).To(Equal(""))

			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with different creation context and transport context", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			// cast the message to the extended interface that has message tracing support
			messageWithDT := message.(OutboundMessageWithTracingSupport)

			// set creation context on message
			creationCtxTraceID, _ := hex.DecodeString("79f90916c9a3dad1eb4b328e00469e45")
			creationCtxSpanID, _ := hex.DecodeString("3b364712c4e1f17f")
			creationCtxTraceState := "sometrace1=Example1"

			// set transport context on message
			transportCtxTraceID, _ := hex.DecodeString("79f90916c9a3dad1eb4b328e00469e45")
			transportCtxSpanID, _ := hex.DecodeString("a7164712c4e1f17f")
			transportCtxTraceState := "sometrace2=Example2"

			var creationCtxTraceID16, transportCtxTraceID16 [16]byte
			var creationCtxSpanID8, transportCtxSpanID8 [8]byte
			copy(creationCtxTraceID16[:], creationCtxTraceID)
			copy(creationCtxSpanID8[:], creationCtxSpanID)
			setCreationCtxOk := messageWithDT.SetCreationTraceContext(creationCtxTraceID16, creationCtxSpanID8, true, &creationCtxTraceState)
			Expect(setCreationCtxOk).To(BeTrue())

			copy(transportCtxTraceID16[:], transportCtxTraceID)
			copy(transportCtxSpanID8[:], transportCtxSpanID)
			setTransportCtxOk := messageWithDT.SetTransportTraceContext(transportCtxTraceID16, transportCtxSpanID8, true, &transportCtxTraceState)
			Expect(setTransportCtxOk).To(BeTrue())

			publisher.Publish(messageWithDT, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				creationTraceID, creationSpanID, creationSampled, creationTraceState, creationOk := message.GetCreationTraceContext()
				transportTraceID, transportSpanID, transportSampled, transportTraceState, transportOk := message.GetTransportTraceContext()

				Expect(creationOk).To(BeTrue())
				Expect(creationTraceID).To(Equal(creationCtxTraceID16)) // should be equal
				Expect(creationSpanID).To(Equal(creationCtxSpanID8))    // should be equal
				Expect(creationSampled).To(BeTrue())
				Expect(creationTraceState).To(Equal(creationCtxTraceState))

				Expect(transportOk).To(BeTrue())
				Expect(transportTraceID).ToNot(Equal([16]byte{}))         // not empty
				Expect(transportTraceID).To(Equal(transportCtxTraceID16)) // should be equal

				Expect(transportSpanID).ToNot(Equal([8]byte{}))        // not empty
				Expect(transportSpanID).To(Equal(transportCtxSpanID8)) // should be equal

				Expect(transportSampled).To(BeTrue())
				Expect(transportTraceState).To(Equal(transportCtxTraceState))

				Expect(creationTraceID).To(Equal(transportTraceID))          // should be equal
				Expect(creationSpanID).ToNot(Equal(transportSpanID))         // should not be equal
				Expect(creationTraceState).ToNot(Equal(transportTraceState)) // should not be equal

			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with no baggage", func() {
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			// cast the message to the extended interface that has message tracing support
			messageWithDT := message.(OutboundMessageWithTracingSupport)

			baggageErr := messageWithDT.SetBaggage("") // set empty baggage
			Expect(baggageErr).To(BeNil())

			publisher.Publish(messageWithDT, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				baggage, ok := message.GetBaggage()
				Expect(ok).To(BeTrue())
				Expect(baggage).To(Equal(""))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

		It("should be able to publish/receive a message with a valid baggage", func() {
			baggage := "baggage1=payload1"
			message, err := messageBuilder.Build()
			Expect(err).ToNot(HaveOccurred())

			// cast the message to the extended interface that has message tracing support
			messageWithDT := message.(OutboundMessageWithTracingSupport)

			baggageErr := messageWithDT.SetBaggage(baggage) // set a valid baggage string
			Expect(baggageErr).To(BeNil())

			publisher.Publish(messageWithDT, resource.TopicOf(topic))

			select {
			case message := <-inboundMessageChannel:
				receivedBaggage, ok := message.GetBaggage()
				Expect(ok).To(BeTrue())
				Expect(receivedBaggage).To(Equal(baggage))
			case <-time.After(1 * time.Second):
				Fail("timed out waiting for message to be delivered")
			}
		})

	})

	Describe("Payload Compression Tests", func() {
		// [X] Test to validate range of level is from 0 to 9 (inclusive) - test part of messaging_service tests
		// [X] Test that payload is still nil after compression level is disabled (set to 0 ) when no payload
		// [X] Test to check that payload is uncompressed when level set to 0
		// [X] Test to check that payload is uncompressed when payload compression is off  (when compression level is not set)
		// [X] Test that payload is still nil after compression when no payload set
		// [X] Test that payload is Most compressed when level is high (like 7)

		Describe("Publish and receive outbound message when Payload Compression is disabled", func() {
			var publisher solace.DirectMessagePublisher
			var receiver solace.DirectMessageReceiver
			var inboundMessageChannel chan message.InboundMessage

			AfterEach(func() {
				var err error
				err = publisher.Terminate(10 * time.Second)
				Expect(err).ToNot(HaveOccurred())
				err = receiver.Terminate(10 * time.Second)
				Expect(err).ToNot(HaveOccurred())

				err = messagingService.Disconnect()
				Expect(err).ToNot(HaveOccurred())
			})

			// initialize and start the publisher/receiver
			ConnectMessagingServiceAndInitialize := func() {
				var err error
				err = messagingService.Connect()
				Expect(err).ToNot(HaveOccurred())

				publisher, err = messagingService.CreateDirectMessagePublisherBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				receiver, err = messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(resource.TopicSubscriptionOf(topic)).Build()
				Expect(err).ToNot(HaveOccurred())

				err = publisher.Start()
				Expect(err).ToNot(HaveOccurred())

				inboundMessageChannel = make(chan message.InboundMessage)
				receiver.ReceiveAsync(func(inboundMessage message.InboundMessage) {
					inboundMessageChannel <- inboundMessage
				})

				err = receiver.Start()
				Expect(err).ToNot(HaveOccurred())
			}

			// Do not set any compression level
			WithPayloadCompressionLevelNotSet := func() {
				builder := messaging.NewMessagingServiceBuilder().
					FromConfigurationProvider(helpers.DefaultConfiguration())
				var err error
				messagingService, err = builder.Build()
				Expect(err).ToNot(HaveOccurred())
				messageBuilder = messagingService.MessageBuilder()
				ConnectMessagingServiceAndInitialize()
			}

			// Set the compression level to zero
			WithPayloadCompressionLevelSetToZero := func() {
				builder := messaging.NewMessagingServiceBuilder().
					FromConfigurationProvider(helpers.DefaultConfiguration()).
					FromConfigurationProvider(config.ServicePropertyMap{
						config.ServicePropertyPayloadCompressionLevel: 0, // disable payload compression (set level to zero)
					})
				var err error
				messagingService, err = builder.Build()
				Expect(err).ToNot(HaveOccurred())
				messageBuilder = messagingService.MessageBuilder()
				ConnectMessagingServiceAndInitialize()
			}

			// Test that payload is still nil after compression level is disabled (set to 0 ) when no payload
			It("payload size should remain the same after publish/receive with no payload", func() {
				WithPayloadCompressionLevelSetToZero() // initialize and start the publisher/receiver - set the compression level to zero
				message, err := messageBuilder.Build()
				Expect(err).ToNot(HaveOccurred())

				publishMessageBytes, _ := message.GetPayloadAsBytes()
				publishMessageBytesSize := len(publishMessageBytes)
				publisher.Publish(message, resource.TopicOf(topic))

				select {
				case message := <-inboundMessageChannel:
					content, ok := message.GetPayloadAsString()
					Expect(ok).To(BeFalse())
					Expect(content).To(Equal(""))
					receivedMessageBytes, ok := message.GetPayloadAsBytes()
					Expect(ok).To(BeFalse())
					Expect(receivedMessageBytes).To(BeNil())
					// check the published message via semp
					client := helpers.GetClient(messagingService)
					Expect(client.DataTxMsgCount).To(Equal(int64(1)))                                  // should be only one message published
					Expect(client.DataRxMsgCount).To(Equal(int64(1)))                                  // should be only one message received
					Expect(publishMessageBytesSize).To(BeNumerically("==", len(receivedMessageBytes))) // received should be equal which is zero
					Expect(client.DataTxByteCount).To(BeNumerically("==", client.DataRxByteCount))     // Tx and Rx data on wire should be the same (no message corruption)
					Expect(client.DataTxByteCount).To(BeNumerically(">", len(receivedMessageBytes)))   // Tx data on wire should be larger than message without payload (only headers)

				case <-time.After(1 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})

			// Test to check that payload is uncompressed when compression level is set to 0
			It("payload size should remain the same after publish/receive with payload and compression level set to zero", func() {
				WithPayloadCompressionLevelSetToZero() // initialize and start the publisher/receiver - set the compression level to zero
				largeByteArray := make([]byte, 16384)
				message, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
				Expect(err).ToNot(HaveOccurred())

				publishMessageBytes, _ := message.GetPayloadAsBytes()
				publishMessageBytesSize := len(publishMessageBytes)
				publisher.Publish(message, resource.TopicOf(topic))

				select {
				case message := <-inboundMessageChannel:
					content, ok := message.GetPayloadAsString()
					Expect(ok).To(BeFalse())
					Expect(content).To(Equal(""))
					receivedMessageBytes, ok := message.GetPayloadAsBytes()
					Expect(ok).To(BeTrue())
					Expect(receivedMessageBytes).ToNot(BeNil())
					// check the published message via semp
					client := helpers.GetClient(messagingService)
					Expect(client.DataTxMsgCount).To(Equal(int64(1)))                                  // should be only one message published
					Expect(client.DataRxMsgCount).To(Equal(int64(1)))                                  // should be only one message received
					Expect(publishMessageBytesSize).To(BeNumerically("==", len(receivedMessageBytes))) // bytes received should be equal to published
					Expect(client.DataTxByteCount).To(BeNumerically("==", client.DataRxByteCount))     // Tx and Rx data on wire should be the same (no message corruption)
					Expect(client.DataTxByteCount).To(BeNumerically(">", len(receivedMessageBytes)))   // Tx data on wire (payload + headers) should be larger than message with payload (only headers)

				case <-time.After(1 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})

			// Test to check that payload is uncompressed when payload compression is off  (when compression level is not set)
			It("payload size should remain the same after publish/receive with payload and compression level not set", func() {
				WithPayloadCompressionLevelNotSet() // initialize and start the publisher/receiver - do not set any compression level
				largeByteArray := make([]byte, 16384)
				message, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
				Expect(err).ToNot(HaveOccurred())

				publishMessageBytes, _ := message.GetPayloadAsBytes()
				publishMessageBytesSize := len(publishMessageBytes)
				publisher.Publish(message, resource.TopicOf(topic))

				select {
				case message := <-inboundMessageChannel:
					content, ok := message.GetPayloadAsString()
					Expect(ok).To(BeFalse())
					Expect(content).To(Equal(""))
					receivedMessageBytes, ok := message.GetPayloadAsBytes()
					Expect(ok).To(BeTrue())
					Expect(receivedMessageBytes).ToNot(BeNil())
					// check the published message via semp
					client := helpers.GetClient(messagingService)
					Expect(client.DataTxMsgCount).To(Equal(int64(1)))                                  // should be only one message published
					Expect(client.DataRxMsgCount).To(Equal(int64(1)))                                  // should be only one message received
					Expect(publishMessageBytesSize).To(BeNumerically("==", len(receivedMessageBytes))) // bytes received should be equal to published
					Expect(client.DataTxByteCount).To(BeNumerically("==", client.DataRxByteCount))     // Tx and Rx data on wire should be the same (no message corruption)
					Expect(client.DataTxByteCount).To(BeNumerically(">", len(receivedMessageBytes)))   // Tx data on wire (payload + headers) should be larger than message with payload (only headers)

				case <-time.After(1 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})

		})

		Describe("Publish and receive outbound message with valid Payload Compression enabled", func() {
			var publisher solace.DirectMessagePublisher
			var receiver solace.DirectMessageReceiver
			var inboundMessageChannel chan message.InboundMessage

			BeforeEach(func() {

				builder := messaging.NewMessagingServiceBuilder().
					FromConfigurationProvider(helpers.DefaultConfiguration()).
					FromConfigurationProvider(config.ServicePropertyMap{
						config.ServicePropertyPayloadCompressionLevel: 7, // high compression level
					})
				var err error
				messagingService, err = builder.Build()
				Expect(err).ToNot(HaveOccurred())
				messageBuilder = messagingService.MessageBuilder()

				err = messagingService.Connect()
				Expect(err).ToNot(HaveOccurred())

				publisher, err = messagingService.CreateDirectMessagePublisherBuilder().Build()
				Expect(err).ToNot(HaveOccurred())
				receiver, err = messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(resource.TopicSubscriptionOf(topic)).Build()
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

			// Test that payload is still nil after compression when no payload set
			It("payload size should remain the same after publish/receive with no payload", func() {
				message, err := messageBuilder.Build()
				Expect(err).ToNot(HaveOccurred())

				publishMessageBytes, _ := message.GetPayloadAsBytes()
				publishMessageBytesSize := len(publishMessageBytes)
				publisher.Publish(message, resource.TopicOf(topic))

				select {
				case message := <-inboundMessageChannel:
					content, ok := message.GetPayloadAsString()
					Expect(ok).To(BeFalse())
					Expect(content).To(Equal(""))
					receivedMessageBytes, ok := message.GetPayloadAsBytes()
					Expect(ok).To(BeFalse())
					Expect(receivedMessageBytes).To(BeNil())
					// check the published message via semp
					client := helpers.GetClient(messagingService)
					Expect(client.DataTxMsgCount).To(Equal(int64(1)))                                  // should be only one message published
					Expect(client.DataRxMsgCount).To(Equal(int64(1)))                                  // should be only one message received
					Expect(publishMessageBytesSize).To(BeNumerically("==", len(receivedMessageBytes))) // received should be equal which is zero
					Expect(client.DataTxByteCount).To(BeNumerically("==", client.DataRxByteCount))     // Tx and Rx data on wire should be the same (no message corruption)
					Expect(client.DataTxByteCount).To(BeNumerically(">", len(receivedMessageBytes)))   // Tx data on wire should be larger than message without payload (only headers)

				case <-time.After(1 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})

			// Test that string payload is compressed when level is set (set to 7)
			// String compression performs worse when the letter frequency of repeating characters is very small
			It("payload should be properly compressed/decompressed after publish/receive with a string payload", func() {
				payload := strings.Repeat("hello world", 200)
				message, err := messageBuilder.BuildWithStringPayload(payload)
				Expect(err).ToNot(HaveOccurred())

				publishMessagePayload, _ := message.GetPayloadAsString()
				publishMessagePayloadSize := len(publishMessagePayload)
				publisher.Publish(message, resource.TopicOf(topic))

				select {
				case message := <-inboundMessageChannel:
					content, ok := message.GetPayloadAsString()
					Expect(ok).To(BeTrue())
					Expect(content).ToNot(BeNil())
					msgBytes, _ := message.GetPayloadAsBytes()
					Expect(msgBytes).ToNot(BeNil())
					// check the published message via semp
					client := helpers.GetClient(messagingService)
					Expect(client.DataTxMsgCount).To(Equal(int64(1)))                              // should be only one message published
					Expect(client.DataRxMsgCount).To(Equal(int64(1)))                              // should be only one message received
					Expect(publishMessagePayloadSize).To(BeNumerically("==", len(content)))        // bytes received should be equal to published
					Expect(client.DataTxByteCount).To(BeNumerically("==", client.DataRxByteCount)) // Tx and Rx data on wire should be the same (no message corruption)
					Expect(client.DataTxByteCount).To(BeNumerically("<", len(msgBytes)))           // Tx data on wire should be Smaller than message when payload is compressed

				case <-time.After(1 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})

			// Test that byte array payload is compressed when level is set (set to 7)
			It("payload should be properly compressed/decompressed after publish/receive with a byte array payload", func() {
				largeByteArray := make([]byte, 16384)
				message, err := messageBuilder.BuildWithByteArrayPayload(largeByteArray)
				Expect(err).ToNot(HaveOccurred())

				publishMessageBytes, _ := message.GetPayloadAsBytes()
				publishMessageBytesSize := len(publishMessageBytes)
				publisher.Publish(message, resource.TopicOf(topic))

				select {
				case message := <-inboundMessageChannel:
					content, ok := message.GetPayloadAsString()
					Expect(ok).To(BeFalse())
					Expect(content).To(Equal(""))
					receivedMessageBytes, ok := message.GetPayloadAsBytes()
					Expect(ok).To(BeTrue())
					Expect(receivedMessageBytes).ToNot(BeNil())
					// check the published message via semp
					client := helpers.GetClient(messagingService)
					Expect(client.DataTxMsgCount).To(Equal(int64(1)))                                  // should be only one message published
					Expect(client.DataRxMsgCount).To(Equal(int64(1)))                                  // should be only one message received
					Expect(publishMessageBytesSize).To(BeNumerically("==", len(receivedMessageBytes))) // bytes received should be equal to published
					Expect(client.DataTxByteCount).To(BeNumerically("==", client.DataRxByteCount))     // Tx and Rx data on wire should be the same (no message corruption)
					Expect(client.DataTxByteCount).To(BeNumerically("<", len(receivedMessageBytes)))   // Tx data on wire should be Smaller than message when payload is compressed

				case <-time.After(1 * time.Second):
					Fail("timed out waiting for message to be delivered")
				}
			})
		})

	})

})
