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
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"solace.dev/go/messaging/pkg/solace/config"
)

var _ = Describe("Configuration Maps", func() {

	var badJSON = []byte(`{invalid:json}`)

	Describe("Message Property Map", func() {
		var messageProperties = config.MessagePropertyMap{
			config.MessagePropertyApplicationMessageID:   float64(10),
			config.MessagePropertyApplicationMessageType: "default",
		}

		const messagePropertiesJSON = `{"solace":{"messaging":{"message":{"application-message-id":10,"application-message-type":"default"}}}}`

		It("can copy a set of properties", func() {
			copyOfProperties := messageProperties.GetConfiguration()
			Expect(copyOfProperties).To(Equal(messageProperties))
			copyOfProperties[config.MessagePropertyApplicationMessageType] = "some_other_value"
			Expect(messageProperties[config.MessagePropertyApplicationMessageType]).To(Equal("default"))
		})

		It("can unmarshal json into a map", func() {
			output := make(config.MessagePropertyMap)
			err := json.Unmarshal([]byte(messagePropertiesJSON), &output)
			Expect(err).ToNot(HaveOccurred())
			Expect(output).To(Equal(messageProperties))
		})

		It("can marshal map into json", func() {
			output, err := json.Marshal(messageProperties)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(output)).To(Equal(messagePropertiesJSON))
		})

		It("can unmarshal and marshal", func() {
			jsonOutput, err := json.Marshal(messageProperties)
			Expect(err).ToNot(HaveOccurred())
			mapOutput := make(config.MessagePropertyMap)
			json.Unmarshal([]byte(jsonOutput), &mapOutput)
			Expect(mapOutput).To(Equal(messageProperties))
		})

		It("fails to unmarshal bad JSON", func() {
			output := make(config.MessagePropertyMap)
			err := json.Unmarshal(badJSON, &output)
			Expect(err).To(HaveOccurred())
			Expect(output).To(BeEmpty())
		})

		It("fails to marshal bad keys", func() {
			output, err := json.Marshal(config.MessagePropertyMap{
				config.MessageProperty("some.key"):      "1",
				config.MessageProperty("some.key.leaf"): "2",
			})
			Expect(output).To(BeEmpty())
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Publisher Property Map", func() {
		var publisherProperties = config.PublisherPropertyMap{
			config.PublisherPropertyBackPressureBufferCapacity: float64(10),
			config.PublisherPropertyBackPressureStrategy:       "default",
		}

		const publisherPropertiesJSON = `{"solace":{"messaging":{"publisher":{"back-pressure":{"buffer-capacity":10,"strategy":"default"}}}}}`

		It("can copy a set of properties", func() {
			copyOfProperties := publisherProperties.GetConfiguration()
			Expect(copyOfProperties).To(Equal(publisherProperties))
			copyOfProperties[config.PublisherPropertyBackPressureStrategy] = "some_other_value"
			Expect(publisherProperties[config.PublisherPropertyBackPressureStrategy]).To(Equal("default"))
		})

		It("can unmarshal json into a map", func() {
			output := make(config.PublisherPropertyMap)
			err := json.Unmarshal([]byte(publisherPropertiesJSON), &output)
			Expect(err).ToNot(HaveOccurred())
			Expect(output).To(Equal(publisherProperties))
		})

		It("can marshal map into json", func() {
			output, err := json.Marshal(publisherProperties)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(output)).To(Equal(publisherPropertiesJSON))
		})

		It("can unmarshal and marshal", func() {
			jsonOutput, err := json.Marshal(publisherProperties)
			Expect(err).ToNot(HaveOccurred())
			mapOutput := make(config.PublisherPropertyMap)
			json.Unmarshal([]byte(jsonOutput), &mapOutput)
			Expect(mapOutput).To(Equal(publisherProperties))
		})

		It("fails to unmarshal bad JSON", func() {
			output := make(config.PublisherPropertyMap)
			err := json.Unmarshal(badJSON, &output)
			Expect(err).To(HaveOccurred())
			Expect(output).To(BeEmpty())
		})

		It("fails to marshal bad keys", func() {
			output, err := json.Marshal(config.PublisherPropertyMap{
				config.PublisherProperty("some.key"):      "1",
				config.PublisherProperty("some.key.leaf"): "2",
			})
			Expect(output).To(BeEmpty())
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Receiver Property Map", func() {
		var receiverProperties = config.ReceiverPropertyMap{
			config.ReceiverPropertyDirectBackPressureBufferCapacity: float64(10),
			config.ReceiverPropertyDirectBackPressureStrategy:       "default",
		}

		const receiverPropertiesJSON = `{"solace":{"messaging":{"receiver":{"direct":{"back-pressure":{"buffer-capacity":10,"strategy":"default"}}}}}}`

		It("can copy a set of properties", func() {
			copyOfProperties := receiverProperties.GetConfiguration()
			Expect(copyOfProperties).To(Equal(receiverProperties))
			copyOfProperties[config.ReceiverPropertyDirectBackPressureStrategy] = "some_other_value"
			Expect(receiverProperties[config.ReceiverPropertyDirectBackPressureStrategy]).To(Equal("default"))
		})

		It("can unmarshal json into a map", func() {
			output := make(config.ReceiverPropertyMap)
			err := json.Unmarshal([]byte(receiverPropertiesJSON), &output)
			Expect(err).ToNot(HaveOccurred())
			Expect(output).To(Equal(receiverProperties))
		})

		It("can marshal map into json", func() {
			output, err := json.Marshal(receiverProperties)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(output)).To(Equal(receiverPropertiesJSON))
		})

		It("can unmarshal and marshal", func() {
			jsonOutput, err := json.Marshal(receiverProperties)
			Expect(err).ToNot(HaveOccurred())
			mapOutput := make(config.ReceiverPropertyMap)
			json.Unmarshal([]byte(jsonOutput), &mapOutput)
			Expect(mapOutput).To(Equal(receiverProperties))
		})

		It("fails to unmarshal bad JSON", func() {
			output := make(config.ReceiverPropertyMap)
			err := json.Unmarshal(badJSON, &output)
			Expect(err).To(HaveOccurred())
			Expect(output).To(BeEmpty())
		})

		It("fails to marshal bad keys", func() {
			output, err := json.Marshal(config.ReceiverPropertyMap{
				config.ReceiverProperty("some.key"):      "1",
				config.ReceiverProperty("some.key.leaf"): "2",
			})
			Expect(output).To(BeEmpty())
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Service Property Map", func() {
		var serviceProperties = config.ServicePropertyMap{
			config.AuthenticationPropertyScheme:              "basic",
			config.AuthenticationPropertySchemeBasicPassword: "default",
			config.TransportLayerPropertyHost:                "localhost",
			config.TransportLayerPropertyKeepAliveInterval:   float64(5),
		}

		const servicePropertiesJSON = `{"solace":{"messaging":{"authentication":{"basic":{"password":"default"},"scheme":"basic"},"transport":{"host":"localhost","keep-alive-interval":5}}}}`

		It("can copy a set of properties", func() {
			copyOfProperties := serviceProperties.GetConfiguration()
			Expect(copyOfProperties).To(Equal(serviceProperties))
			copyOfProperties[config.AuthenticationPropertyScheme] = "some_other_value"
			Expect(serviceProperties[config.AuthenticationPropertyScheme]).To(Equal("basic"))
		})

		It("can unmarshal json into a map", func() {
			output := make(config.ServicePropertyMap)
			err := json.Unmarshal([]byte(servicePropertiesJSON), &output)
			Expect(err).ToNot(HaveOccurred())
			Expect(output).To(Equal(serviceProperties))
		})

		It("can marshal map into json", func() {
			output, err := json.Marshal(serviceProperties)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(output)).To(Equal(servicePropertiesJSON))
		})

		It("can unmarshal and marshal", func() {
			jsonOutput, err := json.Marshal(serviceProperties)
			Expect(err).ToNot(HaveOccurred())
			mapOutput := make(config.ServicePropertyMap)
			json.Unmarshal([]byte(jsonOutput), &mapOutput)
			Expect(mapOutput).To(Equal(serviceProperties))
		})

		It("fails to unmarshal bad JSON", func() {
			output := make(config.ServicePropertyMap)
			err := json.Unmarshal(badJSON, &output)
			Expect(err).To(HaveOccurred())
			Expect(output).To(BeEmpty())
		})

		It("fails to marshal bad keys", func() {
			output, err := json.Marshal(config.ServicePropertyMap{
				config.ServiceProperty("some.key"):      "1",
				config.ServiceProperty("some.key.leaf"): "2",
			})
			Expect(output).To(BeEmpty())
			Expect(err).To(HaveOccurred())
		})
	})
})
