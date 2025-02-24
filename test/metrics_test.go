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
	"fmt"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metrics", func() {
	var builder solace.MessagingServiceBuilder

	BeforeEach(func() {
		builder = messaging.NewMessagingServiceBuilder().
			FromConfigurationProvider(helpers.DefaultConfiguration())
	})

	Context("with a default messaging service", func() {
		var messagingService solace.MessagingService
		BeforeEach(func() {
			messagingService = helpers.BuildMessagingService(builder)
		})

		It("should be able to retrieve metrics without starting the messaging service", func() {
			Expect(messagingService.Metrics().GetValue(metrics.ConnectionAttempts)).To(BeNumerically("==", 0))
		})

		It("should be able to reset metrics without starting the messaging service", func() {
			messagingService.Metrics().Reset()
			Expect(messagingService.Metrics().GetValue(metrics.ConnectionAttempts)).To(BeNumerically("==", 0))
		})

		It("should be able to retrieve a nonexistent stat", func() {
			m := messagingService.Metrics().GetValue(metrics.Metric(metrics.MetricCount))
			Expect(m).To(BeNumerically("==", 0))
			m = messagingService.Metrics().GetValue(metrics.Metric(1000 * metrics.MetricCount))
			Expect(m).To(BeNumerically("==", 0))
		})

		It("should be able to print metrics to a string", func() {
			metrics := messagingService.Metrics()
			Expect(fmt.Sprint(metrics)).To(ContainSubstring(fmt.Sprintf("%p", metrics)))
		})

		Context("with a connected messaging service", func() {
			const topic = "metrics-tests"

			BeforeEach(func() {
				helpers.ConnectMessagingService(messagingService)
			})
			AfterEach(func() {
				if messagingService.IsConnected() {
					messagingService.Disconnect()
				}
			})

			It("can reset metrics", func() {
				helpers.PublishOneMessage(messagingService, "metrics/test")
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				}).Should(BeNumerically("==", 1))
				messagingService.Metrics().Reset()
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", 0))
			})

			It("can retrieve metrics after terminating the service", func() {
				const topic = "metrics/test"
				msgChan := helpers.ReceiveOneMessage(messagingService, topic)
				helpers.PublishOneMessage(messagingService, topic)
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				}).Should(BeNumerically("==", 1))
				Eventually(msgChan).Should(Receive())
				messagingService.Disconnect()
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", 1))
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(BeNumerically("==", 1))
			})

			It("can reset metrics after terminating the service", func() {
				helpers.PublishOneMessage(messagingService, "metrics/test")
				Eventually(func() uint64 {
					return messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				}).Should(BeNumerically("==", 1))
				messagingService.Disconnect()
				messagingService.Metrics().Reset()
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", 0))
			})

			It("increments the control stats when subscribing", func() {
				initialSent := messagingService.Metrics().GetValue(metrics.ControlBytesSent)
				initialSentMsgs := messagingService.Metrics().GetValue(metrics.ControlMessagesSent)
				initialReceived := messagingService.Metrics().GetValue(metrics.ControlBytesReceived)
				initialReceivedMsgs := messagingService.Metrics().GetValue(metrics.ControlMessagesReceived)
				receiver, err := messagingService.CreateDirectMessageReceiverBuilder().WithSubscriptions(resource.TopicSubscriptionOf("some subscription")).Build()
				Expect(err).ToNot(HaveOccurred())
				Expect(receiver.Start()).ToNot(HaveOccurred())
				Expect(receiver.Terminate(10 * time.Second)).ToNot(HaveOccurred())
				Expect(messagingService.Metrics().GetValue(metrics.ControlBytesSent)).To(BeNumerically(">", initialSent))
				Expect(messagingService.Metrics().GetValue(metrics.ControlMessagesSent)).To(BeNumerically(">", initialSentMsgs))
				Expect(messagingService.Metrics().GetValue(metrics.ControlBytesReceived)).To(BeNumerically(">", initialReceived))
				Expect(messagingService.Metrics().GetValue(metrics.ControlMessagesReceived)).To(BeNumerically(">", initialReceivedMsgs))
			})

			It("increments the direct message stats when publishing and subscribing", func() {
				Expect(messagingService.Metrics().GetValue(metrics.DirectBytesSent)).To(BeNumerically("==", 0))
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesSent)).To(BeNumerically("==", 0))
				Expect(messagingService.Metrics().GetValue(metrics.DirectBytesReceived)).To(BeNumerically("==", 0))
				Expect(messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)).To(BeNumerically("==", 0))
				messageChannel := helpers.ReceiveOneMessage(messagingService, topic)
				helpers.PublishOneMessage(messagingService, topic)
				// Wait for the message to arrive
				Eventually(messageChannel).Should(Receive())
				newSent := messagingService.Metrics().GetValue(metrics.DirectBytesSent)
				newSentMsgs := messagingService.Metrics().GetValue(metrics.DirectMessagesSent)
				newReceived := messagingService.Metrics().GetValue(metrics.DirectBytesReceived)
				newReceivedMsgs := messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)
				Expect(newSent).To(BeNumerically(">", 0))
				Expect(newSentMsgs).To(BeNumerically(">", 0))
				Expect(newReceived).To(BeNumerically("==", newSent))
				Expect(newReceivedMsgs).To(BeNumerically("==", newSentMsgs))
			})

			It("increments the total stats when subscribing, publishing and receiving", func() {
				Expect(messagingService.Metrics().GetValue(metrics.TotalBytesSent)).To(BeNumerically("==", 0))
				Expect(messagingService.Metrics().GetValue(metrics.TotalMessagesSent)).To(BeNumerically("==", 0))
				Expect(messagingService.Metrics().GetValue(metrics.TotalBytesReceived)).To(BeNumerically("==", 0))
				Expect(messagingService.Metrics().GetValue(metrics.TotalMessagesReceived)).To(BeNumerically("==", 0))

				messageChannel := helpers.ReceiveOneMessage(messagingService, topic)
				helpers.PublishOneMessage(messagingService, topic)
				// Wait for the message to arrive
				Eventually(messageChannel).Should(Receive())

				Expect(messagingService.Metrics().GetValue(metrics.TotalBytesSent)).To(BeNumerically(">", 0))
				Expect(messagingService.Metrics().GetValue(metrics.TotalMessagesSent)).To(BeNumerically(">", 0))
				Expect(messagingService.Metrics().GetValue(metrics.TotalBytesReceived)).To(BeNumerically(">", 0))
				Expect(messagingService.Metrics().GetValue(metrics.TotalMessagesReceived)).To(BeNumerically(">", 0))
				// TODO update this test with persistent metrics when they are added
				Expect(messagingService.Metrics().GetValue(metrics.TotalBytesSent)).To(BeNumerically("==", messagingService.Metrics().GetValue(metrics.DirectBytesSent)+messagingService.Metrics().GetValue(metrics.PersistentBytesSent)))
				Expect(messagingService.Metrics().GetValue(metrics.TotalMessagesSent)).To(BeNumerically("==", messagingService.Metrics().GetValue(metrics.DirectMessagesSent)+messagingService.Metrics().GetValue(metrics.PersistentMessagesSent)))
				Expect(messagingService.Metrics().GetValue(metrics.TotalBytesReceived)).To(BeNumerically("==", messagingService.Metrics().GetValue(metrics.DirectBytesReceived)+messagingService.Metrics().GetValue(metrics.PersistentBytesReceived)))
				Expect(messagingService.Metrics().GetValue(metrics.TotalMessagesReceived)).To(BeNumerically("==", messagingService.Metrics().GetValue(metrics.DirectMessagesReceived)+messagingService.Metrics().GetValue(metrics.PersistentMessagesReceived)))
			})

			// TODO test persistent metrics
		})
	})

	Context("with a connected plaintext messaging service", func() {
		var messagingService solace.MessagingService
		BeforeEach(func() {
			plaintextURL := fmt.Sprintf("%s:%d", testcontext.Messaging().Host, testcontext.Messaging().MessagingPorts.PlaintextPort)
			messagingService = helpers.BuildMessagingService(builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.TransportLayerPropertyHost: plaintextURL,
			}))
			helpers.ConnectMessagingService(messagingService)
		})
		AfterEach(func() {
			if messagingService.IsConnected() {
				messagingService.Disconnect()
			}
		})
		It("does not increment the compressed bytes stat", func() {
			topic := "metricstestcompressedreceived"
			messageChannel := helpers.ReceiveOneMessage(messagingService, topic)
			helpers.PublishOneMessage(messagingService, topic)
			// Wait for the message to arrive
			Eventually(messageChannel).Should(Receive())
			Expect(messagingService.Metrics().GetValue(metrics.CompressedBytesReceived)).To(BeNumerically("==", 0))
		})
	})

	Context("with a connected compressed messaging service", func() {
		var messagingService solace.MessagingService
		BeforeEach(func() {
			compressedURL := fmt.Sprintf("%s:%d", testcontext.Messaging().Host, testcontext.Messaging().MessagingPorts.CompressedPort)
			messagingService = helpers.BuildMessagingService(builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.TransportLayerPropertyHost: compressedURL,
			}).WithMessageCompression(1))
			helpers.ConnectMessagingService(messagingService)
		})
		AfterEach(func() {
			if messagingService.IsConnected() {
				messagingService.Disconnect()
			}
		})
		It("increments the compressed bytes stat", func() {
			topic := "metricstestcompressedreceived"
			messageChannel := helpers.ReceiveOneMessage(messagingService, topic)
			helpers.PublishOneMessage(messagingService, topic)
			// Wait for the message to arrive
			Eventually(messageChannel).Should(Receive())
			Expect(messagingService.Metrics().GetValue(metrics.CompressedBytesReceived)).To(BeNumerically(">", 0))
		})
	})

	Context("with a toxiproxy messaging service", func() {
		const connectionRetries = 5
		var messagingService solace.MessagingService
		BeforeEach(func() {
			helpers.CheckToxiProxy()
			messagingService = helpers.BuildMessagingService(builder.FromConfigurationProvider(helpers.ToxicConfiguration()).FromConfigurationProvider(config.ServicePropertyMap{
				config.TransportLayerPropertyConnectionRetries:                connectionRetries,
				config.TransportLayerPropertyReconnectionAttemptsWaitInterval: 100, // 100 ms to make this test not super slow
			}))
		})
		AfterEach(func() {
			if messagingService.IsConnected() {
				messagingService.Disconnect()
			}
		})

		It("increments the connection attempts metric", func() {
			// We must do this before each test using toxiproxy
			Expect(messagingService.Metrics().GetValue(metrics.ConnectionAttempts)).To(BeNumerically("==", 0))
			Expect(testcontext.Toxi().SMF().Disable()).ToNot(HaveOccurred())
			defer func() {
				Expect(testcontext.Toxi().SMF().Enable()).ToNot(HaveOccurred())
			}()
			err := messagingService.Connect()
			Expect(err).To(HaveOccurred())
			// Connection attempts will be first attempt + num retries
			Expect(messagingService.Metrics().GetValue(metrics.ConnectionAttempts)).To(BeNumerically("==", connectionRetries+1))
		})
	})
})
