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
	"strconv"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/metrics"
	"solace.dev/go/messaging/pkg/solace/resource"

	//"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	sempconfig "solace.dev/go/messaging/test/sempclient/config"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const PQLabel string = "partition_queue"

var _ = Describe("Partitioned Queue Tests", func() {
	var queueName string = "partitioned_queue_test"
	var topicName string = "partitioned_queue_topic_test"
	var rebalanceDelay int64 = 15
	var partitionCount int32 = 3
	Context("queue has three partitions and rebalance delay of 15 seconds", func() {
		BeforeEach(func() {
			helpers.CreatePartitionedQueue(queueName, partitionCount, rebalanceDelay, topicName)
		})

		AfterEach(func() {
			helpers.DeleteQueue(queueName)
		})

		It("should have at least one key assigned to each partition and same keyed messages go to same partition", Label(PQLabel), func() {
			var messagingServices [4]solace.MessagingService
			var partitionKeys [9]string
			var rebalanceDelayDuration = time.Duration(rebalanceDelay) * 2

			//generate partition keys
			for i := 0; i < 9; i++ {
				partitionKeys[i] = "key_" + strconv.Itoa(i)
			}

			for i := 0; i < 4; i++ {
				messagingServices[i] = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration()))
				helpers.ConnectMessagingService(messagingServices[i])
			}

			defer func() {
				for i := 0; i < 4; i++ {
					helpers.DisconnectMessagingService(messagingServices[i])
				}
			}()

			partitionedQueue := resource.QueueDurableNonExclusive(queueName)
			publisher := helpers.NewPersistentPublisher(messagingServices[0])

			receiverOne, _ := messagingServices[1].CreatePersistentMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicName)).Build(partitionedQueue)

			receiverTwo, _ := messagingServices[2].CreatePersistentMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicName)).Build(partitionedQueue)

			receiverThree, _ := messagingServices[3].CreatePersistentMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicName)).Build(partitionedQueue)

			publisher.Start()
			receiverOne.Start()
			receiverTwo.Start()
			receiverThree.Start()

			messageBuilder := messagingServices[0].MessageBuilder()
			for i := 0; i < 18; i++ {
				msg, _ := messageBuilder.WithProperty(config.MessageProperty(config.QueuePartitionKey), partitionKeys[i%9]).BuildWithStringPayload("Hi Solace")
				publisher.Publish(msg, resource.TopicOf(topicName), nil, nil)
			}

			publisher.Terminate(rebalanceDelayDuration * time.Second)

			messageHandler := func(message message.InboundMessage) {
				fmt.Println("message received")
			}

			receiverOne.ReceiveAsync(messageHandler)
			receiverTwo.ReceiveAsync(messageHandler)
			receiverThree.ReceiveAsync(messageHandler)

			publisherMetrics := messagingServices[0].Metrics()
			receiverOneMetrics := messagingServices[1].Metrics()
			receiverTwoMetrics := messagingServices[2].Metrics()
			receiverThreeMetrics := messagingServices[3].Metrics()

			Eventually(func() uint64 {
				return receiverOneMetrics.GetValue(metrics.PersistentMessagesReceived)
			}).WithTimeout(rebalanceDelayDuration * time.Second).Should(BeNumerically(">=", 2))

			Eventually(func() uint64 {
				return receiverTwoMetrics.GetValue(metrics.PersistentMessagesReceived)
			}).WithTimeout(rebalanceDelayDuration * time.Second).Should(BeNumerically(">=", 2))

			Eventually(func() uint64 {
				return receiverThreeMetrics.GetValue(metrics.PersistentMessagesReceived)
			}).WithTimeout(rebalanceDelayDuration * time.Second).Should(BeNumerically(">=", 2))

			Eventually(func() uint64 {
				totalMessagesReceived := receiverOneMetrics.
					GetValue(metrics.PersistentMessagesReceived) + receiverTwoMetrics.GetValue(metrics.PersistentMessagesReceived) + receiverThreeMetrics.GetValue(metrics.PersistentMessagesReceived)
				return totalMessagesReceived
			}).WithTimeout(rebalanceDelayDuration * time.Second).Should(Equal(publisherMetrics.GetValue(metrics.TotalMessagesSent)))

			Expect(receiverOne.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			Expect(receiverTwo.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			Expect(receiverThree.Terminate(10 * time.Second)).ToNot(HaveOccurred())
		})

		It("generates flow inactive event when no partitions left for consumer to bind", Label(PQLabel), func() {

			var listenerOne solace.ReceiverStateChangeListener
			var listenerTwo solace.ReceiverStateChangeListener
			var listenerThree solace.ReceiverStateChangeListener

			var rebalanceDelayDuration = time.Duration(rebalanceDelay) * 2

			var messagingServices [3]solace.MessagingService

			partitionedQueue := resource.QueueDurableNonExclusive(queueName)

			for i := 0; i < 3; i++ {
				messagingServices[i] = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().
					FromConfigurationProvider(helpers.DefaultConfiguration()))
				helpers.ConnectMessagingService(messagingServices[i])
			}

			defer func() {
				for i := 0; i < 3; i++ {
					helpers.DisconnectMessagingService(messagingServices[i])
				}
			}()
			//activeStateTransitions refer to start-up induced state changes
			//(i.e., receiver transition from passive to active whereas passive transitions are induced on partition downscale
			activeStateTransitions, passiveStateTransitions := 0, 0
			ch := make(chan struct{})

			passiveTransitionIncrementor := func(oldState, newState solace.ReceiverState, timestamp time.Time) {
				if oldState == solace.ReceiverActive && newState == solace.ReceiverPassive {
					passiveStateTransitions++
				} else {
					activeStateTransitions++
				}

				if passiveStateTransitions == 2 && activeStateTransitions == 3 {
					close(ch)
				}
			}

			listenerOne, listenerTwo, listenerThree = passiveTransitionIncrementor, passiveTransitionIncrementor, passiveTransitionIncrementor

			receiverOne, _ := messagingServices[0].CreatePersistentMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicName)).WithActivationPassivationSupport(listenerOne).Build(partitionedQueue)

			receiverTwo, _ := messagingServices[1].CreatePersistentMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicName)).WithActivationPassivationSupport(listenerTwo).Build(partitionedQueue)

			receiverThree, _ := messagingServices[2].CreatePersistentMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicName)).WithActivationPassivationSupport(listenerThree).Build(partitionedQueue)

			Expect(receiverOne.Start()).ToNot(HaveOccurred())
			Expect(receiverTwo.Start()).ToNot(HaveOccurred())
			Expect(receiverThree.Start()).ToNot(HaveOccurred())

			time.Sleep(rebalanceDelayDuration * time.Second)

			testcontext.SEMP().Config().QueueApi.UpdateMsgVpnQueue(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpnQueue{
					PartitionCount: 1,
				},
				testcontext.Messaging().VPN,
				queueName,
				nil,
			)

			Eventually(ch).WithTimeout(rebalanceDelayDuration * time.Second).Should(BeClosed())

			Expect(receiverOne.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			Expect(receiverTwo.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			Expect(receiverThree.Terminate(10 * time.Second)).ToNot(HaveOccurred())
		})
		It("rebinds to same partition after reconnect within rebalance delay", Label(PQLabel), func() {
			var connectionRetries uint = 5
			var intervalDurationSec = time.Duration(2)
			var reconnectDurationTimoutSec = time.Duration(connectionRetries) * intervalDurationSec * 2 // should be about 10 secs
			rebalanceDelayDuration := time.Duration(rebalanceDelay) * 2                                 // should be about 20 secs
			var messagingServices [3]solace.MessagingService
			for i := 0; i < 3; i++ {
				messagingServices[i] = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().
					FromConfigurationProvider(helpers.DefaultConfiguration()).
					WithReconnectionRetryStrategy(config.
						RetryStrategyParameterizedRetry(connectionRetries, intervalDurationSec*time.Second)))

				helpers.ConnectMessagingService(messagingServices[i])
			}

			defer func() {
				for i := 0; i < 3; i++ {
					helpers.DisconnectMessagingService(messagingServices[i])
				}
			}()

			var partitionKeys [9]string
			for i := 0; i < 9; i++ {
				partitionKeys[i] = "key_" + strconv.Itoa(i)
			}

			publisher := helpers.NewPersistentPublisher(messagingServices[0])
			messageBuilder := messagingServices[0].MessageBuilder()
			publisher.Start()

			publisherMetrics := messagingServices[0].Metrics()
			publishMessages := func(firstConnectionAttempt bool) {
				for i := 0; i < 18; i++ {
					msg, _ := messageBuilder.WithProperty(config.MessageProperty(config.QueuePartitionKey), partitionKeys[i%9]).BuildWithStringPayload("Hi Solace")
					publisher.Publish(msg, resource.TopicOf(topicName), nil, nil)
				}

				if firstConnectionAttempt {
					Eventually(func() uint64 {
						return publisherMetrics.GetValue(metrics.TotalMessagesSent)
					}).WithTimeout(rebalanceDelayDuration * time.Second).Should(BeNumerically("==", 18))
				} else {
					Eventually(func() uint64 {
						return publisherMetrics.GetValue(metrics.TotalMessagesSent)
					}).WithTimeout(rebalanceDelayDuration * time.Second).Should(BeNumerically("==", 36))
				}
			}

			publishMessages(true)

			partitionedQueue := resource.QueueDurableNonExclusive(queueName)
			receiverOne, _ := messagingServices[1].CreatePersistentMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicName)).Build(partitionedQueue)
			var receiverOneMessageCount uint64 = 0
			var receiverTwoMessageCount uint64 = 0

			receiverOnePartitionKeys := make([]string, 0, 18)
			receiverOneMessageHandler := func(message message.InboundMessage) {
				// get user property "JMSXGroupID" for partition key
				if partitionKey, present := message.GetProperty(config.QueuePartitionKey); present {
					partitionKeyValue := partitionKey.(string)
					receiverOnePartitionKeys = append(receiverOnePartitionKeys, partitionKeyValue)
				}
				// must count dispatched message after PKey is recorded to avoid racing with main go routine
				receiverOneMessageCount += 1
			}

			receiverTwo, _ := messagingServices[2].CreatePersistentMessageReceiverBuilder().
				WithSubscriptions(resource.TopicSubscriptionOf(topicName)).Build(partitionedQueue)
			receiverTwoMessageHandler := func(message message.InboundMessage) {
				//fmt.Println("Received message in receiverTwo")
				// count the received messages dispatched for the receiver
				receiverTwoMessageCount += 1
			}

			receiverOne.ReceiveAsync(receiverOneMessageHandler)
			receiverTwo.ReceiveAsync(receiverTwoMessageHandler)

			receiverOne.Start()
			receiverTwo.Start()

			Eventually(func() uint64 {
				totalMessagesReceived := receiverOneMessageCount + receiverTwoMessageCount
				return totalMessagesReceived
			}).WithTimeout(rebalanceDelayDuration * time.Second).Should(Equal(publisherMetrics.GetValue(metrics.TotalMessagesSent)))

			numPartitionKeys := len(receiverOnePartitionKeys)
			partitionKeysBeforeDisconnect := make([]string, numPartitionKeys)
			copy(receiverOnePartitionKeys, partitionKeysBeforeDisconnect)
			receiverOnePartitionKeys = receiverOnePartitionKeys[:0]

			reconnectionListenerChan := make(chan struct{})
			messagingServices[2].AddReconnectionListener(func(even solace.ServiceEvent) {
				close(reconnectionListenerChan)
			})

			reconnectAttemptListenerChan := make(chan struct{})
			messagingServices[2].AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
				close(reconnectAttemptListenerChan)
			})

			//temporarily disconnect receiverTwo
			helpers.ForceDisconnectViaSEMPv2(messagingServices[2])

			Eventually(reconnectionListenerChan).WithTimeout(reconnectDurationTimoutSec * time.Second).Should(BeClosed())

			//republish messages
			publishMessages(false)

			Eventually(func() uint64 {
				totalMessagesReceived := receiverOneMessageCount + receiverTwoMessageCount
				return totalMessagesReceived
			}).WithTimeout(rebalanceDelayDuration * time.Second).Should(BeNumerically(">=", publisherMetrics.GetValue(metrics.TotalMessagesSent)))

			partitionKeysAfterReconnection := make([]string, numPartitionKeys)
			copy(receiverOnePartitionKeys, partitionKeysAfterReconnection)

			Expect(partitionKeysBeforeDisconnect).Should(Equal(partitionKeysAfterReconnection))
			Expect(receiverOne.Terminate(10 * time.Second)).ToNot(HaveOccurred())
			Expect(receiverTwo.Terminate(10 * time.Second)).ToNot(HaveOccurred())
		})
	})
})
