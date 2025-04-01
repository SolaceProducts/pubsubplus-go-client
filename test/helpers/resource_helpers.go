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
	"strings"
	"time"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/resource"
	sempconfig "solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/sempclient/monitor"
	"solace.dev/go/messaging/test/testcontext"

	//lint:ignore ST1001 dot import is fine for tests
	. "github.com/onsi/gomega"
)

// CreateQueue function
func CreateQueue(queueName string, topics ...string) {
	_, _, err := testcontext.SEMP().Config().QueueApi.CreateMsgVpnQueue(testcontext.SEMP().ConfigCtx(), sempconfig.MsgVpnQueue{
		QueueName:      queueName,
		IngressEnabled: True,
		EgressEnabled:  True,
		Owner:          "default",
	}, testcontext.Messaging().VPN, nil)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to create queue with name "+queueName)
	for _, topic := range topics {
		_, _, err = testcontext.SEMP().Config().QueueApi.CreateMsgVpnQueueSubscription(testcontext.SEMP().ConfigCtx(),
			sempconfig.MsgVpnQueueSubscription{
				SubscriptionTopic: topic,
			}, testcontext.Messaging().VPN, queueName, nil)
		ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to add subscription for topic "+topic)
	}
}

// CreateNonExclusiveQueue function
func CreateNonExclusiveQueue(queueName string, topics ...string) {
	_, _, err := testcontext.SEMP().Config().QueueApi.CreateMsgVpnQueue(testcontext.SEMP().ConfigCtx(), sempconfig.MsgVpnQueue{
		QueueName:      queueName,
		AccessType:     "non-exclusive",
		IngressEnabled: True,
		EgressEnabled:  True,
		Owner:          "default",
	}, testcontext.Messaging().VPN, nil)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to create queue with name "+queueName)
	for _, topic := range topics {
		_, _, err = testcontext.SEMP().Config().QueueApi.CreateMsgVpnQueueSubscription(testcontext.SEMP().ConfigCtx(),
			sempconfig.MsgVpnQueueSubscription{
				SubscriptionTopic: topic,
			}, testcontext.Messaging().VPN, queueName, nil)
		ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to add subscription for topic "+topic)
	}
}

// CreatePartitionedQueue function
func CreatePartitionedQueue(queueName string, partitionCount int32, partitionRebalanceDelay int64, topics ...string) {
	_, _, err := testcontext.SEMP().Config().QueueApi.CreateMsgVpnQueue(testcontext.SEMP().ConfigCtx(), sempconfig.MsgVpnQueue{
		QueueName:               queueName,
		AccessType:              "non-exclusive",
		Permission:              "modify-topic",
		IngressEnabled:          True,
		EgressEnabled:           True,
		PartitionCount:          partitionCount,
		PartitionRebalanceDelay: partitionRebalanceDelay,
		Owner:                   "default",
	}, testcontext.Messaging().VPN, nil)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to create queue with name "+queueName)
	for _, topic := range topics {
		_, _, err = testcontext.SEMP().Config().QueueApi.CreateMsgVpnQueueSubscription(testcontext.SEMP().ConfigCtx(),
			sempconfig.MsgVpnQueueSubscription{
				SubscriptionTopic: topic,
			}, testcontext.Messaging().VPN, queueName, nil)
		ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to add subscription for topic "+topic)
	}
}

// CreateQueueSubscription function
func CreateQueueSubscription(queueName string, topic string) {
	_, _, err := testcontext.SEMP().Config().QueueApi.CreateMsgVpnQueueSubscription(testcontext.SEMP().ConfigCtx(),
		sempconfig.MsgVpnQueueSubscription{
			SubscriptionTopic: topic,
		}, testcontext.Messaging().VPN, queueName, nil)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to add subscription for topic "+topic)
}

// DeleteQueue function
func DeleteQueue(queueName string) {
	_, _, err := testcontext.SEMP().Config().QueueApi.DeleteMsgVpnQueue(testcontext.SEMP().ConfigCtx(),
		testcontext.Messaging().VPN, queueName)
	ExpectWithOffset(1, err).ToNot(HaveOccurred(), "Failed to delete queue with name "+queueName)
}

// GetQueueSubscriptions function
func GetQueueSubscriptions(queueName string) []string {
	response, _, err := testcontext.SEMP().Monitor().QueueApi.GetMsgVpnQueueSubscriptions(
		testcontext.SEMP().MonitorCtx(),
		testcontext.Messaging().VPN,
		queueName,
		nil,
	)
	Expect(err).ToNot(HaveOccurred())
	results := make([]string, len(response.Data))
	for i, mvqs := range response.Data {
		results[i] = mvqs.SubscriptionTopic
	}
	return results
}

// GetQueue function
func GetQueue(queueName string) *monitor.MsgVpnQueue {
	response, _, err := testcontext.SEMP().Monitor().QueueApi.GetMsgVpnQueue(
		testcontext.SEMP().MonitorCtx(),
		testcontext.Messaging().VPN,
		queueName,
		nil,
	)
	Expect(err).ToNot(HaveOccurred())
	return response.Data
}

// GetQueueMessages function
func GetQueueMessages(queueName string) []monitor.MsgVpnQueueMsg {
	response, _, err := testcontext.SEMP().Monitor().QueueApi.GetMsgVpnQueueMsgs(
		testcontext.SEMP().MonitorCtx(),
		testcontext.Messaging().VPN,
		queueName,
		nil,
	)
	Expect(err).ToNot(HaveOccurred())
	return response.Data
}

// GetQueues
func GetQueues() []monitor.MsgVpnQueue {
	response, _, err := testcontext.SEMP().Monitor().QueueApi.GetMsgVpnQueues(
		testcontext.SEMP().MonitorCtx(),
		testcontext.Messaging().VPN,
		nil,
	)
	Expect(err).ToNot(HaveOccurred())
	return response.Data
}

// GetHostName returns the hostname of the broker. If a messaging service is passes as non-nil, this function assumes
// the service is connected, and creates a new persistent receiver and non-durable queue on the broker. It then
// terminates the receiver, which cleans up the non-durable queue, and resolves the host name from a list of queues
// queried from the broker. If a nil messging service is passed, this function assumes that the queues of interest to
// the test already exist on the broker, and queries them to resolve the hostname without using API objects.
func GetHostName(messagingService solace.MessagingService, queueName string) (string, bool) {
	var hostName string
	var queueList []monitor.MsgVpnQueue
	if messagingService != nil {
		receiver, err := messagingService.CreatePersistentMessageReceiverBuilder().Build(resource.QueueNonDurableExclusive(queueName))
		if err != nil {
			return "", false
		}
		err = receiver.Start()
		if err != nil {
			return "", false
		}
		queueList = GetQueues()
		// We don't need to delete the queue in this case since a non durable queue will be destroyed on the broker once
		// its associated flow is unbound
		receiver.Terminate(0 * time.Second)
	} else {
		queueList = GetQueues()
	}
	retrievedQueueName := ""
loop:
	for i := range queueList {
		if strings.Contains(queueList[i].QueueName, queueName) {
			retrievedQueueName = queueList[i].QueueName
			break loop
		}
	}
	if retrievedQueueName == "" {
		return "", false
	}
	const queuePrefix = "#P2P/QTMP/v:"
	// This should be at the beginning, but just in case it's not, we still get the idx
	first_half_idx := strings.Index(retrievedQueueName, queuePrefix) + len(queuePrefix)
	second_half_idx := strings.Index(retrievedQueueName, "/"+queueName)
	hostName = strings.TrimSpace(retrievedQueueName[first_half_idx:second_half_idx])
	return hostName, false
}
