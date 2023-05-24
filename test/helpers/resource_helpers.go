// pubsubplus-go-client
//
// Copyright 2021-2023 Solace Corporation. All rights reserved.
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
    _, _, err := testcontext.SEMP().Config().QueueApi.CreateMsgVpnQueue(testcontext.SEMP() .ConfigCtx(), sempconfig.MsgVpnQueue{
        QueueName:      queueName,
        AccessType:     "non-exclusive",
        Permission:     "modify-topic",
        IngressEnabled: True,
        EgressEnabled:  True,
        PartitionCount: partitionCount,
        PartitionRebalanceDelay: partitionRebalanceDelay,
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
