// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource_test

import (
	"testing"

	"solace.dev/go/messaging/pkg/solace/resource"
)

func TestValidShareNameOf(t *testing.T) {
	shareName := "mysharename"
	share := resource.ShareNameOf(shareName)
	if share == nil {
		t.Errorf("Expected share to not be nil, got nil")
	}
	if share.GetName() != shareName {
		t.Errorf("Expected share name to equal %s, got %s", shareName, share.GetName())
	}
}

func TestValidTopicSubscription(t *testing.T) {
	topicSubscriptionExpression := "mytopicsubscription"
	topicSubscription := resource.TopicSubscriptionOf(topicSubscriptionExpression)
	if topicSubscription == nil {
		t.Errorf("Expected topic subscription to not be nil, got nil")
	}
	if topicSubscription.GetName() != topicSubscriptionExpression {
		t.Errorf("Expected topic subscription name to equal %s, got %s", topicSubscriptionExpression, topicSubscription.GetName())
	}
}

func TestValidQueue(t *testing.T) {
	queueName := "myqueue"
	queue := resource.QueueDurableExclusive(queueName)
	if queue == nil {
		t.Errorf("Expected queue to not be nil, got nil")
	}
	if queue.GetName() != queueName {
		t.Errorf("Expected queue name to equal %s, got %s", queueName, queue.GetName())
	}
}
