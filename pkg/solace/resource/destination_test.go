// pubsubplus-go-client
//
// Copyright 2021-2024 Solace Corporation. All rights reserved.
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

func TestNewCachedMessageSubscriptionRequest(t *testing.T) {
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 3001 // in valid range - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest := resource.NewCachedMessageSubscriptionRequest(resource.LiveCancelsCached,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest.GetCachedMessageSubscriptionRequestStrategy() == nil {
		t.Error("Did not expect nil cachedMessageSubscriptionStrategy when NewCachedMessageSubscriptionRequest() called with valid cachedMessageSubscriptionStrategy.")
	}
	if cachedMessageSubscriptionRequest.GetCacheName() == "" {
		t.Error("Did not expect CacheName to be empty when NewCachedMessageSubscriptionRequest() called with valid cacheName.")
	}
	if cachedMessageSubscriptionRequest.GetSubscription() == nil {
		t.Error("Did not expect topic subscription to be nil when NewCachedMessageSubscriptionRequest() called with valid subscription.")
	}
	if cachedMessageSubscriptionRequest.GetCacheAccessTimeout() == 0 {
		t.Error("Did not expect cacheAccessTimeout to be 0 when NewCachedMessageSubscriptionRequest() called with valid cacheAccessTimeout.")
	}
	if cachedMessageSubscriptionRequest.GetMaxCachedMessages() == 0 {
		t.Error("Did not expect maxCachedMessages to be 0 when NewCachedMessageSubscriptionRequest() called with valid maxCachedMessages.")
	}
	if cachedMessageSubscriptionRequest.GetCachedMessageAge() == 0 {
		t.Error("Did not expect cachedMessageAge to be 0 when NewCachedMessageSubscriptionRequest() called with valid cachedMessageAge.")
	}
}

func TestNewCachedMessageSubscriptionRequestWithInvalidCachedMessageSubscriptionStrategy(t *testing.T) {
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const invalidCachedMessageSubscriptionStrategy = -1 // invalid subscription strategy
	const cacheAccessTimeout = 3001                     // in valid range - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5                         // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15                         // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest := resource.NewCachedMessageSubscriptionRequest(invalidCachedMessageSubscriptionStrategy,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest.GetCachedMessageSubscriptionRequestStrategy() != nil {
		t.Error("Expected nil cachedMessageSubscriptionStrategy in returned struct with invalid cachedMessageSubscriptionStrategy (-1)")
	}
}

func TestNewCachedMessageSubscriptionRequestWithEmptyCacheName(t *testing.T) {
	const cacheName = "" // empty cache name
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 5000 // (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest := resource.NewCachedMessageSubscriptionRequest(resource.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest.GetCacheName() != "" {
		t.Error("Expected CacheName to be empty when NewCachedMessageSubscriptionRequest() called with empty cacheName")
	}
}

func TestNewCachedMessageSubscriptionRequestWithNilSubscription(t *testing.T) {
	const cacheName = "test-cache"
	const cacheAccessTimeout = 5000 // (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest := resource.NewCachedMessageSubscriptionRequest(resource.AsAvailable,
		cacheName, nil, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest.GetSubscription() != nil {
		t.Error("Expected topic subscription to be nil when NewCachedMessageSubscriptionRequest() called with nil topic subscription")
	}
}
