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
	cachedMessageSubscriptionRequest := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyLiveCancelsCached,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if *(cachedMessageSubscriptionRequest.GetCachedMessageSubscriptionRequestStrategy()) != resource.CacheRequestStrategyLiveCancelsCached {
		t.Error("Expected GetCachedMessageSubscriptionRequestStrategy() to match passed in cachedMessageSubscriptionStrategy when NewCachedMessageSubscriptionRequest() called with valid cachedMessageSubscriptionStrategy.")
	}
	if cachedMessageSubscriptionRequest.GetCacheName() != cacheName {
		t.Error("Expected GetCacheName() to match CacheName when NewCachedMessageSubscriptionRequest() called with valid cacheName.")
	}
	if cachedMessageSubscriptionRequest.GetName() != subscriptionString {
		t.Error("Expected GetName() to match passed in topic subscription name when NewCachedMessageSubscriptionRequest() called with valid subscription.")
	}
	if cachedMessageSubscriptionRequest.GetCacheAccessTimeout() != cacheAccessTimeout {
		t.Error("Expected GetCacheAccessTimeout() to match passed in cacheAccessTimeout when NewCachedMessageSubscriptionRequest() called with valid cacheAccessTimeout.")
	}
	if cachedMessageSubscriptionRequest.GetMaxCachedMessages() != maxCachedMessages {
		t.Error("Expected GetMaxCachedMessages() to match passed in maxCachedMessages when NewCachedMessageSubscriptionRequest() called with valid maxCachedMessages.")
	}
	if cachedMessageSubscriptionRequest.GetCachedMessageAge() != cachedMessageAge {
		t.Error("Expected GetCachedMessageAge() to match passed in cachedMessageAge when NewCachedMessageSubscriptionRequest() called with valid cachedMessageAge.")
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
	cachedMessageSubscriptionRequest := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyAsAvailable,
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
	cachedMessageSubscriptionRequest := resource.NewCachedMessageSubscriptionRequest(resource.CacheRequestStrategyAsAvailable,
		cacheName, nil, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest.GetName() != "" {
		t.Error("Expected topic subscription to be nil when NewCachedMessageSubscriptionRequest() called with nil topic subscription")
	}
}
