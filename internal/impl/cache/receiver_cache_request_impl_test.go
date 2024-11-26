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

package cache

import (
	"math"
	"testing"

	"solace.dev/go/messaging/pkg/solace/cache"
	"solace.dev/go/messaging/pkg/solace/resource"
)

func TestNewCachedMessageSubscriptionRequestWithInvalidPositiveCachedMessageSubscriptionStrategy(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const invalidCachedMessageSubscriptionStrategy = 5 // invalid subscription strategy
	const cacheAccessTimeout = 3001                    // in valid range - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5                        // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15                        // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(invalidCachedMessageSubscriptionStrategy,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest != nil || err == nil {
		t.Error("Expected error while calling NewCachedMessageSubscriptionRequest() with invalid cachedMessageSubscriptionStrategy (5)")
	}
}

func TestNewCachedMessageSubscriptionRequestWithInvalidNegativeCachedMessageSubscriptionStrategy(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const invalidCachedMessageSubscriptionStrategy = -1 // invalid subscription strategy
	const cacheAccessTimeout = 3001                     // in valid range - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5                         // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15                         // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(invalidCachedMessageSubscriptionStrategy,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest != nil || err == nil {
		t.Error("Expected error while calling NewCachedMessageSubscriptionRequest() with invalid cachedMessageSubscriptionStrategy (-1)")
	}
}

func TestNewCachedMessageSubscriptionRequestWithValidCachedMessageSubscriptionStrategy(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 3001 // in valid range - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.LiveCancelsCached,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest == nil || err != nil {
		t.Error("Did not expect error while calling NewCachedMessageSubscriptionRequest() with valid cachedMessageSubscriptionStrategy. Error: ", err)
	}
}

func TestNewCachedMessageSubscriptionRequestWithEmptyCacheName(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "" // empty cache name not valid
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 5000 // (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest != nil || err == nil {
		t.Error("Expected error while calling NewCachedMessageSubscriptionRequest() with empty cacheName")
	}
}

func TestNewCachedMessageSubscriptionRequestWithNilSubscription(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const cacheAccessTimeout = 5000 // (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, nil, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest != nil || err == nil {
		t.Error("Expected error while calling NewCachedMessageSubscriptionRequest() with nil topic subscription")
	}
}

func TestNewCachedMessageSubscriptionRequestWithInvalidCacheAccessTimeout(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 2999 // less than the allowed cache access time (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest != nil || err == nil {
		t.Error("Expected error while calling NewCachedMessageSubscriptionRequest() with out of range cacheAccessTimeout")
	}
}

func TestNewCachedMessageSubscriptionRequestWithValidLowerLimitOfCacheAccessTimeout(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 3000 // in valid range - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest == nil || err != nil {
		t.Error("Did not expect error while calling NewCachedMessageSubscriptionRequest() with valid lower limit of cacheAccessTimeout. Error: ", err)
	}
}

func TestNewCachedMessageSubscriptionRequestWithValidUpperLimitOfCacheAccessTimeout(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = math.MaxInt32 // greater than the allowed cache access time (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 5              // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15              // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest == nil || err != nil {
		t.Error("Did not expect error while calling NewCachedMessageSubscriptionRequest() with valid upper limit of cacheAccessTimeout. Error: ", err)
	}
}

func TestNewCachedMessageSubscriptionRequestWithInvalidMaxCachedMessages(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 5000 // (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = -1    // invalid value (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest != nil || err == nil {
		t.Error("Expected error while calling NewCachedMessageSubscriptionRequest() with out of range maxCachedMessages")
	}
}

func TestNewCachedMessageSubscriptionRequestWithValidLowerLimitOfMaxCachedMessages(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 5000 // in valid range - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 0     // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15     // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest == nil || err != nil {
		t.Error("Did not expect error while calling NewCachedMessageSubscriptionRequest() with valid lower limit of maxCachedMessages. Error: ", err)
	}
}

func TestNewCachedMessageSubscriptionRequestWithValidUpperLimitOfMaxCachedMessages(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 5000         // (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = math.MaxInt32 // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 15             // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest == nil || err != nil {
		t.Error("Did not expect error while calling NewCachedMessageSubscriptionRequest() with valid upper limit of maxCachedMessages. Error: ", err)
	}
}

func TestNewCachedMessageSubscriptionRequestWithInvalidCachedMessageAge(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 5000 // (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 50    // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = -1     // invalid value (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest != nil || err == nil {
		t.Error("Expected error while calling NewCachedMessageSubscriptionRequest() with out of range cachedMessageAge")
	}
}

func TestNewCachedMessageSubscriptionRequestWithValidLowerLimitOfCachedMessageAge(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 5000 // in valid range - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 50    // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = 0      // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest == nil || err != nil {
		t.Error("Did not expect error while calling NewCachedMessageSubscriptionRequest() with valid lower limit of cachedMessageAge. Error: ", err)
	}
}

func TestNewCachedMessageSubscriptionRequestWithValidUpperLimitOfCachedMessageAge(t *testing.T) {
	cacheRequest := NewReceiverCacheRequestImpl()
	const cacheName = "test-cache"
	const subscriptionString = "some-subscription"
	subscription := resource.TopicSubscriptionOf(subscriptionString)
	const cacheAccessTimeout = 5000        // (in milliseconds) - (valid range from 3000 to signed MaxInt32)
	const maxCachedMessages = 50           // in valid range (valid range from 0 to signed MaxInt32)
	const cachedMessageAge = math.MaxInt32 // in valid range (valid range from 0 to signed MaxInt32)
	cachedMessageSubscriptionRequest, err := cacheRequest.NewCachedMessageSubscriptionRequest(cache.AsAvailable,
		cacheName, subscription, cacheAccessTimeout, maxCachedMessages, cachedMessageAge)

	if cachedMessageSubscriptionRequest == nil || err != nil {
		t.Error("Did not expect error while calling NewCachedMessageSubscriptionRequest() with valid upper limit of cachedMessageAge. Error: ", err)
	}
}
