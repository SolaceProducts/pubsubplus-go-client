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

// Package resource contains types and factory functions for various
// broker resources such as topics and queues.
package cache

import (
	"fmt"
	"math"

	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/internal/impl/validation"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/cache"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

type cachedMessageSubscriptionRequest struct {
	cacheName          string
	name               string
	subscription       resource.TopicSubscription
	cacheAccessTimeout int32
	maxCachedMessages  int32
	cachedMessageAge   int32
}

// GetCacheName retrieves the name of the cache.
func (request *cachedMessageSubscriptionRequest) GetCacheName() string {
	return request.cacheName
}

// GetName retrieves the name of the topic subscription.
func (request *cachedMessageSubscriptionRequest) GetName() string {
	return request.name
}

// receiverCacheRequestImpl structure
type receiverCacheRequestImpl struct {
	logger logging.LogLevelLogger
}

// NewReceiverCacheRequestImpl function
func NewReceiverCacheRequestImpl() solace.ReceiverCacheRequest {
	return &receiverCacheRequestImpl{
		logger: logging.For(receiverCacheRequestImpl{}),
	}
}

// NewCachedMessageSubscriptionRequest returns a CachedMessageSubscriptionRequest that can be used to configure a cache request
// and an error indicating whether or not the operation was successful. If the operation was successful the error is nil. Otherwise,
// the error will be non-nil and will indicate the reason the operation failed.
// The cachedMessageSubscriptionStrategy indicates how the API should pass received cached/live messages to the API after a cache
// request has been sent. Refer to [CachedMessageSubscriptionStrategy] for details on what behaviour each strategy configures.
// The cache_name parameter indicates the name of the cache to retrieve messages from.
// The subscription parameter indicates what topic the cache request should match against
// The cacheAccessTimeout parameter indicates how long in milliseconds a cache request is permitted to take before it is internally
// cancelled. The valid range for this timeout is between 3000 and signed int 32 max. This value specifies a timer for the internal
// requests that occur between this API and a PubSub+ cache. A single call to a [ReceiverCacheRequests] interface method
// can lead to one or more of these internal requests. As long as each of these internal requests
// complete before the specified time-out, the timeout value is satisfied.
// The maxCachedMessages parameter indicates the max number of messages expected to be returned as a part of a cache response. The range
// of this paramater is between 0 and signed int32 max, with 0 indicating that there should be no restrictions on the number of messages
// received as a part of a cache request.
// The cachedMessageAge parameter indicates the max age in seconds of the messages to be retrieved from a cache. The range
// of this parameter is between 0 and signed int 32 max, with 0 indicating that there should be no restrictions on the age of messages
// to be retrieved.
func (receiverCacheRequest *receiverCacheRequestImpl) NewCachedMessageSubscriptionRequest(cachedMessageSubscriptionStrategy cache.CachedMessageSubscriptionStrategy,
	cacheName string,
	subscription resource.TopicSubscription,
	cacheAccessTimeout int32,
	maxCachedMessages int32,
	cachedMessageAge int32) (cache.CachedMessageSubscriptionRequest, error) {
	// Todo: Implementation here
	// do some validations here
	// validate for the cacheName Timeout range
	_, _, err := validation.StringPropertyValidation("cacheName", cacheAccessTimeout)
	if err != nil {
		return nil, err
	}
	if (subscription == resource.TopicSubscription{}) {
		return nil, solace.NewError(&solace.InvalidConfigurationError{}, fmt.Sprintf("expected required property 'subscription' to be set"), nil)
	}
	// validate for the cacheAccess Timeout range
	_, _, err = validation.IntegerPropertyValidationWithRange("cacheAccessTimeout", cacheAccessTimeout, 3000, math.MaxInt32)
	if err != nil {
		return nil, err
	}
	// validate for the cache messages range
	_, _, err = validation.IntegerPropertyValidationWithRange("maxCachedMessages", cacheAccessTimeout, 0, math.MaxInt32)
	if err != nil {
		return nil, err
	}
	// validate for the cache message TTL range
	_, _, err = validation.IntegerPropertyValidationWithRange("cachedMessageAge", cachedMessageAge, 0, math.MaxInt32)
	if err != nil {
		return nil, err
	}

	// return back a valid cache message subscription request if everything checks out
	return &cachedMessageSubscriptionRequest{
		name:               cacheName,
		cacheName:          cacheName,
		subscription:       subscription,
		cacheAccessTimeout: cacheAccessTimeout,
		maxCachedMessages:  maxCachedMessages,
		cachedMessageAge:   cachedMessageAge,
	}, nil
}

// RequestCachedAsync asynchronously requests cached data from a cache, and
// defers processing of the resulting cache response to the application through
// the returned channel.
func (receiverCacheRequest *receiverCacheRequestImpl) RequestCachedAsync(cachedMessageSubscriptionRequest cache.CachedMessageSubscriptionRequest,
	cacheRequestId message.CacheRequestID) <-chan cache.CacheResponse {
	// TODO: Implementation here
	return nil
}

// RequestCachedAsyncWithCallback asynchronously requests cached data from a cache,
// and processes the resulting cache response through the provided function callback.
func (receiverCacheRequest *receiverCacheRequestImpl) RequestCachedAsyncWithCallback(cachedMessageSubscriptionRequest cache.CachedMessageSubscriptionRequest,
	cacheRequestId message.CacheRequestID, callback func(cache.CacheResponse)) {
	// TODO: Implementation here
}
