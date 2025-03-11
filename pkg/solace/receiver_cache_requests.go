// pubsubplus-go-client
//
// Copyright 2025 Solace Corporation. All rights reserved.
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

package solace

import (
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// ReceiverCacheRequests provides an interface through which the application can request cached messages from a cache.
//   - cachedMessageSubscriptionRequest: Configuration for the submitted cache request. Refer to
//     [solace.dev/go/messaging/pkg/resource.CachedMessageSubscriptionRequest] for more details.
//   - cacheRequestID: An identifier that can be used to correlate received cached messages with a cache
//     request and response. This cache request ID MUST be unique for the duration of application execution, and
//     it is the responsibility of the application to ensure this. This ID will be returned to the application through
//     the [CacheResponse] provided to the application after the cache request has completed.
//
// The provided function callback or returned channel will provide to the application only the cache responses
// resulting from outstanding cache requests. Data messages related to the cache response will be passed through the
// conventional [DirectMessageReceiver] interfaces of Receive() and ReceiveAsync().
//
// In cases where the application does not immediately process the cache response, it may appear that the application
// does not receive the expected cache response within the timeout configured through
// [resource.NewCachedMessageSubscriptionRequest]. It is important to note that the configured timeout applies only to
// the network, so if the API receives the cache response before the timeout expires, but the application does not
// process the response until after the timeout expires, the cache response will still be marked as complete.
type ReceiverCacheRequests interface {

	// RequestCachedAsync asynchronously requests cached data from a cache and defers processing of the resulting
	// cache response to the application through the returned channel.
	// Returns IllegalStateError if the service is not connected or the receiver is not running.
	// Returns InvalidConfigurationError if an invalid [resource.CachedMessageSubscriptionRequest] was passed.
	RequestCachedAsync(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID message.CacheRequestID) (<-chan CacheResponse, error)

	// RequestCachedAsyncWithCallback asynchronously requests cached data from a cache and processes the resulting
	// cache response through the provided function callback.
	// Returns IllegalStateError if the service is not connected or the receiver is not running.
	// Returns InvalidConfigurationError if an invalid [resource.CachedMessageSubscriptionRequest] was passed.
	RequestCachedAsyncWithCallback(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID message.CacheRequestID, callback func(CacheResponse)) error
}
