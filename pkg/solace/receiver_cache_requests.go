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

package solace

import (
        "solace.dev/go/messaging/pkg/solace/resource"
        "solace.dev/go/messaging/pkg/solace/message"
)

// ReceiverCacheRequests Provides an interface through which the application can request cached messages from a cache.
// The cachedMessageSubscriptionRequest provides configuration for the impedning cache request. Refer to
// [CachedMessageSubscriptionRequest] for more details.
// The cacheRequestID provides an identifier the can be used to correlate received cached messages with a cache
// request and response. This cache request ID MUST be unique for the duration of application execution, and it is the
// responsibility of the application to ensure this.
// This ID will be returned in either the function callback or channel, depending on the chosen method.
// The provided function callback or returned channel will provide to the application only the cache responses
// resulting from outstanding cache requests. Data messages related to the cache response willbe passed through the
// conventional [Receiver] interfaces of [Receive()] and [ReceiveAsync()].
type ReceiverCacheRequests interface {
        /* TODO: Check the error types in this doc string are correct. */

        // RequestCachedAsync asynchronously requests cached data from a cache and defers processing of the resulting
        // cache response to the application throufh the returned channel.
        // Returns PubSubPlusClientError if the operation could not be performed.
        // Returns IllegalStateError if the service is not connected or the receiver is not running.
        RequestCachedAsync(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID message.CacheRequestID) (<- chan CacheResponse, error)

        /* TODO: Check the error types in this doc string are correct. */

        // RequestCachedAsyncWithCallback asynchronously requests cached data from a cache and processes the resulting
        // cache response through the provided function callback.
        // Returns PubSubPlusClientError if the operation could not be performed.
        // Returns IllegalStateError if the service is not connected or the receiver is not running.
        RequestCachedAsyncWithCallback(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID message.CacheRequestID, callback func(CacheResponse)) error
}
