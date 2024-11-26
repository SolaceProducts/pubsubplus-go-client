package solace

import (
	"solace.dev/go/messaging/pkg/solace/cache"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// ReceiverCacheRequets Provides an interface through which the application can request cached messages from a cache.
// The cachedMessageSubscriptionRequest provides configuration for the impending cache request. Refer to [CachedMessageSubscriptionRequest] for more details.
// The cacheRequestId provides an identifier that can be used to correlated received cached messages with a cache request and cache response. This cache request ID
// MUST be unique for the duration of application execution, and it is the responsibility of the application to ensure this.
// This ID will be returned in either the function callback or channel, depdending on the chosen method.
// The provided function callback or returned channel will provide to the application only the cache responses resulting from outstanding cache requests.
// Data messages related to the cache response will be passed through the conventional [Receiver] interfaces of [Receive()] and [ReceiveAsync()].
type ReceiverCacheRequest interface {
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
	NewCachedMessageSubscriptionRequest(cachedMessageSubscriptionStrategy cache.CachedMessageSubscriptionStrategy,
		cacheName string, subscription *resource.TopicSubscription, cacheAccessTimeout int32, maxCachedMessages int32,
		cachedMessageAge int32) (cache.CachedMessageSubscriptionRequest, error)

	// RequestCachedAsync asynchronously requests cached data from a cache, and
	// defers processing of the resulting cache response to the application through
	// the returned channel.
	RequestCachedAsync(cachedMessageSubscriptionRequest cache.CachedMessageSubscriptionRequest,
		cacheRequestID message.CacheRequestID) <-chan cache.CacheResponse

	// RequestCachedAsyncWithCallback asynchronously requests cached data from a cache,
	// and processes the resulting cache response through the provided function callback.
	RequestCachedAsyncWithCallback(cachedMessageSubscriptionRequest cache.CachedMessageSubscriptionRequest,
		cacheRequestID message.CacheRequestID, callback func(cache.CacheResponse))
}
