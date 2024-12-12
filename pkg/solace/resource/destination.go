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
package resource

import "fmt"

// Destination represents a message destination on a broker.
// Some examples of destinations include queues and topics.
// Destination implementations can be retrieved by the various helper functions,
// such as TopicOf, QueueDurableExclusive, and TopicSubscriptionOf.
type Destination interface {
	// GetName retrieves the name of the destination
	GetName() string
}

// Subscription represents the valid subscriptions that can be specified to receivers.
// Valid subscriptions include *resource.TopicSubscription.
type Subscription interface {
	Destination
	// GetSubscriptionType will return the type of the subscription as a string
	GetSubscriptionType() string
}

// Topic is an implementation of destination representing a topic
// that can be published to.
type Topic struct {
	name string
}

// TopicOf creates a new topic with the specified name.
// Topic name must not be empty.
func TopicOf(expression string) *Topic {
	return &Topic{expression}
}

// GetName returns the name of the topic. Implements the Destination interface.
func (t *Topic) GetName() string {
	return t.name
}

// String implements fmt.Stringer
func (t *Topic) String() string {
	return fmt.Sprintf("Topic: %s", t.GetName())
}

// ShareName is an interface for identifiers that are associated
// with a shared subscription.
// See https://docs.solace.com/PubSub-Basics/Direct-Messages.htm#Shared in the Solace documentation.
type ShareName struct {
	name string
}

// ShareNameOf returns a new share name with the provided name.
// Valid share names are not empty and do not contain special
// characters '>' or '*'. Returns a new ShareName with the given string.
func ShareNameOf(name string) *ShareName {
	return &ShareName{name}
}

// GetName returns the share name. Implements the Destination interface.
func (sn *ShareName) GetName() string {
	return sn.name
}

// String implements fmt.Stringer
func (sn *ShareName) String() string {
	return fmt.Sprintf("ShareName: %s", sn.GetName())
}

// TopicSubscription is a subscription to a topic often used for receivers.
type TopicSubscription struct {
	topic string
}

// GetName returns the topic subscription expression. Implements the Destination interface.
func (t *TopicSubscription) GetName() string {
	return t.topic
}

// GetSubscriptionType returns the type of the topic subscription as a string
func (t *TopicSubscription) GetSubscriptionType() string {
	return fmt.Sprintf("%T", t)
}

// String implements fmt.Stringer
func (t *TopicSubscription) String() string {
	return fmt.Sprintf("TopicSubscription: %s", t.GetName())
}

// TopicSubscriptionOf creates a TopicSubscription of the specified topic string.
func TopicSubscriptionOf(topic string) *TopicSubscription {
	return &TopicSubscription{topic}
}

// Queue represents a queue used for guaranteed messaging receivers.
type Queue struct {
	name                           string
	exclusivelyAccessible, durable bool
}

// GetName returns the name of the queue. Implements the Destination interface.
func (q *Queue) GetName() string {
	return q.name
}

// IsExclusivelyAccessible determines if Queue supports exclusive or shared-access mode.
// Returns true if the Queue can serve only one consumer at any one time, false if the
// Queue can serve multiple consumers with each consumer serviced in a round-robin fashion.
func (q *Queue) IsExclusivelyAccessible() bool {
	return q.exclusivelyAccessible
}

// IsDurable determines if the Queue is durable. Durable queues are privisioned objects on
// the broker that have a lifespan that is independent of any one client session.
func (q *Queue) IsDurable() bool {
	return q.durable
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue: %s, exclusive: %t, durable: %t", q.GetName(), q.IsExclusivelyAccessible(), q.IsDurable())
}

// QueueDurableExclusive creates a new durable, exclusive queue with the specified name.
func QueueDurableExclusive(queueName string) *Queue {
	return &Queue{
		name:                  queueName,
		exclusivelyAccessible: true,
		durable:               true,
	}
}

// QueueDurableNonExclusive creates a durable, non-exclusive queue with the specified name.
func QueueDurableNonExclusive(queueName string) *Queue {
	return &Queue{
		name:                  queueName,
		exclusivelyAccessible: false,
		durable:               true,
	}
}

// QueueNonDurableExclusive creates an exclusive, non-durable queue with the specified name.
func QueueNonDurableExclusive(queueName string) *Queue {
	return &Queue{
		name:                  queueName,
		exclusivelyAccessible: true,
		durable:               false,
	}
}

// QueueNonDurableExclusiveAnonymous creates an anonymous, exclusive, and non-durable queue.
func QueueNonDurableExclusiveAnonymous() *Queue {
	return &Queue{
		name:                  "",
		exclusivelyAccessible: true,
		durable:               false,
	}
}

// CachedMessageSubscriptionStrategy indicates how the API should pass received cached and live messages to the application. Refer to each
// variant for details on what behaviour they configure.
type CachedMessageSubscriptionStrategy int

const (
	// AsAvailable provides a configuration for receiving a concurrent mix of both live and cached messages on the given TopicSubscription.
	AsAvailable CachedMessageSubscriptionStrategy = iota

	// LiveCancelsCached provides a configuration for initially passing received cached messages to the application and as soon as live
	// messages are received, passing those instead and passing no more cached messages.
	LiveCancelsCached

	// CachedFirst provides a configuration for passing only cached messages to the application, before passing the received live messages.
	// The live messages passed to the application thereof this configuration can be received as early as when the cache request is sent
	// by the API, and are enqueued until the cache response is received and its associated cached messages, if available, are passed to
	// the application.
	CachedFirst

	// CachedOnly provides a configuration for passing only cached messages and no live messages to the application.
	CachedOnly
)

// CachedMessageSubscriptionRequest provides an interface through which cache request configurations can be constructed. These
// configurations can then be passed to a call to a [RequestCached] interface method to request cached data. Refer to each of the below
// factory methods for details on what configuration they provide.
type CachedMessageSubscriptionRequest interface {

	// GetName retrieves the name of the topic subscription.
	GetName() string

	// GetCacheName retrieves the name of the cache.
	GetCacheName() string

	// GetCacheAccessTimeout retrieves the timeout for the cache request.
	GetCacheAccessTimeout() int32

	// GetMaxCachedMessages retrieves the max number of cached messages to be retrived in a request.
	GetMaxCachedMessages() int32

	// GetCachedMessageAge retrieves the max age of cached messages to be retrieved in a request.
	GetCachedMessageAge() int32

	// GetCachedMessageSubscriptionRequestStrategy retrieves the configured type of subscription strategy.
	GetCachedMessageSubscriptionRequestStrategy() *CachedMessageSubscriptionStrategy
}

type cachedMessageSubscriptionRequest struct {
	cacheName                         string
	subscription                      *TopicSubscription
	cacheAccessTimeout                int32
	maxCachedMessages                 int32
	cachedMessageAge                  int32
	cachedMessageSubscriptionStrategy *CachedMessageSubscriptionStrategy
}

// GetName retrieves the name of the topic subscription.
func (request *cachedMessageSubscriptionRequest) GetName() string {
	if request.subscription == nil {
		return "" // if topic subscription is nil, return an empty string
	}
	return request.subscription.GetName()
}

// GetCacheName retrieves the name of the cache.
func (request *cachedMessageSubscriptionRequest) GetCacheName() string {
	return request.cacheName
}

// GetCacheAccessTimeout retrieves the timeout for the cache request.
func (request *cachedMessageSubscriptionRequest) GetCacheAccessTimeout() int32 {
	return request.cacheAccessTimeout
}

// GetMaxCachedMessages retrieves the max number of cached messages to be retrived in a request.
func (request *cachedMessageSubscriptionRequest) GetMaxCachedMessages() int32 {
	return request.maxCachedMessages
}

// GetCachedMessageAge retrieves the max age of cached messages to be retrieved in a request.
func (request *cachedMessageSubscriptionRequest) GetCachedMessageAge() int32 {
	return request.cachedMessageAge
}

// GetCachedMessageSubscriptionRequestStrategy retrieves the configured type of subscription strategy.
func (request *cachedMessageSubscriptionRequest) GetCachedMessageSubscriptionRequestStrategy() *CachedMessageSubscriptionStrategy {
	return request.cachedMessageSubscriptionStrategy
}

// NewCachedMessageSubscriptionRequest returns a CachedMessageSubscriptionRequest that can be used to configure a cache request.
// The cachedMessageSubscriptionStrategy indicates how the API should pass received cached/live messages to the API after a cache
// request has been sent. Refer to [CachedMessageSubscriptionStrategy] for details on what behaviour each strategy configures.
// The cacheName parameter indicates the name of the cache to retrieve messages from.
// The subscription parameter indicates what topic the cache request should match against
// The cacheAccessTimeout parameter indicates how long in milliseconds a cache request is permitted to take before it is internally
// cancelled. The valid range for this timeout is between 3000 and signed int 32 max. This value specifies a timer for the internal
// requests that occur between this API and a PubSub+ cache. A single call to a [ReceiverCacheRequest] interface method
// can lead to one or more of these internal requests. As long as each of these internal requests
// complete before the specified time-out, the timeout value is satisfied.
// The maxCachedMessages parameter indicates the max number of messages expected to be returned as a part of a cache response. The range
// of this paramater is between 0 and signed int32 max, with 0 indicating that there should be no restrictions on the number of messages
// received as a part of a cache request.
// The cachedMessageAge parameter indicates the max age in seconds of the messages to be retrieved from a cache. The range
// of this parameter is between 0 and signed int 32 max, with 0 indicating that there should be no restrictions on the age of messages
// to be retrieved.
// The construction of NewCachedMessageSubscriptionRequest does not validate these parameter values. Instead, they are validated
// when the cache request is sent after a call to a [ReceiverCacheRequest] interface method.
func NewCachedMessageSubscriptionRequest(cachedMessageSubscriptionStrategy CachedMessageSubscriptionStrategy,
	cacheName string,
	subscription *TopicSubscription,
	cacheAccessTimeout int32,
	maxCachedMessages int32,
	cachedMessageAge int32) CachedMessageSubscriptionRequest {
	// map the cachedMessageSubscriptionStrategy
	var cachedMsgSubStrategy *CachedMessageSubscriptionStrategy = nil
	switch cachedMessageSubscriptionStrategy {
	case AsAvailable:
            fallthrough
	case CachedFirst:
            fallthrough
	case CachedOnly:
            fallthrough
	case LiveCancelsCached:
		// these are valid
		cachedMsgSubStrategy = &cachedMessageSubscriptionStrategy
	default:
		cachedMsgSubStrategy = nil
	}
	// return back a valid cache message subscription request if everything checks out
	return &cachedMessageSubscriptionRequest{
		cacheName:                         cacheName,
		subscription:                      subscription,
		cacheAccessTimeout:                cacheAccessTimeout,
		maxCachedMessages:                 maxCachedMessages,
		cachedMessageAge:                  cachedMessageAge,
		cachedMessageSubscriptionStrategy: cachedMsgSubStrategy,
	}
}
