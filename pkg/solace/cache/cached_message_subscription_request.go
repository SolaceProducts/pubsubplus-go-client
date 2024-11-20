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

// Package cache contains the interfaces required by the cache feature.
package cache

// CachedMessageSubscriptionRequest provides an interface through which cache request configurations can be constructed. These
// configurations can then be passed to a call to a [RequestCached] interface method to request cached data. Refer to each of the below
// factory methods for details on what configuration they provide.
type CachedMessageSubscriptionRequest interface {
	// GetCacheName retrieves the name of the cache.
	GetCacheName() string

	// GetName retrieves the name of the topic subscription.
	GetName() string
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
