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

import "solace.dev/go/messaging/pkg/solace/message"

// CacheResponse provides information about the response received from the cache.
type CacheResponse interface {
	// GetCacheRequestOutcome describes at a high level the result of the cache request.
	GetCacheRequestOutcome() CacheRequestOutcome

	// GetCacheRequestID provides a unique integer that can be used
	// to correlate cache responses and the received, previously cached data messages.
	GetCacheRequestID() message.CacheRequestID

	// GetError - the error field will be nil if the cache request was successful
	GetError() error
}

// cacheResponse provides information about the response received from the cache.
type cacheResponse struct {
	cacheRequestOutcome CacheRequestOutcome
	cacheRequestID      message.CacheRequestID
	err                 error
}

// Refer to the [CacheRequestOutcome] for details on what this field indicates.
func (c *cacheResponse) GetCacheRequestOutcome() CacheRequestOutcome {
	return c.cacheRequestOutcome
}

// GetCacheRequestId refer to [CacheRequestID] for details on what this field indicates
func (c *cacheResponse) GetCacheRequestID() message.CacheRequestID {
	return c.cacheRequestID
}

// GetError the error field will be nil if the cache request was successful,
// and will be not nil if a problem was encountered.
func (c *cacheResponse) GetError() error {
	return c.err
}

// CacheRequestOutcome represents the outcome of a cache response.
// Refer to the doc string of each variant for more details.
type CacheRequestOutcome int

const (
	// Ok indicates that the cache request succeeded, and returned cached messages
	Ok CacheRequestOutcome = iota

	// NoData indicates that the cache request succeeded,
	// but that no cached messages were available to be returned
	NoData

	// SuspectData indicates that the cache request succeeded,
	// but that the returned cached messages came from a suspect cache.
	SuspectData

	// Failed indicates that the cache request failed in some way.
	// Refer to the 'error' associated with this outcome through the RequestCached interface.
	Failed
)
