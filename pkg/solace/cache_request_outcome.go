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

import "solace.dev/go/messaging/pkg/solace/message"

// CacheRequestOutcome represents the outcome of a cache response. Refer to the doc string of each variant for more details.
type CacheRequestOutcome int

const (
	// Ok indicates that the cache request succeeded, and returned cached messages
	CacheRequestOutcomeOk CacheRequestOutcome = iota

	// NoData indicates that the cache request succeeded, but that no cached messages were available to be returned
	CacheRequestOutcomeNoData

	// SuspectData indicates that the cache request succeeded, but that the returned cached messages came from a suspect cache.
	CacheRequestOutcomeSuspectData

	// Failed indicates that the cache request failed in some way.
	// Refer to the 'error' associated with this outcome through the RequestCached interface.
	CacheRequestOutcomeFailed
)

// CacheResponse provides information about the response received from the cache.
type CacheResponse interface {

	// GetCacheRequestOutcome retrieves the cache request outcome for the cache response
	GetCacheRequestOutcome() CacheRequestOutcome

	// GetCacheRequestID retrieves the cache request ID that generated the cache response
	GetCacheRequestID() message.CacheRequestID

	// GetError retrieves the error field, will be nil if the cache request
	// was successful, and will be not nil if a problem was encountered.
	GetError() error
}
