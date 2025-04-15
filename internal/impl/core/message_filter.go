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

package core

import (
	"solace.dev/go/messaging/internal/ccsmp"
	apimessage "solace.dev/go/messaging/pkg/solace/message"
)

type MessageFilterConfig = ccsmp.SolClientMessageFilteringConfigPt

// defaultMessageFilterConfigValue provides a nil pointer to indicate that message filtering is not needed.
var defaultMessageFilterConfigValue MessageFilterConfig = nil

type cacheRequestMessageFilter struct {
	pointer        MessageFilterConfig
	cacheRequestID apimessage.CacheRequestID
	dispatchID     uintptr
}

func (filter *cacheRequestMessageFilter) Filter() MessageFilterConfig {
	return filter.pointer
}

func (filter *cacheRequestMessageFilter) SetupFiltering() {
	filter.pointer = ccsmp.AllocMessageFilteringConfigForCacheRequests(filter.pointer, filter.dispatchID, filter.cacheRequestID)
}

func (filter *cacheRequestMessageFilter) CleanupFiltering() {
	ccsmp.CleanMessageFilteringConfigForCacheRequests(filter.pointer)
}

func newCacheRequestMessageFilter(cacheRequestID apimessage.CacheRequestID, dispatchID uintptr) *cacheRequestMessageFilter {
	return &cacheRequestMessageFilter{
		pointer:        defaultMessageFilterConfigValue,
		cacheRequestID: cacheRequestID,
		dispatchID:     dispatchID,
	}
}

type ReceivedMessageFilter interface {
	// SetupFiltering allocates and configures any resources associated with filtering received messages.
	SetupFiltering()
	// CleanupFiltering cleans any resources associated with filtering received messages.
	CleanupFiltering()
	// Filter() returns the configured filter that is applied to received messages
	Filter() MessageFilterConfig
}
