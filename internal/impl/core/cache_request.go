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
	"solace.dev/go/messaging/pkg/solace/resource"
)

type CacheRequest interface {
	// RequestConfig returns the [resource.CachedMessageSubscriptionRequest] that was configured by the application
	// for this cache request.
	RequestConfig() resource.CachedMessageSubscriptionRequest
	// ID returns the [CacheRequestID] that was specified for this cache request by the application.
	ID() apimessage.CacheRequestID
	// Processor returns the method through which the application decided to handle the cache response that will result
	// from this cache request.
	Processor() CacheResponseProcessor
	// CacheSession returns the [CoreCacheSession] that was created to service this cache request.
	CacheSession() CoreCacheSession
	// Index returns the [CacheRequestMapIndex] used to associate this cache request with its processor in the
	// receiver's internal map.
	Index() CacheRequestMapIndex
	// UsesLocalDispatch returns whether or not the [CacheRequest] uses local dispatch for subscription management.
	UsesLocalDispatch() bool
	// MessageFilter returns the filter used to filter received messages. If the cache request has not configured a filter,
	// this method returns nil.
	MessageFilter() *ReceivedMessageFilter
}

type CacheRequestImpl struct {
	cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest
	cacheRequestID                   apimessage.CacheRequestID
	cacheResponseProcessor           CacheResponseProcessor
	cacheSession                     ccsmp.SolClientCacheSession
	index                            CacheRequestMapIndex
	dispatchID                       uintptr
	messageFilter                    ReceivedMessageFilter
}

func GetCacheSessionFromCacheRequestIndex(cacheRequestMapIndex CacheRequestMapIndex) CoreCacheSession {
	return ccsmp.WrapSolClientCacheSessionPt(ccsmp.SolClientCacheSessionPt(cacheRequestMapIndex))
}

func GetCacheRequestMapIndexFromCacheSession(cacheSession ccsmp.SolClientCacheSession) CacheRequestMapIndex {
	return CacheRequestMapIndex(cacheSession.ConvertPointerToInt())
}

func (cacheRequest *CacheRequestImpl) RequestConfig() resource.CachedMessageSubscriptionRequest {
	return cacheRequest.cachedMessageSubscriptionRequest
}

func (cacheRequest *CacheRequestImpl) ID() apimessage.CacheRequestID {
	return cacheRequest.cacheRequestID
}

func (cacheRequest *CacheRequestImpl) Processor() CacheResponseProcessor {
	return cacheRequest.cacheResponseProcessor
}

func (cacheRequest *CacheRequestImpl) CacheSession() ccsmp.SolClientCacheSession {
	return cacheRequest.cacheSession
}

func (cacheRequest *CacheRequestImpl) Index() CacheRequestMapIndex {
	return cacheRequest.index
}

func (cacheRequest *CacheRequestImpl) UsesLocalDispatch() bool {
	strategy := cacheRequest.cachedMessageSubscriptionRequest.GetCachedMessageSubscriptionRequestStrategy()
	return ccsmp.CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags[*strategy] == ccsmp.LocalDispatchFlags()
}

func (cacheRequest *CacheRequestImpl) MessageFilter() *ReceivedMessageFilter {
	if cacheRequest.messageFilter != nil {
		return &cacheRequest.messageFilter
	}
	return nil
}

func NewCacheRequest(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest, cacheRequestID apimessage.CacheRequestID, cacheResponseHandler CacheResponseProcessor, cacheSession ccsmp.SolClientCacheSession, dispatchID uintptr) CacheRequest {
	cacheRequestImpl := CacheRequestImpl{
		cachedMessageSubscriptionRequest: cachedMessageSubscriptionRequest,
		cacheRequestID:                   cacheRequestID,
		cacheResponseProcessor:           cacheResponseHandler,
		cacheSession:                     cacheSession,
		index:                            GetCacheRequestMapIndexFromCacheSession(cacheSession),
		dispatchID:                       dispatchID,
	}
	if cacheRequestImpl.UsesLocalDispatch() {
		cacheRequestImpl.messageFilter = newCacheRequestMessageFilter(cacheRequestID, dispatchID)
		cacheRequestImpl.messageFilter.SetupFiltering()
	}
	return &cacheRequestImpl
}
