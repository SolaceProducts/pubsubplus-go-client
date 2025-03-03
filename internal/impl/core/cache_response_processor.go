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
	"solace.dev/go/messaging/pkg/solace"
	// apimessage "solace.dev/go/messaging/pkg/solace/message"
)

// CacheResponseProcessor provides an interface through which the information necessary to process a cache response
// that is passed from CCSMP can be acquired.
type CacheResponseProcessor interface {
	// GetCallback returns the configured cache response processing method.
	GetCallback() func(solace.CacheResponse)

	// ProcessCacheResponse processes the cache response according to the implementation
	ProcessCacheResponse(solace.CacheResponse)

	// // GetCacheRequestInfo returns the original information that was used to send the cache request.
	// // This is useful for comparing a received cache response during processing, or for adding
	// // information to logging or error messages when this information cannot be retrieved from the
	// // cache response.
	// GetCacheRequestInfo() *CacheRequestInfo
}

// cacheResponseProcessor holds an application-provided callback that is responsible for post-processing the cache
// response. cacheResponseProcessor implements the CacheResponseProcessor interface to allow safe access of this
// callback when being retrieved from a map of heterogeneous values.
type cacheResponseProcessor struct {
	CacheResponseProcessor
	//	cacheRequestInfo CacheRequestInfo
	callback func(solace.CacheResponse)
}

// func NewCacheResponseProcessor(callback func(solace.CacheResponse), cacheRequestInfo CacheRequestInfo) cacheResponseProcessor {
func NewCacheResponseProcessor(callback func(solace.CacheResponse)) cacheResponseProcessor {
	return cacheResponseProcessor{
		//		cacheRequestInfo: cacheRequestInfo,
		callback: callback,
	}
}

func (crp cacheResponseProcessor) GetCallback() func(solace.CacheResponse) {
	return crp.callback
}

func (crp cacheResponseProcessor) ProcessCacheResponse(cacheResponse solace.CacheResponse) {
	crp.callback(cacheResponse)
}

//func (crp cacheResponseProcessor) GetCacheRequestInfo() *CacheRequestInfo {
//	return &crp.cacheRequestInfo
//}

//// CacheRequestInfo holds the original information that was used to send the cache request.
//// This is useful for comparing a received cache response during processing, or for adding
//// information to logging or error messages when this information cannot be retrieved from the
//// cache response.
///* NOTE: This is actually most useful in generating cache response stubs for cache sessions that somehow got lost and
// * that still need to be cleaned up during termination.*/
//type CacheRequestInfo struct {
//	/* NOTE: we don't need to include the cache session pointer in this struct since it is only ever stored
//	 * in a map where the cache session pointer is used as the key.*/
//	cacheRequestID apimessage.CacheRequestID
//	topic          string
//    isLocalDispatch bool
//}
//
//func NewCacheRequestInfo(cacheRequestID apimessage.CacheRequestID, topic string) CacheRequestInfo {
//	return CacheRequestInfo{
//		cacheRequestID: cacheRequestID,
//		topic:          topic,
//        isLocalDispatch: false,
//	}
//}
//
//func (i CacheRequestInfo) WithLocalDispatch() CacheRequestInfo {
//        i.isLocalDispatch = true
//        return i
//}
//
//func (i *CacheRequestInfo) GetTopic() string {
//	return i.topic
//}
//
//func (i *CacheRequestInfo) GetCacheRequestID() apimessage.CacheRequestID {
//	return i.cacheRequestID
//}
//
//func (i *CacheRequestInfo) usesLocalDispatch() bool {
//        return i.isLocalDispatch
//}
