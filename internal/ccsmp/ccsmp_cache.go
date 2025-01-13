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

package ccsmp

/*
#cgo CFLAGS: -DSOLCLIENT_PSPLUS_GO
#include <stdlib.h>
#include <stdio.h>

#include <string.h>

#include "solclient/solClient.h"
#include "solclient/solClientMsg.h"
#include "solclient/solCache.h"
#include "./ccsmp_helper.h"

solClient_rxMsgCallback_returnCode_t messageReceiveCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p );
solClient_rxMsgCallback_returnCode_t requestResponseReplyMessageReceiveCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p );
solClient_rxMsgCallback_returnCode_t defaultMessageReceiveCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p );
void eventCallback ( solClient_opaqueSession_pt opaqueSession_p, solClient_session_eventCallbackInfo_pt eventInfo_p, void *user_p );
void handleLogCallback(solClient_log_callbackInfo_pt logInfo_p, void *user_p);

solClient_rxMsgCallback_returnCode_t flowMessageReceiveCallback ( solClient_opaqueFlow_pt opaqueFlow_p, solClient_opaqueMsg_pt msg_p, void *user_p );
solClient_rxMsgCallback_returnCode_t defaultFlowMessageReceiveCallback ( solClient_opaqueFlow_pt opaqueFlow_p, solClient_opaqueMsg_pt msg_p, void *user_p );
void flowEventCallback ( solClient_opaqueFlow_pt opaqueFlow_p, solClient_flow_eventCallbackInfo_pt eventInfo_p, void *user_p );

solClient_returnCode_t _solClient_version_set(solClient_version_info_pt version_p);
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"

	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// CachedMessageSubscriptionRequestStrategyMappingToCCSMPCacheRequestFlags is the mapping for Cached message Subscription Request Strategies to respective CCSMP cache request flags
var CachedMessageSubscriptionRequestStrategyMappingToCCSMPCacheRequestFlags = map[resource.CachedMessageSubscriptionStrategy]C.solClient_cacheRequestFlags_t{
	resource.AsAvailable:       C.solClient_cacheRequestFlags_t(C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FLOWTHRU | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY),
	resource.LiveCancelsCached: C.solClient_cacheRequestFlags_t(C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FULFILL | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY),
	resource.CachedFirst:       C.solClient_cacheRequestFlags_t(C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_QUEUE | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY),
	resource.CachedOnly:        C.solClient_cacheRequestFlags_t(C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FLOWTHRU | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY),
}

// CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags is the mapping for Cached message Subscription Request Strategies to respective CCSMP subscription flags
var CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags = map[resource.CachedMessageSubscriptionStrategy]C.solClient_subscribeFlags_t{
	resource.AsAvailable:       C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
	resource.LiveCancelsCached: C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
	resource.CachedFirst:       C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
	resource.CachedOnly:        C.SOLCLIENT_SUBSCRIBE_FLAGS_LOCAL_DISPATCH_ONLY,
}

// SolClientCacheSessionPt is assigned a value
type SolClientCacheSessionPt = C.solClient_opaqueCacheSession_pt

/*
	sessionToCacheEventCallbackMap is required as a global var even though cache sessions etc. are

scoped to a single receiver. This is required because the event callback that is passed to CCSMP when
performing an async cache request cannot have a pointer into Go-managed memory by being associated with
receiver. If it did, and CCSMP the event callback after the Go-managed receiver instance was garbage
collected, the callback would point to garabage. If we can't have the callback syntactically associated
with the receiver, we need a different way to make sure that a given event callback is operating on the
correct receiver, since different receivers could have different associated data that needs to be operated
on by the event callback. We use a global map which uses the user_pointer specific to the receiver to do this.
Mapping of individual cache sessions within a receiver is done through a separate mechanism, and is unrelated
to this one.
*/
var cacheToEventCallbackMap sync.Map

func CreateCacheSession(cacheSessionProperties []string, sessionP SolClientSessionPt) (SolClientCacheSessionPt, *SolClientErrorInfoWrapper) {
	cacheSessionPropertiesP, freeArrayFunc := ToCArray(cacheSessionProperties, true)
	defer freeArrayFunc()
	var cacheSessionP SolClientCacheSessionPt
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.SessionCreateCacheSession(
			cacheSessionPropertiesP,
			sessionP,
			&cacheSessionP,
		)
	})
	return cacheSessionP, errorInfo
}

func SendCacheRequest(
	dispatchID uintptr,
	cacheSessionP SolClientCacheSessionPt,
	topic string,
	cacheRequestID message.CacheRequestID,
	cacheRequestFlags C.solClient_cacheRequestFlags_t, // There may be a custom type for this? TBD
	subscribeFlags C.solClient_subscribeFlags_t, // There may be a custom type for this? TBD
	eventCallback SolClientCacheEventCallback,
) *SolClientErrorInfoWrapper {

	cacheToEventCallbackMap.Store(cacheSessionP, eventCallback)

	topicP := C.CString(topic)
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.CacheSessionSendCacheRequest(
			C.solClient_uint64_t(dispatchID),
			cacheSessionP,
			topicP,
			C.solClient_uint64_t(cacheRequestID), // This is done elsewhere such as in SolClientMessgeSetSequenceNumber
			cacheRequestFlags,
			subscribeFlags,
		)
	})
	return errorInfo
}

func DestroyCacheSession(cacheSessionP SolClientCacheSessionPt) *SolClientErrorInfoWrapper {
	/* NOTE: We remove the cache session pointer from the map before we destroy the cache session
	 * so that map access using that pointer as an index won't collide with another cache session
	 * that is created immediately after this one is destroyed. This should be deleted in the cache
	 * event callback. We delete again here in case there was a problem in the cache event callback
	 * so that we don't leak resources. If the entry has already been deleted, the following call
	 * will not fail, and will effectively be a no-op.*/
	cacheToEventCallbackMap.Delete(cacheSessionP)

	return handleCcsmpError(func() SolClientReturnCode {
		return C.CacheSessionDestroy(&cacheSessionP)
	})
}

func CancelCacheRequest(cacheSessionP SolClientCacheSessionPt) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		return C.CacheSessionCancelRequests(cacheSessionP)
	})
}

// SolClientCacheEventCallback functions format CCSMP args into Go objects and then pass those objects
// to a separate function that actually processes them
type SolClientCacheEventCallback = func(CacheEventInfo)
type CacheEventInfoPt = C.solCache_eventCallbackInfo_pt

type SolClientCacheEvent = C.solCache_event_t

type CacheEventInfo struct {
	cacheSessionP  SolClientCacheSessionPt
	event          SolClientCacheEvent
	topic          string
	returnCode     SolClientReturnCode
	subCode        SolClientSubCode
	cacheRequestID message.CacheRequestID
	err            error
}

func (eventInfo *CacheEventInfo) String() string {
	return fmt.Sprintf("CacheEventInfo:\n\tcacheSessionP: 0x%x\n\tevent: %d\n\ttopic: %s\n\treturnCode: %d\n\tsubCode: %d\n\tcacheRequestID: %d\n\terr: %s", eventInfo.cacheSessionP, eventInfo.event, eventInfo.topic, eventInfo.returnCode, eventInfo.subCode, eventInfo.cacheRequestID, eventInfo.err)
}

const (
	SolClientCacheEventRequestCompletedNotice SolClientCacheEvent = 1
)

func NewConfiguredCacheEventInfo(cacheSessionP SolClientCacheSessionPt, cacheRequestID message.CacheRequestID, topic string, err error) CacheEventInfo {
	return CacheEventInfo{
		cacheSessionP:  cacheSessionP,
		event:          SolClientCacheEventRequestCompletedNotice,
		topic:          topic,
		returnCode:     SolClientReturnCodeFail,
		subCode:        SolClientSubCodeInternalError,
		cacheRequestID: cacheRequestID,
		err:            err,
	}
}

func (i *CacheEventInfo) GetCacheSessionPointer() SolClientCacheSessionPt {
	return i.cacheSessionP
}

func CacheEventInfoFromCoreCacheEventInfo(eventCallbackInfo CacheEventInfoPt, userP uintptr) CacheEventInfo {
	return CacheEventInfo{
		cacheSessionP:  SolClientCacheSessionPt(userP),
		event:          SolClientCacheEvent(eventCallbackInfo.cacheEvent),
		topic:          C.GoString(eventCallbackInfo.topic),
		returnCode:     SolClientReturnCode(eventCallbackInfo.rc),
		subCode:        SolClientSubCode(eventCallbackInfo.subCode),
		cacheRequestID: message.CacheRequestID(eventCallbackInfo.cacheRequestId),
	}
}

//export goCacheEventCallback
func goCacheEventCallback( /*opaqueSessionP*/ _ SolClientSessionPt, eventCallbackInfo CacheEventInfoPt, userP unsafe.Pointer) {
	/* NOTE: We don't need to use the session pointer since we can use the user_p(a.k.a. the cache session pointer)
	 * which is guaranteed to be unique for at least the duration that the cache session pointer is in the global
	 * map since during receiver termination we destory the cache session only after we remove it from all maps.
	 */
	if callback, ok := cacheToEventCallbackMap.Load(SolClientCacheSessionPt(uintptr(userP))); ok {
		eventInfo := CacheEventInfoFromCoreCacheEventInfo(eventCallbackInfo, uintptr(userP))
		callback.(SolClientCacheEventCallback)(eventInfo)
		/* NOTE: We remove the cache session pointer from the map before we destroy the cache session
		 * so that map access using that pointer as an index won't collide with another cache session
		 * that is created immediately after this one is destroyed.*/
		cacheToEventCallbackMap.Delete(eventInfo.cacheSessionP)
	} else {
		if logging.Default.IsDebugEnabled() {
			logging.Default.Debug("Received event callback from core API without an associated cache event callback")
		}
	}
}
