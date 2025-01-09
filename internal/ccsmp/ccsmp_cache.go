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

/* TODO: Not sure if we really need this. */

// SolClientCacheSession structure
type SolClientCacheSession struct {
	pointer SolClientCacheSessionPt
}

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
	/* NOTE: Do we need to add the dispatch callback to the map here, or is that already done by the receiver in calls to Receive() or ReceiveAsync()? */

	topicP := C.CString(topic)
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.CacheSessionSendCacheRequest(
			C.solClient_uint64_t(dispatchID),
			// TODO: rework this, we should probably just have a pointer and not a whole object.
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
	 * that is created immediately after this one is destroyed.*/
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

// SolClientCacheEventCallback functions should format CCSMP args into Go objects and then pass those objects
// to a separate function that actually processes them, maybe through a channel with a background go routine
// polling it?
type SolClientCacheEventCallback = func(SolClientCacheEventInfo)
type SolClientCacheEventInfoPt = C.solCache_eventCallbackInfo_pt

type SolClientCacheEvent = C.solCache_event_t

type SolClientCacheEventInfo struct {
	/* TODO: Rename this to CacheEventInfo to better distinguish it from CCSMP objects, since it now has more than
	 * just the original event fields. */
	cacheSessionP  SolClientCacheSessionPt
	event          SolClientCacheEvent
	topic          string
	returnCode     SolClientReturnCode
	subCode        SolClientSubCode
	cacheRequestID message.CacheRequestID
	err            error
}

func (eventInfo *SolClientCacheEventInfo) String() string {
	return fmt.Sprintf("SolClientCacheEventInfo:\n\tcacheSesionP: 0x%x\n\tevent: %d\n\ttopic: %s\n\treturnCode: %d\n\tsubCode: %d\n\tcacheRequestID: %d\n\terr: %s", eventInfo.cacheSessionP, eventInfo.event, eventInfo.topic, eventInfo.returnCode, eventInfo.subCode, eventInfo.cacheRequestID, eventInfo.err)
}

const (
	SolClientCacheEventRequestCompletedNotice SolClientCacheEvent = 1
)

func NewConfiguredSolClientCacheEventInfo(cacheSessionP SolClientCacheSessionPt, cacheRequestID message.CacheRequestID, topic string, err error) SolClientCacheEventInfo {
	return SolClientCacheEventInfo{
		cacheSessionP:  cacheSessionP,
		event:          SolClientCacheEventRequestCompletedNotice,
		topic:          topic,
		returnCode:     SolClientReturnCodeFail,
		subCode:        SolClientSubCodeInternalError,
		cacheRequestID: cacheRequestID,
		err:            err,
	}
}

func (i *SolClientCacheEventInfo) GetCacheSessionPointer() SolClientCacheSessionPt {
	return i.cacheSessionP
}

func SolClientCacheEventInfoFromCoreCacheEventInfo(eventCallbackInfo SolClientCacheEventInfoPt, userP uintptr) SolClientCacheEventInfo {
	return SolClientCacheEventInfo{
		cacheSessionP:  SolClientCacheSessionPt(userP),
		event:          SolClientCacheEvent(eventCallbackInfo.cacheEvent),
		topic:          C.GoString(eventCallbackInfo.topic),
		returnCode:     SolClientReturnCode(eventCallbackInfo.rc),
		subCode:        SolClientSubCode(eventCallbackInfo.subCode),
		cacheRequestID: message.CacheRequestID(eventCallbackInfo.cacheRequestId),
	}
}

//export goCacheEventCallback
func goCacheEventCallback( /*opaqueSessionP*/ _ SolClientSessionPt, eventCallbackInfo SolClientCacheEventInfoPt, userP unsafe.Pointer) {
	/* NOTE: We don't need to use the session pointer since we can use the user_p(a.k.a. the cache session pointer)
	 * which is guaranteed to be unique for at least the duration that the cache session pointer is in the global
	 * map since during receiver termination we destory the cache session only after we remove it from all maps.
	 */
	if callback, ok := cacheToEventCallbackMap.Load(SolClientCacheSessionPt(uintptr(userP))); ok {
		callback.(SolClientCacheEventCallback)(SolClientCacheEventInfoFromCoreCacheEventInfo(eventCallbackInfo, uintptr(userP)))
	} else {
		if logging.Default.IsDebugEnabled() {
			logging.Default.Debug("Received event callback from core API without an associated cache event callback")
		}
	}
}
