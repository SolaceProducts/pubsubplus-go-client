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

// Package ccsmp isolates interop between C and
// PSPGo. Contributions to this module should be limited to Go wrappers
// around C functions, Go types representing C data types.
package ccsmp

// This module specifically must be limited to any functions or data types relating directly to cache operations in C.

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

void debugStatement(char *);

// Prototypes for C API internal interfaces available only to wrapper APIs.
solClient_returnCode_t _solClient_version_set(solClient_version_info_pt version_p);
*/
import "C"
import (
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	"solace.dev/go/messaging/internal/impl/logging"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"
)

// SolClientCacheSessionPt is a type alias to the opaque cache session pointer in CCSMP.
type SolClientCacheSessionPt = C.solClient_opaqueCacheSession_pt

type SolClientCacheSession struct {
	pointer SolClientCacheSessionPt
}

func (cacheSession *SolClientCacheSession) String() string {
	return fmt.Sprintf("SolClientCacheSession::cache session pointer 0x%x", cacheSession.pointer)
}

func WrapSolClientCacheSessionPt(cacheSessionP SolClientCacheSessionPt) SolClientCacheSession {
	return SolClientCacheSession{pointer: cacheSessionP}
}

func ConvertCachedMessageSubscriptionRequestToCcsmpPropsList(cachedMessageSubscriptionRequest resource.CachedMessageSubscriptionRequest) []string {
	return []string{
		SolClientCacheSessionPropCacheName, cachedMessageSubscriptionRequest.GetCacheName(),
		SolClientCacheSessionPropMaxAge, strconv.FormatInt(int64(cachedMessageSubscriptionRequest.GetCachedMessageAge()), 10),
		SolClientCacheSessionPropMaxMsgs, strconv.FormatInt(int64(cachedMessageSubscriptionRequest.GetMaxCachedMessages()), 10),
		SolClientCacheSessionPropRequestreplyTimeoutMs, strconv.FormatInt(int64(cachedMessageSubscriptionRequest.GetCacheAccessTimeout()), 10),
	}
}

// CachedMessageSubscriptionRequestStrategyMappingToCCSMPCacheRequestFlags is the mapping for Cached message Subscription Request Strategies to respective CCSMP cache request flags
var CachedMessageSubscriptionRequestStrategyMappingToCCSMPCacheRequestFlags = map[resource.CachedMessageSubscriptionStrategy]C.solClient_cacheRequestFlags_t{
	resource.CacheRequestStrategyAsAvailable:       C.solClient_cacheRequestFlags_t(C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FLOWTHRU | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY),
	resource.CacheRequestStrategyLiveCancelsCached: C.solClient_cacheRequestFlags_t(C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FULFILL | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY),
	resource.CacheRequestStrategyCachedFirst:       C.solClient_cacheRequestFlags_t(C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_QUEUE | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY),
	resource.CacheRequestStrategyCachedOnly:        C.solClient_cacheRequestFlags_t(C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FLOWTHRU | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY),
}

// CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags is the mapping for Cached message Subscription Request Strategies to respective CCSMP subscription flags
var CachedMessageSubscriptionRequestStrategyMappingToCCSMPSubscribeFlags = map[resource.CachedMessageSubscriptionStrategy]C.solClient_subscribeFlags_t{
	resource.CacheRequestStrategyAsAvailable:       C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
	resource.CacheRequestStrategyLiveCancelsCached: C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
	resource.CacheRequestStrategyCachedFirst:       C.SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
	resource.CacheRequestStrategyCachedOnly:        C.SOLCLIENT_SUBSCRIBE_FLAGS_LOCAL_DISPATCH_ONLY,
}

type SolClientSubscribeFlags = C.solClient_subscribeFlags_t

func LocalDispatchFlags() SolClientSubscribeFlags {
	return C.SOLCLIENT_SUBSCRIBE_FLAGS_LOCAL_DISPATCH_ONLY
}

/* NOTE: sessionToCacheEventCallbackMap is required as a global var even though cache sessions etc. are scoped to a
 * single receiver. This is required because the event callback that is passed to CCSMP when performing an async cache
 * request cannot have a pointer into Go-managed memory by being associated with receiver. If it did, and CCSMP the
 * event callback after the Go-managed receiver instance was garbage collected, the callback would point to garabage.
 * If we can't have the callback syntactically associated with the receiver, we need a different way to make sure that
 * a given event callback is operating on the correct receiver, since different receivers could have different
 * associated data that needs to be operated on by the event callback. We use a global map which uses the user_pointer
 * specific to the receiver to do this. Mapping of individual cache sessions within a receiver is done through a
 * separate mechanism, and is unrelated to this one.
 *
 * We use only the SolClientCacheSessionPt instead of the whole SolClientCacheSession because the pt evaluates to a
 * long, which can be hashed and searched faster in the map than a more complex object. At the time that this is
 * implemented (1/21/2025), the SolClientSession object holds only the pt, but allows for more fields to be added in the
 * future, which could greatly increase the size of the object, and so slow down hashing. Furthermore, at this time it
 * is unclear what further information beyond the SolClientCacheSessionPt would be useful in indexing, which is the
 * purpose of the cacheToEventCallbackMap. So, it is left as an exercise to a future developer to refactor the
 * cacheToEventCallbackMap along with SolClientSession to include more information useful for indexing, as needed.
 */
var cacheToEventCallbackMap sync.Map

func (session *SolClientSession) CreateCacheSession(cacheSessionProperties []string) (SolClientCacheSession, *SolClientErrorInfoWrapper) {
	cacheSessionPropertiesP, freeArrayFunc := ToCArray(cacheSessionProperties, true)
	defer freeArrayFunc()
	var cacheSessionP SolClientCacheSessionPt
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.SessionCreateCacheSession(
			cacheSessionPropertiesP,
			session.pointer,
			&cacheSessionP,
		)
	})
	return WrapSolClientCacheSessionPt(cacheSessionP), errorInfo
}

func (cacheSession *SolClientCacheSession) ConvertPointerToInt() uintptr {
	return uintptr(cacheSession.pointer)
}

func (cacheSession *SolClientCacheSession) SendCacheRequest(
	dispatchID uintptr,
	topic string,
	cacheRequestID message.CacheRequestID,
	cacheRequestFlags C.solClient_cacheRequestFlags_t,
	subscribeFlags C.solClient_subscribeFlags_t,
	eventCallback SolClientCacheEventCallback,
	messageFilterConfig SolClientMessageFilteringConfigPt,
) *SolClientErrorInfoWrapper {

	cacheToEventCallbackMap.Store(cacheSession.pointer, eventCallback)

	topicP := C.CString(topic)
	fmt.Printf("Calling CacheSessionSendCacheRequest with:\ndispatchId 0x%x\ntopic %s\n cacheRequestId %d\ncacheRequestFlags 0x%x\nsubscribeFlags 0x%x\nwithFiltering 0x%x\n", dispatchID, topic, cacheRequestID, cacheRequestFlags, subscribeFlags, uintptr(unsafe.Pointer(messageFilterConfig)))
	errorInfo := handleCcsmpError(func() SolClientReturnCode {
		return C.CacheSessionSendCacheRequest(
			C.uintptr_t(dispatchID),
			cacheSession.pointer,
			topicP,
			C.solClient_uint64_t(cacheRequestID), // This is done elsewhere such as in SolClientMessgeSetSequenceNumber
			cacheRequestFlags,
			subscribeFlags,
			messageFilterConfig,
		)
	})
	return errorInfo
}

func (cacheSession *SolClientCacheSession) DestroyCacheSession() *SolClientErrorInfoWrapper {
	/* NOTE: We remove the cache session pointer from the map before we destroy the cache session
	 * so that map access using that pointer as an index won't collide with another cache session
	 * that is created immediately after this one is destroyed. This should be deleted in the cache
	 * event callback. We delete again here in case there was a problem in the cache event callback
	 * so that we don't leak resources. If the entry has already been deleted, the following call
	 * will not fail, and will effectively be a no-op.*/
	cacheToEventCallbackMap.Delete(cacheSession.pointer)

	return handleCcsmpError(func() SolClientReturnCode {
		return C.CacheSessionDestroy(&cacheSession.pointer)
	})
}

func (cacheSession *SolClientCacheSession) CancelCacheRequest() *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		return C.CacheSessionCancelRequests(cacheSession.pointer)
	})
}

// solClientSessionSubscribeWithFlags wraps solClient_session_topicSubscribeWithDispatch
func (session *SolClientSession) UnsubscribeFromCacheRequestTopic(topic string, flags C.solClient_subscribeFlags_t, userP uintptr, correlationID uintptr) *SolClientErrorInfoWrapper {
	return handleCcsmpError(func() SolClientReturnCode {
		cString := C.CString(topic)
		defer C.free(unsafe.Pointer(cString))
		return C.SessionCacheTopicUnsubscribeWithFlags(session.pointer,
			cString,
			flags,
			C.solClient_uint64_t(userP),
			C.solClient_uint64_t(correlationID))
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
	var errString string
	if eventInfo.err != nil {
		errString = eventInfo.err.Error()
	} else {
		errString = "nil"
	}
	return fmt.Sprintf("CacheEventInfo:\n\tcacheSessionP: 0x%x\n\tevent: %d\n\ttopic: %s\n\treturnCode: %d\n\tsubCode: %d\n\tcacheRequestID: %d\n\terr: %s", eventInfo.cacheSessionP, eventInfo.event, eventInfo.topic, eventInfo.returnCode, eventInfo.subCode, eventInfo.cacheRequestID, errString)
}

const (
	SolClientCacheEventRequestCompletedNotice SolClientCacheEvent = 1
)

func NewCacheEventInfoForCancellation(cacheSession SolClientCacheSession, cacheRequestID message.CacheRequestID, topic string, err error) CacheEventInfo {
	return CacheEventInfo{
		cacheSessionP:  cacheSession.pointer,
		event:          SolClientCacheEventRequestCompletedNotice,
		topic:          topic,
		returnCode:     SolClientReturnCodeFail,
		subCode:        SolClientSubCodeCacheRequestCancelled,
		cacheRequestID: cacheRequestID,
		err:            err,
	}
}

func (i *CacheEventInfo) GetCacheSessionPointer() SolClientCacheSessionPt {
	return i.cacheSessionP
}

func CacheEventInfoFromCoreCacheEventInfo(eventCallbackInfo CacheEventInfoPt, userP uintptr) CacheEventInfo {
	return CacheEventInfo{
		/* FIX: Does getting the cache session from the userP work if the userP set for the cache request was either the filter info struct or dispatch ID? */
		cacheSessionP:  SolClientCacheSessionPt(userP),
		event:          SolClientCacheEvent(eventCallbackInfo.cacheEvent),
		topic:          C.GoString(eventCallbackInfo.topic),
		returnCode:     SolClientReturnCode(eventCallbackInfo.rc),
		subCode:        SolClientSubCode(eventCallbackInfo.subCode),
		cacheRequestID: message.CacheRequestID(eventCallbackInfo.cacheRequestId),
		err:            nil,
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
		fmt.Printf("Got to goCacheEventCallback with cacheEventInfo:\n%s\n", eventInfo.String())
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

//export goPrint
func goPrint(charPointer *C.char) {
	fmt.Printf("%s\n", C.GoString(charPointer))
}

type SolClientMessageFilteringConfigPt = C.solClientgo_msgDispatchCacheRequestIdFilterInfo_pt

func AllocMessageFilteringConfigForCacheRequests(filteringConfigP SolClientMessageFilteringConfigPt, dispatchId uintptr, cacheRequestId message.CacheRequestID) SolClientMessageFilteringConfigPt {
	fmt.Printf("AllocMessageFilteringConfigForCacheRequests::filter.pointer before ccsmp call is 0x%x\n", uintptr(unsafe.Pointer(filteringConfigP)))
	C.solClientgo_createAndConfigureMessageFiltering(&filteringConfigP, C.uintptr_t(dispatchId), C.solClient_uint64_t(cacheRequestId))
	fmt.Printf("AllocMessageFilteringConfigForCacheRequests::filter.pointer after ccsmp call is 0x%x\n", uintptr(unsafe.Pointer(filteringConfigP)))
	fmt.Printf("AllocMessageFilteringConfigForCacheRequests::&filter.pointer after ccsmp call is 0x%x\n", uintptr(unsafe.Pointer(&filteringConfigP)))
	return filteringConfigP
}

func CleanMessageFilteringConfigForCacheRequests(filteringConfigP SolClientMessageFilteringConfigPt) {
	C.solClientgo_freeFilteringConfig(filteringConfigP)
}
