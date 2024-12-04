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
#include <stdlib.h>
#include <stdio.h>

#include <string.h>

#include "solclient/solClient.h"
#include "solclient/solClientMsg.h"
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
        "unsafe"
        "sync"

        "solace.dev/go/messaging/pkg/solace/resource"
        "solace.dev/go/messaging/pkg/solace/message"
        "solace.dev/go/messaging/internal/impl/logging"
)

// SolClientCacheSessionPt is assigned a value
type SolClientCacheSessionPt = *C.solClient_opaqueCacheSession_pt

/* TODO: Not sure if we really need this. */
type SolClientCacheSession struct {
       pointer SolClientCacheSessionPt 
}

/* sessionToCacheEventCallbackMap is required as a global var even though cache sessions etc. are
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

func RegisterCacheEventCallback()

func (sessionP *SolClientSessionPt) CreateCacheSession(cacheSessionProperties []string, messagingServiceSessionPointer string) (SolClientCacheSessionPt, *SolClientErrorInfoWrapper) {
        cacheSessionPropertiesP , freeArrayFunc := ToCArray(cacheSessionProperties, true)
        defer freeArrayFunc()
        var cacheSessionP SolClientCacheSessionPt
        errorInfo := handleCcsmpError(func() SolClientReturnCode {
                return C.SessionCreateCacheSession(
                        cacheSessionPropertiesP,
                        sessionP,
                        cacheSessionP,
                )
        })
        /* TODO: handle errors from cache sessino create? */
        return cacheSessionP, errorInfo
}

func (cacheSessionP SolClientCacheSessionPt) SendCacheRequest (
        topic string,
        cacheRequestId message.CacheRequestId,
        cacheRequestFlags uint32, // There may be a custom type for this? TBD
        subscribeFlags uint32, // There may be a custom type for this? TBD
        ) *SolClientErrorInfoWrapper {

        topicP := C.CString(topic)
        errorInfo := handleCcsmpError(func() SolClientReturnCode {
                return C.CacheSessionSendCacheRequest(
                        // TODO: rework this, we should probably just have a pointer and not a whole object.
                        cacheSessionP,
                        topicP,
                        C.solClient_uint64_t(cacheRequestId), // This is done elsewhere such as in SolClientMessgeSetSequenceNumber
                        C.solClient_cacheRequestFlags_t(cacheRequestFlags), // Could also use solClient_uint32_t
                        C.solClientSubscribeFlags_t(subscribeFlags), // Could also use solClient_uint32_t
                )
        })
        return errorInfo
}

func (cacheSessionP SolClientCacheSessionPt) DestroyCacheSession() *SolClientErrorInfoWrapper {
        return handleCcsmpError(func() SolClientReturnCode {
                return C.CacheSessionDestroy(cacheSessionP)
        })
}

func (cacheSessionP SolClientCacheSessionPt) CancelCacheRequest() * SolClientErrorInfoWrapper {
        return handleCcsmpError(func () SolClientReturnCode {
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
        cacheSessionP SolClientCacheSessionPt
        event SolClientCacheEvent
        topic string
        returnCode SolClientResponseCode
        subCode SolClientSubCode
        cacheRequestId message.CacheRequestId
}

func (i * SolClientCacheEventInfo) GetCacheSessionPointer() SolClientCacheSessionPt {
        return i.cacheSessionP
}

func SolClientCacheEventInfoFromCoreCacheEventInfo(eventCallbackInfo SolClientCacheEventInfoPt, userP unsafe.Pointer) SolClientCacheEventInfo {
        return SolClientCacheEventInfo{
                cacheSessionP: SolClientCacheSessionPt(userP),
                event: SolClientCacheEvent(eventCallbackInfo.cacheEvent),
                topic: C.GoString(eventCallbackInfo.topic),
                returnCode: SolClientResponseCode(eventCallbackInfo.rc),
                subCode: SolClientSubCode(eventCallbackInfo.subCode),
                cacheRequestId: message.CacheRequestId(eventCallbackInfo.cacheRequestId),
        }
}

//export goCacheEventCallback
func goCacheEventCallback(opaqueSessionP SolClientSessionPt, eventCallbackInfo SolClientCacheEventInfoPt, userP unsafe.Pointer) {
        if callback, ok := cacheToEventCallbackMap.Load(uintptr(userP)); ok {
callback.(SolClientCacheEventCallback)(SolClientCacheEventInfoFromCoreCacheEventInfo(eventCallbackInfo, userP))
        } else {
                logging.Default.Debug("Received event callback from core API without an associated cache event callback")
        }
}
