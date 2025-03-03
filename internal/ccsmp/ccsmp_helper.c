// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "./ccsmp_helper.h"
#include "solclient/solClient.h"
#include "solclient/solClientMsg.h"
#include "solclient/solCache.h"
#include <stdint.h>
#include <stdlib.h>

//
// external callbacks defined in ccsmp_callbacks.c
//
solClient_rxMsgCallback_returnCode_t
messageReceiveCallback(solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p);

solClient_rxMsgCallback_returnCode_t
defaultMessageReceiveCallback(solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p);

solClient_rxMsgCallback_returnCode_t
requestResponseReplyMessageReceiveCallback(solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p);

solClient_rxMsgCallback_returnCode_t
flowMessageReceiveCallback(solClient_opaqueFlow_pt opaqueFlow_p, solClient_opaqueMsg_pt msg_p, void *user_p);

void eventCallback(solClient_opaqueSession_pt opaqueSession_p, solClient_session_eventCallbackInfo_pt eventInfo_p, void *user_p);

void flowEventCallback(solClient_opaqueFlow_pt opaqueFlow_p, solClient_flow_eventCallbackInfo_pt eventInfo_p, void *user_p);

void cacheEventCallback(solClient_opaqueSession_pt opaqueSession_p, solCache_eventCallbackInfo_pt eventInfo_p, void *user_p);

void solClientgo_freeFilteringConfig(solClientgo_msgDispatchCacheRequestIdFilterInfo_pt filteringConfig_pt) {
        const int CHAR_POINTER_LEN = 1000;
        char charPointer[CHAR_POINTER_LEN];
        snprintf(charPointer, CHAR_POINTER_LEN, "freeFilteringConfig::freeing memory..");
        debugStatement(charPointer);
        free(filteringConfig_pt);
        snprintf(charPointer, CHAR_POINTER_LEN, "freeFilteringConfig::Finished freeing memory.");
        debugStatement(charPointer);
}

void solClientgo_createAndConfigureMessageFiltering(solClientgo_msgDispatchCacheRequestIdFilterInfo_pt * filteringConfig_pt, uintptr_t dispatchId, solClient_uint64_t cacheRequestId) {
        const int CHAR_POINTER_LEN = 1000;
        char charPointer[CHAR_POINTER_LEN];
        snprintf(charPointer, CHAR_POINTER_LEN, "createAndConfigureMessageFilter::Creating message filter config");
        debugStatement(charPointer);

        *filteringConfig_pt = (solClientgo_msgDispatchCacheRequestIdFilterInfo_t *)malloc(sizeof(solClientgo_msgDispatchCacheRequestIdFilterInfo_t));
        (*filteringConfig_pt)->callback_p = messageReceiveCallback;
        (*filteringConfig_pt)->dispatchID = dispatchId;
        (*filteringConfig_pt)->cacheRequestId = cacheRequestId;
        snprintf(charPointer, CHAR_POINTER_LEN, "createAndConfigureMessageFilter::filteringConfig_pt is: %p, dispatchId is %ld, cacheRequestId is %lld", *filteringConfig_pt, (**filteringConfig_pt).dispatchID, (**filteringConfig_pt).cacheRequestId);
        debugStatement(charPointer);
}


//void debugStatement(char * charPointer);

solClient_rxMsgCallback_returnCode_t
cacheFilterCallback(solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void * user_p);

solClient_returnCode_t
solClientgo_msg_isRequestReponseMsg(solClient_opaqueMsg_pt msg_p, char **correlationId_p) {
    solClient_returnCode_t rc = SOLCLIENT_FAIL;
    const char *correlationId = NULL;
    if ( correlationId_p == NULL ) {
        return rc;
    }
    if ( !solClient_msg_isReplyMsg(msg_p) ) {
        return rc;
    }
    if ( SOLCLIENT_OK != (rc = solClient_msg_getCorrelationId(msg_p, &correlationId)) ) {
        return rc;
    }
    if (!SOLCLIENTGO_HAS_REPLY_CORRELATION_ID_PREFIX(correlationId)) {
        return SOLCLIENT_FAIL;
    }
    // This string is a direct read from the message backing memory and shoud be copied into go memory for persistent use.
    *correlationId_p = (char *)correlationId;
    return SOLCLIENT_OK;
}

//typedef solClient_uint64_t solClientgo_cachedMessageFilterMethodId_t;

//typedef struct solClientgo_msgDispatchCacheRequestIdFilterInfo {
//        solClient_uint64_t cacheRequestId;
//        solClient_session_rxMsgCallbackFunc_t callback_p;
//        uintptr dispatchID;
//        solClientgo_cachedMessageFilterMethodId_t filterMethodId;
//} solClientgo_msgDispatchCacheRequestIdFilterInfo_t;

solClient_returnCode_t
solClientgo_filterCachedMessageByCacheRequestId(solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void * user_p) {
        const int CHAR_POINTER_LEN = 1000;
        char charPointer[CHAR_POINTER_LEN];
        solClient_uint64_t foundCacheRequestId = 0;
        solClient_returnCode_t ret = SOLCLIENT_FAIL;
        solClientgo_msgDispatchCacheRequestIdFilterInfo_t * info_p = (solClientgo_msgDispatchCacheRequestIdFilterInfo_t *)user_p;
        if ( solClient_msg_getCacheRequestId(msg_p, &foundCacheRequestId) != SOLCLIENT_OK) {
                /* Failed operation so inform API message can be discarded. */
                printf("Failed to retrieve cacheRequestId from msg\n");
                return SOLCLIENT_FAIL;
        }
        snprintf(charPointer, CHAR_POINTER_LEN, "Successfully found cacheRequestId from msg");
        debugStatement(charPointer);
        snprintf(charPointer, CHAR_POINTER_LEN, "info_p->cacheRequestId is %lld, foundCacheRequestId is: %lld", info_p->cacheRequestId, foundCacheRequestId);
        debugStatement(charPointer);
        if ( info_p->cacheRequestId != foundCacheRequestId ) {
                printf("Failed to match cacheRequestId from msg\n");
                return SOLCLIENT_NOT_FOUND;
        }
        snprintf(charPointer, CHAR_POINTER_LEN, "Successfully matched cacheRequestId from msg");
        debugStatement(charPointer);
        return SOLCLIENT_OK;
}
/*
solClient_returnCode_t
solClientgo_getCachedMessageFilterMethod(solClientgo_cachedMessageFilterMethodId_t methodId, void ** callback_pt_pt) {
        solClient_returnCode_t ret = SOLCLIENT_FAIL;
        switch ( methodId ) {
                case DO_NOT_FILTER_CACHED_MESSAGES:
                        ret = SOLCLIENT_OK;
                        break;
                case FILTER_CACHED_MESSAGE_BY_CACHE_REQUEST_ID:
                        ret = SOLCLIENT_OK;
                        *callback_pt_pt = (void *)&solClientgo_filterCachedMessageByCacheRequestId;
                        break;
                default:
                        ret = SOLCLIENT_NOT_FOUND;
                        break;
        }
        return ret;
}
*/
//const solClientgo_cachedMessageFilterMethodId_t DO_NOT_FILTER_CACHED_MESSAGES = 0;
//const solClientgo_cachedMessageFilterMethodId_t FILTER_CACHED_MESSAGE_BY_CACHE_REQUEST_ID = 1;

solClient_returnCode_t
SessionCreate( solClient_propertyArray_pt sessionPropsP,
                solClient_opaqueContext_pt contextP,
                solClient_opaqueSession_pt *opaqueSession_p)
{
        /* allocate the session create struct */
        solClient_session_createFuncInfo_t sessionCreateFuncInfo;
        sessionCreateFuncInfo.rxMsgInfo.callback_p = (solClient_session_rxMsgCallbackFunc_t)defaultMessageReceiveCallback;
        sessionCreateFuncInfo.rxMsgInfo.user_p = NULL;
        sessionCreateFuncInfo.eventInfo.callback_p = (solClient_session_eventCallbackFunc_t)eventCallback;
        sessionCreateFuncInfo.eventInfo.user_p = NULL;
        // allocate thse struct fields to NULL too
        sessionCreateFuncInfo.rxInfo.user_p = NULL;
        sessionCreateFuncInfo.rxInfo.callback_p = NULL;

        return solClient_session_create(sessionPropsP, contextP, opaqueSession_p, &sessionCreateFuncInfo, sizeof(sessionCreateFuncInfo));
}

solClient_returnCode_t
SessionContextCreate( solClient_propertyArray_pt contextPropsP,
                        solClient_opaqueContext_pt *contextP)
{
        /* allocate the session context create struct to NULL */
        solClient_context_createFuncInfo_t contextCreateFuncInfo;
        contextCreateFuncInfo.regFdInfo.user_p = NULL;
        contextCreateFuncInfo.regFdInfo.regFdFunc_p = NULL;
        contextCreateFuncInfo.regFdInfo.unregFdFunc_p = NULL;

        return solClient_context_create(contextPropsP, contextP, &contextCreateFuncInfo, sizeof(contextCreateFuncInfo));
}

solClient_returnCode_t  
SessionFlowCreate( solClient_opaqueSession_pt   opaqueSession_p,
                    solClient_propertyArray_pt  flowPropsP,
                    solClient_opaqueFlow_pt     *opaqueFlow_p,
                    solClient_uint64_t          flowID) 
{
    /* set the flowID in the flow create struct */
    solClient_flow_createFuncInfo_t flowCreateFuncInfo;
	flowCreateFuncInfo.rxMsgInfo.callback_p = flowMessageReceiveCallback;
	flowCreateFuncInfo.rxMsgInfo.user_p = (void *)flowID;
	flowCreateFuncInfo.eventInfo.callback_p = (solClient_flow_eventCallbackFunc_t)flowEventCallback;
	flowCreateFuncInfo.eventInfo.user_p = (void *)flowID;
    // allocate these struct fields too
	flowCreateFuncInfo.rxInfo.user_p = NULL;
	flowCreateFuncInfo.rxInfo.callback_p = NULL;

    return solClient_session_createFlow(flowPropsP, opaqueSession_p, opaqueFlow_p, &flowCreateFuncInfo, sizeof(flowCreateFuncInfo));
}

solClient_returnCode_t  
FlowTopicSubscribeWithDispatch( solClient_opaqueFlow_pt opaqueFlow_p,
                                solClient_subscribeFlags_t flags,
                                const char              *topicSubscription_p,
                                solClient_flow_rxMsgDispatchFuncInfo_t *dispatchFuncInfo_p,
                                solClient_uint64_t      correlationTag) 
{
    return solClient_flow_topicSubscribeWithDispatch( opaqueFlow_p,
                                                        flags,
                                                        topicSubscription_p,
                                                        dispatchFuncInfo_p,
                                                        (void *)correlationTag);
}

solClient_returnCode_t  
FlowTopicUnsubscribeWithDispatch(  solClient_opaqueFlow_pt opaqueFlow_p,
                                    solClient_subscribeFlags_t flags,
                                    const char              *topicSubscription_p,
                                    solClient_flow_rxMsgDispatchFuncInfo_t *dispatchFuncInfo_p,
                                    solClient_uint64_t      correlationTag) 
{
    return solClient_flow_topicUnsubscribeWithDispatch( opaqueFlow_p,
                                                        flags,
                                                        topicSubscription_p,
                                                        dispatchFuncInfo_p,
                                                        (void *)correlationTag);
}

solClient_returnCode_t  
_SessionTopicSubscribeWithFlags( solClient_opaqueSession_pt             opaqueSession_p,
                                const char                              *topicSubscription_p,
                                solClient_subscribeFlags_t              flags,
                                solClient_session_rxMsgCallbackFunc_t   callback_p,
                                solClient_uint64_t                      dispatchId,
                                solClient_uint64_t                      correlationTag) 
{
    solClient_session_rxMsgDispatchFuncInfo_t dispatchInfo;      /* msg dispatch callback to set */
    dispatchInfo.dispatchType = SOLCLIENT_DISPATCH_TYPE_CALLBACK;
    dispatchInfo.callback_p = callback_p;
    dispatchInfo.user_p = (void *)dispatchId;
    dispatchInfo.rfu_p = NULL;
    return solClient_session_topicSubscribeWithDispatch ( opaqueSession_p,
                                                          flags,
                                                          topicSubscription_p,
                                                          &dispatchInfo,
                                                          (void *)correlationTag);
}

solClient_returnCode_t
SessionTopicSubscribeWithFlags( solClient_opaqueSession_pt opaqueSession_p,
                                const char                *topicSubscription_p,
                                solClient_subscribeFlags_t flags,
                                solClient_uint64_t  dispatchId,
                                solClient_uint64_t  correlationTag)
{
    return _SessionTopicSubscribeWithFlags ( opaqueSession_p,
                                            topicSubscription_p,
                                            flags,
                                            messageReceiveCallback,
                                            dispatchId,
                                            correlationTag );
}

solClient_returnCode_t
SessionReplyTopicSubscribeWithFlags( solClient_opaqueSession_pt opaqueSession_p,
                                const char                *topicSubscription_p,
                                solClient_subscribeFlags_t flags,
                                solClient_uint64_t      dispatchId,
                                solClient_uint64_t      correlationTag)
{
    return _SessionTopicSubscribeWithFlags ( opaqueSession_p,
                                            topicSubscription_p,
                                            flags,
                                            requestResponseReplyMessageReceiveCallback,
                                            dispatchId,
                                            correlationTag );
}

solClient_returnCode_t  
_SessionTopicUnsubscribeWithFlags(   solClient_opaqueSession_pt opaqueSession_p,
                                    const char                *topicSubscription_p,
                                    solClient_subscribeFlags_t flags,
                                    solClient_session_rxMsgCallbackFunc_t callback_p,
                                    solClient_uint64_t      dispatchId,
                                    solClient_uint64_t      correlationTag) 
{
    solClient_session_rxMsgDispatchFuncInfo_t dispatchInfo;      /* msg dispatch callback to set */
    dispatchInfo.dispatchType = SOLCLIENT_DISPATCH_TYPE_CALLBACK;
    dispatchInfo.callback_p = callback_p;
    dispatchInfo.user_p = (void *)dispatchId;
    dispatchInfo.rfu_p = NULL;
    return solClient_session_topicUnsubscribeWithDispatch ( opaqueSession_p,
                                                            flags,
                                                            topicSubscription_p,
                                                            &dispatchInfo,
                                                            (void *)correlationTag);
}

solClient_returnCode_t
SessionCacheTopicUnsubscribeWithFlags(solClient_opaqueSession_pt opaqueSession_p,
                const char *topicSubscription_p,
                solClient_subscribeFlags_t flags,
                solClient_uint64_t dispatchId,
                solClient_uint64_t correlationTag)
{
        return _SessionTopicUnsubscribeWithFlags (opaqueSession_p,
                        topicSubscription_p,
                        flags,
                        cacheFilterCallback,
                        dispatchId,
                        correlationTag);
}


solClient_returnCode_t 
SessionTopicUnsubscribeWithFlags(   solClient_opaqueSession_pt opaqueSession_p,
                                    const char                *topicSubscription_p,
                                    solClient_subscribeFlags_t flags,
                                    solClient_uint64_t      dispatchId,
                                    solClient_uint64_t      correlationTag)
{
    return _SessionTopicUnsubscribeWithFlags ( opaqueSession_p,
                                            topicSubscription_p,
                                            flags,
                                            messageReceiveCallback,
                                            dispatchId,
                                            correlationTag );
}

solClient_returnCode_t
SessionReplyTopicUnsubscribeWithFlags(  solClient_opaqueSession_pt opaqueSession_p,
                                        const char                *topicSubscription_p,
                                        solClient_subscribeFlags_t flags,
                                        solClient_uint64_t      dispatchId,
                                        solClient_uint64_t      correlationTag)
{
    return _SessionTopicUnsubscribeWithFlags ( opaqueSession_p,
                                            topicSubscription_p,
                                            flags,
                                            requestResponseReplyMessageReceiveCallback,
                                            dispatchId,
                                            correlationTag );
}

solClient_returnCode_t  
SessionTopicEndpointUnsubscribeWithFlags(  solClient_opaqueSession_pt opaqueSession_p,
                                    solClient_propertyArray_pt  endpointProps,
                                    solClient_subscribeFlags_t flags,
                                    const char              *topicSubscription_p,
                                    solClient_uint64_t      correlationTag) 
{
    return solClient_session_endpointTopicUnsubscribe( endpointProps,
                                                        opaqueSession_p,
                                                        flags,
                                                        topicSubscription_p,
                                                        (void *)correlationTag);
}

solClient_returnCode_t  
SessionEndpointProvisionWithFlags(  solClient_opaqueSession_pt opaqueSession_p,
                                    solClient_propertyArray_pt  endpointProps,
                                    solClient_uint32_t flags,
                                    solClient_uint64_t      correlationTag) 
{
    return solClient_session_endpointProvision( endpointProps,
                                                        opaqueSession_p,
                                                        flags,
                                                        (void *)correlationTag,
                                                        NULL,
                                                        0);
}

solClient_returnCode_t  
SessionEndpointDeprovisionWithFlags(  solClient_opaqueSession_pt opaqueSession_p,
                                    solClient_propertyArray_pt  endpointProps,
                                    solClient_uint32_t flags,
                                    solClient_uint64_t      correlationTag) 
{
    return solClient_session_endpointDeprovision( endpointProps,
                                                        opaqueSession_p,
                                                        flags,
                                                        (void *)correlationTag);
}

solClient_returnCode_t
SessionCreateCacheSession(
        solClient_propertyArray_pt cacheSessionProps_p,
        solClient_opaqueSession_pt opaqueSession_p,
        solClient_opaqueCacheSession_pt * opaqueCacheSession_p)
{
        return solClient_session_createCacheSession((const char * const *)cacheSessionProps_p, opaqueSession_p, opaqueCacheSession_p);
}

solClient_returnCode_t
CacheSessionSendCacheRequest(
        uintptr_t dispatchId,
        solClient_opaqueCacheSession_pt opaqueCacheSession_p,
        const char * topic_p,
        solClient_uint64_t cacheRequestId,
        solClient_cacheRequestFlags_t cacheFlags,
        solClient_subscribeFlags_t subscribeFlags,
        solClientgo_msgDispatchCacheRequestIdFilterInfo_pt filterConfig_p)
        //solClient_bool_t withFiltering)
{
        const int CHAR_POINTER_LEN = 1000;
        char charPointer[CHAR_POINTER_LEN];
        snprintf(charPointer, CHAR_POINTER_LEN, "Got to CacheSessionSendCacheRequest");
        debugStatement(charPointer);
        solClient_session_rxMsgDispatchFuncInfo_t dispatchInfo;      /* msg dispatch callback to set */
        if ( filterConfig_p != NULL ) {
                /* NOTE: If the filter config is not NULL, we take its configuration to mean that
                 * it should be taken into account when sending the cache request.
                 */

//                solClientgo_msgDispatchCacheRequestIdFilterInfo_t * cacheFilterInfo_pt = (solClientgo_msgDispatchCacheRequestIdFilterInfo_t *)malloc(sizeof(solClientgo_msgDispatchCacheRequestIdFilterInfo_t));
//                cacheFilterInfo_pt->callback_p = messageReceiveCallback;
//                cacheFilterInfo_pt->dispatchID = dispatchId;
//                cacheFilterInfo_pt->cacheRequestId = cacheRequestId;

                dispatchInfo.callback_p = cacheFilterCallback;
                //dispatchInfo.user_p = (void *)&cacheFilterInfo;
                dispatchInfo.user_p = (void *)filterConfig_p;
                snprintf(charPointer,
                         CHAR_POINTER_LEN,
                         "CacheSessionSendCacheRequest::messageReceiveCallback is %p",
                         messageReceiveCallback);
                debugStatement(charPointer);

                snprintf(charPointer,
                         CHAR_POINTER_LEN,
                         "CacheSessionSendCacheRequest::cacheFilterCallback is %p",
                         cacheFilterCallback);
                debugStatement(charPointer);

                snprintf(charPointer,
                         CHAR_POINTER_LEN,
                         "CacheSessionSendCacheRequest::cacheFilterInfo.callback_p is %p",
                         filterConfig_p->callback_p);
                         //cacheFilterInfo.callback_p);
                debugStatement(charPointer);

                snprintf(charPointer,
                         CHAR_POINTER_LEN,
                         "CacheSessionSendCacheRequest::user_p is: %p",
                         dispatchInfo.user_p);
                debugStatement(charPointer);

        } else {
                dispatchInfo.callback_p = messageReceiveCallback;
                dispatchInfo.user_p = (void *)dispatchId;
        }
        dispatchInfo.dispatchType = SOLCLIENT_DISPATCH_TYPE_CALLBACK;
        dispatchInfo.rfu_p = NULL;

        return solClient_cacheSession_sendCacheRequestWithDispatch(
                opaqueCacheSession_p,
                topic_p,
                cacheRequestId,
                (solCache_eventCallbackFunc_t)cacheEventCallback,
                /* NOTE: CCSMP does not copy the contents of user_p, only the pointer. This means we cannot have
                 * Go-allocated memory as the object being pointed to, since that object might be garbage
                 * collected before the cache response is received and the user_p is used for some purpose
                 * by either the CCSMP or PSPGo APIs. We also cannot allocate a struct for user_p in C since
                 * it will go out of scope by the end of this function and we won't be able to clean it up
                 * properly. This means that our only option, AFAIK, is to just pass the cache session
                 * pointer as a void pointer, since its lifecycle is managed outside of this function in a
                 * safe way, and must survive at least until CCSMP receives a cache response or the request
                 * is cancelled. While destroying the cache session, the user_p/opaqueCacheSession_p will be
                 * removed from the tables anyways. Cancelling a cache request is always followed by destroying
                 * the cache associated session.
                 * */
                (void *)opaqueCacheSession_p,
                cacheFlags,
                subscribeFlags,
                &dispatchInfo);
}

solClient_returnCode_t
CacheSessionDestroy(solClient_opaqueCacheSession_pt * opaqueCacheSession_p) {
        return solClient_cacheSession_destroy(opaqueCacheSession_p);
}

solClient_returnCode_t
CacheSessionCancelRequests(solClient_opaqueCacheSession_pt opaqueCacheSession_p) {
        return solClient_cacheSession_cancelCacheRequests(opaqueCacheSession_p);
}


