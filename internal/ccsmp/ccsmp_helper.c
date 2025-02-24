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
