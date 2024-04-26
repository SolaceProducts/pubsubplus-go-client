// pubsubplus-go-client
//
// Copyright 2021-2024 Solace Corporation. All rights reserved.
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
requestResponseReplyMessageReceiveCallback(solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p);

void *uintptr_to_void_p(solClient_uint64_t ptr)
{
    return (void *)ptr;
}

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
_SessionTopicSubscribeWithFlags( solClient_opaqueSession_pt opaqueSession_p,
                                const char                *topicSubscription_p,
                                solClient_subscribeFlags_t flags,
                                solClient_session_rxMsgCallbackFunc_t callback_p,
                                void                      *dispatchId_p,
                                void                      *correlationTag_p) 
{
    solClient_session_rxMsgDispatchFuncInfo_t dispatchInfo;      /* msg dispatch callback to set */
    dispatchInfo.dispatchType = SOLCLIENT_DISPATCH_TYPE_CALLBACK;
    dispatchInfo.callback_p = callback_p;
    dispatchInfo.user_p = dispatchId_p;
    dispatchInfo.rfu_p = NULL;
    return solClient_session_topicSubscribeWithDispatch ( opaqueSession_p,
                                                          flags,
                                                          topicSubscription_p,
                                                          &dispatchInfo,
                                                          correlationTag_p);
}


solClient_returnCode_t
SessionTopicSubscribeWithFlags( solClient_opaqueSession_pt opaqueSession_p,
                                const char                *topicSubscription_p,
                                solClient_subscribeFlags_t flags,
                                void                      *dispatchId_p,
                                void                      *correlationTag_p)
{
    return _SessionTopicSubscribeWithFlags ( opaqueSession_p,
                                            topicSubscription_p,
                                            flags,
                                            messageReceiveCallback,
                                            dispatchId_p,
                                            correlationTag_p );
}

solClient_returnCode_t
SessionReplyTopicSubscribeWithFlags( solClient_opaqueSession_pt opaqueSession_p,
                                const char                *topicSubscription_p,
                                solClient_subscribeFlags_t flags,
                                void                      *dispatchId_p,
                                void                      *correlationTag_p)
{
    return _SessionTopicSubscribeWithFlags ( opaqueSession_p,
                                            topicSubscription_p,
                                            flags,
                                            requestResponseReplyMessageReceiveCallback,
                                            dispatchId_p,
                                            correlationTag_p );
}

solClient_returnCode_t  
_SessionTopicUnsubscribeWithFlags(   solClient_opaqueSession_pt opaqueSession_p,
                                    const char                *topicSubscription_p,
                                    solClient_subscribeFlags_t flags,
                                    solClient_session_rxMsgCallbackFunc_t callback_p,
                                    void                      *dispatchId_p,
                                    void                      *correlationTag_p) 
{
    solClient_session_rxMsgDispatchFuncInfo_t dispatchInfo;      /* msg dispatch callback to set */
    dispatchInfo.dispatchType = SOLCLIENT_DISPATCH_TYPE_CALLBACK;
    dispatchInfo.callback_p = callback_p;
    dispatchInfo.user_p = dispatchId_p;
    dispatchInfo.rfu_p = NULL;
    return solClient_session_topicUnsubscribeWithDispatch ( opaqueSession_p,
                                                            flags,
                                                            topicSubscription_p,
                                                            &dispatchInfo,
                                                            correlationTag_p);
}

solClient_returnCode_t 
SessionTopicUnsubscribeWithFlags(   solClient_opaqueSession_pt opaqueSession_p,
                                    const char                *topicSubscription_p,
                                    solClient_subscribeFlags_t flags,
                                    void                      *dispatchId_p,
                                    void                      *correlationTag_p)
{
    return _SessionTopicUnsubscribeWithFlags ( opaqueSession_p,
                                            topicSubscription_p,
                                            flags,
                                            messageReceiveCallback,
                                            dispatchId_p,
                                            correlationTag_p );
}

solClient_returnCode_t
SessionReplyTopicUnsubscribeWithFlags(  solClient_opaqueSession_pt opaqueSession_p,
                                        const char                *topicSubscription_p,
                                        solClient_subscribeFlags_t flags,
                                        void                      *dispatchId_p,
                                        void                      *correlationTag_p)
{
    return _SessionTopicUnsubscribeWithFlags ( opaqueSession_p,
                                            topicSubscription_p,
                                            flags,
                                            requestResponseReplyMessageReceiveCallback,
                                            dispatchId_p,
                                            correlationTag_p );
}

