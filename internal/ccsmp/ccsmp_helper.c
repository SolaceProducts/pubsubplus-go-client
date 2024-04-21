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

//
// external callbacks defined in ccsmp_callbacks.c
//
solClient_rxMsgCallback_returnCode_t
messageReceiveCallback(solClient_opaqueSession_pt opaqueSession_p, solClient_opaqueMsg_pt msg_p, void *user_p);

void *uintptr_to_void_p(solClient_uint64_t ptr)
{
    return (void *)ptr;
}

solClient_returnCode_t  SessionTopicSubscribe(
                        solClient_opaqueSession_pt opaqueSession_p,
                        const char                *topicSubscription_p,
                        void                      *dispatchId_p,
                        void                      *correlationTag_p) 
{
    solClient_session_rxMsgDispatchFuncInfo_t dispatchInfo;      /* msg dispatch callback to set */
    dispatchInfo.dispatchType = SOLCLIENT_DISPATCH_TYPE_CALLBACK;
    dispatchInfo.callback_p = messageReceiveCallback;
    dispatchInfo.user_p = dispatchId_p;
    dispatchInfo.rfu_p = NULL;
    return solClient_session_topicSubscribeWithDispatch ( opaqueSession_p,
                                                          SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
                                                          topicSubscription_p,
                                                          &dispatchInfo,
                                                          correlationTag_p);
}

solClient_returnCode_t  SessionTopicUnsubscribe(
                        solClient_opaqueSession_pt opaqueSession_p,
                        const char                *topicSubscription_p,
                        void                      *dispatchId_p,
                        void                      *correlationTag_p) 
{
    solClient_session_rxMsgDispatchFuncInfo_t dispatchInfo;      /* msg dispatch callback to set */
    dispatchInfo.dispatchType = SOLCLIENT_DISPATCH_TYPE_CALLBACK;
    dispatchInfo.callback_p = messageReceiveCallback;
    dispatchInfo.user_p = dispatchId_p;
    dispatchInfo.rfu_p = NULL;
    return solClient_session_topicUnsubscribeWithDispatch ( opaqueSession_p,
                                                            SOLCLIENT_SUBSCRIBE_FLAGS_REQUEST_CONFIRM,
                                                            topicSubscription_p,
                                                            &dispatchInfo,
                                                            correlationTag_p);
}
