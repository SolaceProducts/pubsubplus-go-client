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

#ifndef CCSMP_HELPER_H
#define CCSMP_HELPER_H

#include "solclient/solClient.h"

// Reexport error info fields as they need to be copied.
// Since only a single error info struct will be returned,
// we add the ReturnCode field. Fields are capitalized to
// allow them to be exported by the CCSMP package.
typedef struct solClient_errorInfo_wrapper
{
    solClient_returnCode_t ReturnCode;
    solClient_errorInfo_t * DetailedErrorInfo;
} solClient_errorInfo_wrapper_t;

/**
 * uintptr_to_void_p takes in a uintptr and returns
 * a void pointer containing the data of ptr
 * 
 * this is used to get around "possible misuse of unsafe.Pointer"
 * when not storing a pointer in a void* but instead using the uintptr_t 
 * size of data 
 * 
 * this function should NEVER be used to convert a real pointer into a
 * void pointer, in all other cases unsafe.Pointer should be used.
 * 
 * Note that right now only uint64 is accepted. In the future, if 32 bit
 * operating systems are supported, this may need to change to a more complex
 * definition.
 */
solClient_returnCode_t  SessionCreate(
                        solClient_propertyArray_pt sessionPropsP,
                        solClient_opaqueContext_pt contextP,
                        solClient_opaqueSession_pt *opaqueSession_p);

solClient_returnCode_t  SessionContextCreate(
                        solClient_propertyArray_pt contextPropsP,
                        solClient_opaqueContext_pt *contextP);

solClient_returnCode_t  SessionFlowCreate(
                        solClient_opaqueSession_pt      opaqueSession_p,
                        solClient_propertyArray_pt      flowPropsP,
                        solClient_opaqueFlow_pt         *opaqueFlow_p,
                        solClient_uint64_t              flowID_p);

solClient_returnCode_t  FlowTopicSubscribeWithDispatch(
                        solClient_opaqueFlow_pt opaqueFlow_p,
                        solClient_subscribeFlags_t flags,
                        const char              *topicSubscription_p,
                        solClient_flow_rxMsgDispatchFuncInfo_t *dispatchFuncInfo_p,
                        solClient_uint64_t      correlationTag);

solClient_returnCode_t  FlowTopicUnsubscribeWithDispatch(
                        solClient_opaqueFlow_pt opaqueFlow_p,
                        solClient_subscribeFlags_t flags,
                        const char              *topicSubscription_p,
                        solClient_flow_rxMsgDispatchFuncInfo_t *dispatchFuncInfo_p,
                        solClient_uint64_t      correlationTag);

solClient_returnCode_t  SessionTopicSubscribeWithFlags(
                        solClient_opaqueSession_pt  opaqueSession_p,
                        const char                  *topicSubscription_p,
                        solClient_subscribeFlags_t  flags,
                        solClient_uint64_t          dispatchId,
                        solClient_uint64_t          correlationTag);

solClient_returnCode_t  SessionTopicUnsubscribeWithFlags(
                        solClient_opaqueSession_pt  opaqueSession_p,
                        const char                  *topicSubscription_p,
                        solClient_subscribeFlags_t  flags,
                        solClient_uint64_t          dispatchId,
                        solClient_uint64_t          correlationTag);

solClient_returnCode_t  SessionReplyTopicSubscribeWithFlags(
                        solClient_opaqueSession_pt  opaqueSession_p,
                        const char                  *topicSubscription_p,
                        solClient_subscribeFlags_t  flags,
                        solClient_uint64_t          dispatchId,
                        solClient_uint64_t          correlationTag);

solClient_returnCode_t  SessionReplyTopicUnsubscribeWithFlags(
                        solClient_opaqueSession_pt  opaqueSession_p,
                        const char                  *topicSubscription_p,
                        solClient_subscribeFlags_t  flags,
                        solClient_uint64_t          dispatchId,
                        solClient_uint64_t          correlationTag);

solClient_returnCode_t  SessionTopicEndpointUnsubscribeWithFlags(
                        solClient_opaqueSession_pt  opaqueSession_p,
                        solClient_propertyArray_pt  endpointProps,
                        solClient_subscribeFlags_t flags,
                        const char              *topicSubscription_p,
                        solClient_uint64_t      correlationTag);

solClient_returnCode_t  SessionEndpointProvisionWithFlags(
                        solClient_opaqueSession_pt  opaqueSession_p,
                        solClient_propertyArray_pt  endpointProps,
                        solClient_uint32_t  flags,
                        solClient_uint64_t          correlationTag);

solClient_returnCode_t  SessionEndpointDeprovisionWithFlags(
                        solClient_opaqueSession_pt  opaqueSession_p,
                        solClient_propertyArray_pt  endpointProps,
                        solClient_uint32_t  flags,
                        solClient_uint64_t          correlationTag);

/**
 * Definition of solclientgo correlation prefix
 */
#define SOLCLIENTGO_REPLY_CORRELATION_PREFIX "#GOS"

/**
 * Macro for determining if a message correlation has the solclientgo correlation prefix
 * corrId_p correlation id pointer/expression, must not be NULL.
 *          Should be a utf8 null terminal string, any string that is not null terminal must
 *          have a buffer size greater then 4.
 */
#define SOLCLIENTGO_HAS_REPLY_CORRELATION_ID_PREFIX(corrId_p) (   \
    (corrId_p)[0] == (SOLCLIENTGO_REPLY_CORRELATION_PREFIX)[0] && \
    (corrId_p)[1] == (SOLCLIENTGO_REPLY_CORRELATION_PREFIX)[1] && \
    (corrId_p)[2] == (SOLCLIENTGO_REPLY_CORRELATION_PREFIX)[2] && \
    (corrId_p)[3] == (SOLCLIENTGO_REPLY_CORRELATION_PREFIX)[3]    \
    )

solClient_returnCode_t
solClientgo_msg_isRequestReponseMsg(solClient_opaqueMsg_pt msg_p, char **correlationId_p);

#endif
