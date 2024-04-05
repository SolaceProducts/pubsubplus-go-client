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
