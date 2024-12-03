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
#include "solclient/solCache.h"
*/
import "C"
import (
	"solace.dev/go/messaging/pkg/solace/resource"
)

// Mapping for Cached message Subscription Request Strategies to respectiveCCSMP flags
var cachedMessageSubscriptionRequestStrategyMappingToCCSMP = map[resource.CachedMessageSubscriptionStrategy]C.solClient_cacheRequestFlags_t{
	resource.AsAvailable:       C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FLOWTHRU | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY,
	resource.LiveCancelsCached: C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FULFILL | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY,
	resource.CachedFirst:       C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_QUEUE | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY,
	resource.CachedOnly:        C.SOLCLIENT_CACHEREQUEST_FLAGS_LIVEDATA_FLOWTHRU | C.SOLCLIENT_CACHEREQUEST_FLAGS_NOWAIT_REPLY,
}
