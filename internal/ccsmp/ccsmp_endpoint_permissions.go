// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
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
#include "solclient/solClient.h"
*/
import "C"

const (
	// SolClientEndpointPermissionNone: with no permissions
	SolClientEndpointPermissionNone = C.SOLCLIENT_ENDPOINT_PERM_NONE
	// SolClientEndpointPermissionReadOnly: with read-only permission
	SolClientEndpointPermissionReadOnly = C.SOLCLIENT_ENDPOINT_PERM_READ_ONLY
	// SolClientEndpointPermissionConsume: with consume permission
	SolClientEndpointPermissionConsume = C.SOLCLIENT_ENDPOINT_PERM_CONSUME
	// SolClientEndpointPermissionTopic: with Modify Topic permission
	SolClientEndpointPermissionModifyTopic = C.SOLCLIENT_ENDPOINT_PERM_MODIFY_TOPIC
	// SolClientEndpointPermissionDelete: with Delete (all) permissions
	SolClientEndpointPermissionDelete = C.SOLCLIENT_ENDPOINT_PERM_DELETE
)
