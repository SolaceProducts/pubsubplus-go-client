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

// Code generated by ccsmp_log_level_generator.go via go generate. DO NOT EDIT.

/*
#cgo CFLAGS: -DSOLCLIENT_PSPLUS_GO
#include "solclient/solClient.h"
*/
import "C"

const (
	// SolClientLogLevelEmergency: This level is not used by the API.
	SolClientLogLevelEmergency SolClientLogLevel = C.SOLCLIENT_LOG_EMERGENCY
	// SolClientLogLevelAlert: This level is not used by the API.
	SolClientLogLevelAlert SolClientLogLevel = C.SOLCLIENT_LOG_ALERT
	// SolClientLogLevelCritical: A serious error that can make the API unusable.
	SolClientLogLevelCritical SolClientLogLevel = C.SOLCLIENT_LOG_CRITICAL
	// SolClientLogLevelError: An unexpected condition within the API that can affect its operation.
	SolClientLogLevelError SolClientLogLevel = C.SOLCLIENT_LOG_ERROR
	// SolClientLogLevelWarning: An unexpected condition within the API that is not expected to affect its operation.
	SolClientLogLevelWarning SolClientLogLevel = C.SOLCLIENT_LOG_WARNING
	// SolClientLogLevelNotice: Significant informational messages about the normal operation of the API. These messages are never output in the normal process of sending or receiving a message from the broker.
	SolClientLogLevelNotice SolClientLogLevel = C.SOLCLIENT_LOG_NOTICE
	// SolClientLogLevelInfo: Informational messages about the normal operation of the API. These might include information related to sending or receiving messages from the broker.
	SolClientLogLevelInfo SolClientLogLevel = C.SOLCLIENT_LOG_INFO
	// SolClientLogLevelDebug: Debugging information generally useful to API developers (very verbose).
	SolClientLogLevelDebug SolClientLogLevel = C.SOLCLIENT_LOG_DEBUG
)
