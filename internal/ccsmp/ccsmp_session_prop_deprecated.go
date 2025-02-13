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

// This file is hand-maintained with relevant session properties from solClientDeprecated.h

/*
#cgo CFLAGS: -DSOLCLIENT_PSPLUS_GO
#include "solclient/solClient.h"
#include "solclient/solClientDeprecated.h"
*/
import "C"

const (
	// SolClientSessionPropSslExcludedProtocols: This property specifies a comma separated list of excluded SSL protocol(s). Valid SSL protocols are 'SSLv3', 'TLSv1', 'TLSv1.1', 'TLSv1.2'. Default: ::SOLCLIENT_SESSION_PROP_DEFAULT_SSL_EXCLUDED_PROTOCOLS.
	SolClientSessionPropSslExcludedProtocols = C.SOLCLIENT_SESSION_PROP_SSL_EXCLUDED_PROTOCOLS
)
