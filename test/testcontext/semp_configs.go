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

package testcontext

import (
	"solace.dev/go/messaging/test/sempclient/config"
)

const defaultClientProfileName = "default"
const defaultClientUsernamePassword = "default"
const defaultClientUsername = "default"

func GetDefaultCacheMessageVpnConfig(msgVpnName string) config.MsgVpn {
	return config.MsgVpn{
		AuthenticationBasicEnabled:     True,
		AuthenticationBasicProfileName: "",
		AuthenticationBasicType:        "none",
		Enabled:                        True,
		MaxMsgSpoolUsage:               0,
		MsgVpnName:                     msgVpnName,
	}
}

func GetDefaultCacheMessageVpnClientProfileConfig(msgVpnName string) config.MsgVpnClientProfile {
	return config.MsgVpnClientProfile{
		ClientProfileName:                    defaultClientProfileName,
		AllowBridgeConnectionsEnabled:        True,
		AllowGuaranteedEndpointCreateEnabled: True,
		AllowGuaranteedMsgReceiveEnabled:     True,
		AllowGuaranteedMsgSendEnabled:        True,
		MsgVpnName:                           msgVpnName,
	}
}

func GetDefaultCacheMessageVpnClientUsernameConfig(msgVpnName string) config.MsgVpnClientUsername {
	return config.MsgVpnClientUsername{
		Password:          defaultClientUsernamePassword,
		ClientUsername:    defaultClientUsername,
		MsgVpnName:        msgVpnName,
		Enabled:           True,
		ClientProfileName: defaultClientProfileName,
	}
}

func GetMessageVpnOperationalStateConfig(msgVpnName string, enabled bool) config.MsgVpn {
	return config.MsgVpn{
		Enabled:    BoolPointer(enabled),
		MsgVpnName: msgVpnName,
	}
}
