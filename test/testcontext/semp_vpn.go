
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
        "fmt"

        "solace.dev/go/messaging/test/sempclient/config"
)

func CreateMessageVpnIfNotPresent(semp SEMPv2, msgVpnConfig config.MsgVpn) error {
        vpn, httpResponse, err := semp.Monitor().MsgVpnApi.GetMsgVpn(semp.MonitorCtx(), msgVpnConfig.MsgVpnName, nil)
                if httpResponse.StatusCode == 400 {
        /* VPN does not exist, so create it */
                        _, _, err = semp.Config().MsgVpnApi.CreateMsgVpn(semp.ConfigCtx(), msgVpnConfig, nil)
                        if err != nil {
                                return err
                        }
                } else if httpResponse.StatusCode == 200 {
                        /* VPN exists so check if it is enabled. */
                        if !**vpn.Data.Enabled {
                               return fmt.Errorf("vpn %s is present but not enabled", vpn.Data.MsgVpnName)
                        }
                } else {
                        return fmt.Errorf("encountered unexpected HTTP response code %d with nil error", httpResponse.StatusCode)
                }
        return err
}

func UpdateMsgVpnClientProfile(semp SEMPv2, msgVpnClientProfileConfig config.MsgVpnClientProfile) error {
        _, _, err := semp.Config().MsgVpnApi.UpdateMsgVpnClientProfile(semp.ConfigCtx(),
                                msgVpnClientProfileConfig,
                                msgVpnClientProfileConfig.MsgVpnName,
                                msgVpnClientProfileConfig.ClientProfileName,
                                nil)
        return err
}

func UpdateMsgVpnClientUsername(semp SEMPv2, msgVpnClientUsername config.MsgVpnClientUsername) error {
        _, _, err := semp.Config().MsgVpnApi.UpdateMsgVpnClientUsername(semp.ConfigCtx(),
                                msgVpnClientUsername,
                                msgVpnClientUsername.MsgVpnName,
                                msgVpnClientUsername.ClientProfileName,
                                nil)
                                return err

}

func UpdateMsgVpnOperationalState(semp SEMPv2, msgVpnConfig config.MsgVpn) error {
        _, _, err := semp.Config().MsgVpnApi.UpdateMsgVpn(semp.ConfigCtx(),
                                msgVpnConfig,
                                msgVpnConfig.MsgVpnName,
                                nil)
                                return err

}

func UpdateMessageVpn(semp SEMPv2, msgVpnClientProfileConfig config.MsgVpnClientProfile, msgVpnClientUsername config.MsgVpnClientUsername, msgVpnOperationalStateConfig config.MsgVpn) error {
        err := UpdateMsgVpnClientProfile(semp, msgVpnClientProfileConfig)
        if err != nil {
                return err
        }
        err = UpdateMsgVpnClientUsername(semp, msgVpnClientUsername)
        if err != nil {
                return err
        }
        err = UpdateMsgVpnOperationalState(semp, msgVpnOperationalStateConfig)
        return err
}
