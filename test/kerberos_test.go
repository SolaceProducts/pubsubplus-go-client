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

package test

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/helpers"
	sempconfig "solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/sempclient/monitor"
	"solace.dev/go/messaging/test/testcontext"
)

var _ = Describe("Kerberos", func() {

	var builder solace.MessagingServiceBuilder

	BeforeEach(func() {
		if !testcontext.Kerberos() {
			Skip("No kerberos server available, skipping Kerberos tests")
		}
		builder = messaging.NewMessagingServiceBuilder().
			FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyVPNName:     testcontext.Messaging().VPN,
				config.TransportLayerPropertyHost: fmt.Sprintf("tcp://%s:%d", testcontext.Messaging().Host, testcontext.Messaging().MessagingPorts.PlaintextPort),
			})
	})

	Context("with Kerberos enabled on the target VPN", func() {
		BeforeEach(func() {
			_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(), sempconfig.MsgVpn{
				AuthenticationKerberosEnabled: helpers.True,
				AuthenticationBasicEnabled:    helpers.False,
			}, testcontext.Messaging().VPN, nil)
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(), sempconfig.MsgVpn{
				AuthenticationKerberosEnabled: helpers.False,
				AuthenticationBasicEnabled:    helpers.True,
			}, testcontext.Messaging().VPN, nil)
			Expect(err).ToNot(HaveOccurred())
		})

		It("can connect using Kerberos when configured with a strategy", func() {
			helpers.TestConnectDisconnectMessagingService(builder.
				WithAuthenticationStrategy(config.KerberosAuthentication("")))
		})

		It("can connect using Kerberos when configured using properties", func() {
			helpers.TestConnectDisconnectMessagingService(builder.
				FromConfigurationProvider(config.ServicePropertyMap{
					config.AuthenticationPropertyScheme: config.AuthenticationSchemeKerberos,
				}))
		})

		It("can connect using Kerberos using a service name when configured with a strategy", func() {
			helpers.TestConnectDisconnectMessagingService(builder.
				WithAuthenticationStrategy(config.KerberosAuthentication("solace")))
		})

		It("can connect using Kerberos using a service name when configured using properties", func() {
			helpers.TestConnectDisconnectMessagingService(builder.
				FromConfigurationProvider(config.ServicePropertyMap{
					config.AuthenticationPropertyScheme:                     config.AuthenticationSchemeKerberos,
					config.AuthenticationPropertySchemeKerberosInstanceName: "solace",
				}))
		})

		It("fails to connect using an invalid service name", func() {
			helpers.TestFailedConnectMessagingService(builder.
				WithAuthenticationStrategy(config.KerberosAuthentication("invalid_service_name")), func(err error) {
				helpers.ValidateError(err, &solace.AuthenticationError{})
			})
		})

		Context("with a client username created", func() {
			const clientUsername = "kerberos_user"

			BeforeEach(func() {
				_, _, err := testcontext.SEMP().Config().ClientUsernameApi.CreateMsgVpnClientUsername(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientUsername{
						ClientUsername: clientUsername,
						Enabled:        helpers.True,
					},
					testcontext.Messaging().VPN, nil,
				)
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				_, _, err := testcontext.SEMP().Config().ClientUsernameApi.DeleteMsgVpnClientUsername(
					testcontext.SEMP().ConfigCtx(),
					testcontext.Messaging().VPN,
					clientUsername,
				)
				Expect(err).ToNot(HaveOccurred())
			})

			It("fails to connect using a provided username", func() {
				helpers.TestConnectDisconnectMessagingServiceClientValidation(builder.
					WithAuthenticationStrategy(config.KerberosAuthentication("")).
					FromConfigurationProvider(config.ServicePropertyMap{
						config.AuthenticationPropertySchemeKerberosUserName: clientUsername,
					}),
					func(client *monitor.MsgVpnClient) {
						// the API provided username should be ignored
						Expect(client.ClientUsername).ToNot(Equal(clientUsername))
					})
			})

			Context("with API provided username enabled", func() {
				BeforeEach(func() {
					_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(), sempconfig.MsgVpn{
						AuthenticationKerberosAllowApiProvidedUsernameEnabled: helpers.True,
					}, testcontext.Messaging().VPN, nil)
					Expect(err).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(), sempconfig.MsgVpn{
						AuthenticationKerberosAllowApiProvidedUsernameEnabled: helpers.False,
					}, testcontext.Messaging().VPN, nil)
					Expect(err).ToNot(HaveOccurred())
				})

				It("can connect with an API provided username", func() {
					helpers.TestConnectDisconnectMessagingServiceClientValidation(builder.
						WithAuthenticationStrategy(config.KerberosAuthentication("")).
						FromConfigurationProvider(config.ServicePropertyMap{
							config.AuthenticationPropertySchemeKerberosUserName: clientUsername,
						}),
						func(client *monitor.MsgVpnClient) {
							Expect(client.ClientUsername).To(Equal(clientUsername))
						},
					)
				})

				It("can connect with an API provided username when given a client certificate username", func() {
					helpers.TestConnectDisconnectMessagingServiceClientValidation(builder.
						WithAuthenticationStrategy(config.KerberosAuthentication("")).
						FromConfigurationProvider(config.ServicePropertyMap{
							config.AuthenticationPropertySchemeKerberosUserName:   clientUsername,
							config.AuthenticationPropertySchemeClientCertUserName: "someOtherUsername",
						}),
						func(client *monitor.MsgVpnClient) {
							Expect(client.ClientUsername).To(Equal(clientUsername))
						},
					)
				})

				It("can connect with an API provided username when given a client certificate username", func() {
					helpers.TestConnectDisconnectMessagingServiceClientValidation(builder.
						WithAuthenticationStrategy(config.KerberosAuthentication("")).
						FromConfigurationProvider(config.ServicePropertyMap{
							config.AuthenticationPropertySchemeKerberosUserName: clientUsername,
							config.AuthenticationPropertySchemeBasicUserName:    "someOtherUsername",
						}),
						func(client *monitor.MsgVpnClient) {
							Expect(client.ClientUsername).To(Equal(clientUsername))
						},
					)
				})
			})
		})

	})

	Context("with Kerberos disabled on the target VPN", func() {
		It("fails to conenct with kerberos authentication", func() {
			helpers.TestFailedConnectMessagingService(builder.
				WithAuthenticationStrategy(config.KerberosAuthentication("")), func(err error) {
				helpers.ValidateNativeError(err, subcode.KerberosAuthenticationIsShutdown)
			})
		})
	})
})
