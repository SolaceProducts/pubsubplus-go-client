// pubsubplus-go-client
//
// Copyright 2021-2022 Solace Corporation. All rights reserved.
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
	"io/ioutil"
        "time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/constants"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	sempconfig "solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/sempclient/monitor"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("OAuth Strategy", func() {
	const rootAuthorityName = "oauth_trusted_root"

	const tokenA = "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3NvbGFjZS5pZC50b2tlbi50ZXN0LmNvbSIsInN1YiI6InNvbGNsaWVudF9vYXV0aCIsImF1ZCI6InNvbGNsaWVudF9vYXV0aCIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoyMTQ3NDgzNjQ3LCJncm91cHMiOiJzb2xjbGllbnRfb2F1dGhfYXV0aF9ncm91cCJ9.UXTA4LGaq3flAmBMKq_vne07x1HyQMoDKWJgPHQYOVWkrRKTXArswDi_l8hOoQp-Smqqq0kJamVXjS4WlCK0ir3_ONhY8ZsERjYK5574-ojhxnpPAqTbx3nau0cYe0Ni19oFPPPFTjvBqgYRZKS7a-BB9o4UiOYVWzfqFWAfeFKHj4rmkv8GWgbP7yegylbUGGT38y9jNWG1BWL7JU36P2mzqOc46i7idKU0-QYd_gBrTsADh_O-mJlWLoLo-TtS8MIxduwsjeWIGFtadZJZjB2fDyCWDHwLTj-yQmak7HT3GvZdW7NPN1eHRKqZCax_j4ATRNGWMy08q4KSVeBwdpLeYZw1qyp6uZB_0-dYTNr8dA3Vcljzi7JQPFirvQdCSjUigad_LgPAIoHKaJIFWjxEf9cYWCQFO9dgg2YZvATMW301m4saMmDJpqhDIPoJgW3RBYa55ExCENQx82IiBgHl6fjbKWMGKS7s6pUdHsc_2QO_Wetp3uKkv2YegN6E3wFU-UZsITfVAvuAXSqE9QBqlX0b3gTq3RsUywh5jDosNY5g8zHCRzY2lpcFq9PRuZjAzBWvhufIMOyZK-uXF7aPY09boBzWj496q07hBwx6WF38XIGZkeWe1wZg_LrVUC_2gb_cULhrblUSdfMuMsuNhxS0XGCFd23Zr_cjaZI"
	const tokenB = "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3NvbGFjZS5pZC50b2tlbi50ZXN0LmNvbSIsInN1YiI6InNvbGNsaWVudF9vYXV0aCIsImF1ZCI6InNvbGNsaWVudF9vYXV0aCIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoyMTQ3NDgzNjQ3fQ.TMlpVZx1ogQiT1REhKrCiFY9k9IUcZIZwaVKjaLRXd2pfy-wEEOSNu_ePGKWfqs_AfRuTNBU4lcXtt_5P_oIepwOSdp-nxaFE-UANjjmnGsFq8CSceXYRYkTfwxzqQ1b70w6F3OMOYsEejlqgQ4wn7G1c4JbfKplKj5t29hv3M6oKTX6gaoZUnZDBy0yZidOTbw2WQsJ7EZ9akCY86BdWg1MoAwzhRuyN5TNf73yU-C_4aukTKetbPywQ2NLN9lynItObjWoneCtxJQQj1N0ngoLrgw_gefEs8rXgvXH3OMfQSSKh7bovLB_yjQWqqlpj0HXebsf3H0JtForJsNdMm_DeGdsghBT6OzA2cW2lzZP7B1RNPDBP08XLTVLGIyLqR1zcWXlN-r3dmMU4TXmiisjRnPudmLt4K-oqZ2sweW3phi3FUaLH23S02YmpXRXucBUh-PISg1S22a7_Xr3m9FbVYTjIuyP323do42w7DCMkOy967Mk00hktU-q6whwCDFuCXgaxsEZ7ffeLYjG7TejyJuB9bNpTuLQc5YWuA5IVLlUHcQCMNaC2DJFsMGl69ty97I526jr0S2hC_nSYCKgpiu5Qsle_69otpsx8NzKywqKgHTNdhu1UT8xdTKC-gSTs2wplIIkN5QAEXUe-Ik38_RzsPvRc3Vjt6xf4II"
	const tokenC = "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL3NvbGFjZS5hY2Nlc3MudG9rZW4udGVzdC5jb20iLCJzdWIiOiJzb2xjbGllbnRfb2F1dGgiLCJhdWQiOiJzb2xjbGllbnRfb2F1dGgiLCJpYXQiOjE1MTYyMzkwMjIsImV4cCI6MjE0NzQ4MzY0N30.RhRVo2KgON1TLUIwbzKvKYXLLapMzaHsHIKS7dI9uIQAqeII8cu0vgPp9r1IRNo8PVSDvrGfRS3Dh_rb_1bX0JDWM4i5sDCovj3748l8HyEVbpdCwH99xQO_TzZC1KkQ6GMQU5FFt7sT_YHIdF7yqC5XZ9HrW8cyZ-rVpvdLVIf_36cykzLYvqAErOgedfhqTKrqF0Q2U16MH_O2dHAL-Xz246wTppDqOB_7EeVZEJDASuICxQyl6LxfM8LLxI5oelvjYNDv1q4JNKLlgtu6VQeuLU04AMiIn0DSH8dF5IsjN9s0igAsfvyo5zRadlWkRbcJUXuTQ80Ju_1wHQMiikr5QaEreR7doOucOpKWCal6Iyhf0C330TPd2KFF3gfLiOw96Zgj0SpiILUVtoXigm07XB20i1HN5J2K8Buaw7VW_UqqdpnVKaVniVuucJw_dr-_zv5cxaqeT1LVSGFfpM5ksv9HaQdT2URo9xHt7WEFNyQSAdOoxW9gF2l0mhGweD0ZXISFyjXowKZTOPuJlDdfutOA0n5_xr-22_slFOkUi4NgnEselQmX7fMYTFDg-Hz1PM4UAozTm77PeN3VOOUOsNZuROjGpbn4CshyZY11zXIEtUGARLB9GsJKecpGsNjOVZdiQG_FeNsht09Z-2RJ_7vLw8EMUaSoclW24Oc"
	const tokenD = "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzb2xjbGllbnRfb2F1dGgiLCJhdWQiOiJzb2xjbGllbnRfb2F1dGgiLCJpYXQiOjE1MTYyMzkwMjIsImV4cCI6MjE0NzQ4MzY0N30.AUJ9-uwTiXzyui0uJ3y6nFOLq1mqTn-lKZ6qXRQD8M5RizPoVL7_PRFx_ZbYn7aDK1XX3_-Ow1lKJ4TI5A0-PVbbprZPpFs0L3xfn3icZecCajM7YXaz-PixCsVbxZt9xa_qM471-OQh1HV95kAo3Ban1d8tKpksirV6mqBDDu51MMyx8OtxrYxB10EC0fVarfdeNa0IFwCebbG2L1UO4fNi4o9wvQULQ9JAKm_fXS9F6JeAl4eZ0bO4etHCABnMjdpXM91FHF5xNwcze0RhGBlFW83o4dtNxclK7gvXuwEw1Kz6h83BEdUaagRjTYQ90KJBUeudbuMI7ABwMliwUiPAkK8kh3j2Xh3wWkMkg3rfd3zrFuCbmKFulpPoRQIl7Uh9YpejoZWB63cQPwx6bPgGjCBqEB_uwJsW66olCDUDWO0YpnX9094aU6Zc4D22w1__Vp0ovXQHUKuIVi0r4ZgbHzc5p8UMChD3TBOqtkiBahAmHajc2mvCM8Q0uAGKGxAb2bHo5W9q_A1U0Urn2qZbKGl1J4aYoIibQvX9yc__8CwGbNLD2_C2FEPHpjRP8gw19MMXnfb1PU5nbAzSfjYa0kZcJK3Qja9w0eQo61nfv_jb-vig0iEEdmHWEt3y8S5MqxU9rellj9SanJJiN_k83tI5-kO9eQ664En7MnE"
	const issuerIdentifier = "https://solace.access.token.test.com"

	var builder solace.MessagingServiceBuilder
	var messagingService solace.MessagingService
	var url string

	Describe("When OAuth Authentication is allowed on the Message VPN", func() {

		BeforeEach(func() {
			if testcontext.OAuth().Hostname == "" {
				Skip("No OAuth server found")
			}

			endpointJwks := fmt.Sprintf("https://%s:4400/", testcontext.OAuth().Hostname)
			endpointUserInfo := fmt.Sprintf("https://%s:4401/", testcontext.OAuth().Hostname)

			var err error
			certContent, err := ioutil.ReadFile(constants.ValidClientCertificatePEM)
			Expect(err).ToNot(HaveOccurred())
			_, _, err = testcontext.SEMP().Config().CertAuthorityApi.CreateCertAuthority(testcontext.SEMP().ConfigCtx(), sempconfig.CertAuthority{
				CertAuthorityName:           rootAuthorityName,
				CertContent:                 string(certContent),
				CrlDayList:                  "daily",
				CrlTimeList:                 "3:00",
				CrlUrl:                      "",
				OcspNonResponderCertEnabled: helpers.False,
				OcspOverrideUrl:             "",
				OcspTimeout:                 5,
				RevocationCheckEnabled:      helpers.False,
			}, nil)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpn{
					AuthenticationOauthEnabled: helpers.True,
				},
				testcontext.Messaging().VPN,
				nil,
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().AuthenticationOauthProfileApi.CreateMsgVpnAuthenticationOauthProfile(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpnAuthenticationOauthProfile{
					Enabled:                      helpers.True,
					OauthProfileName:             "SolaceOauthClient",
					AuthorizationGroupsClaimName: "groups",
					ClientId:                     "solclient_oauth",
					OauthRole:                    "client",
					EndpointJwks:                 endpointJwks,
					EndpointUserinfo:             endpointUserInfo,
					Issuer:                       "https://solace.id.token.test.com",
				},
				testcontext.Messaging().VPN,
				nil,
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().AuthenticationOauthProfileApi.CreateMsgVpnAuthenticationOauthProfile(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpnAuthenticationOauthProfile{
					Enabled:                               helpers.True,
					OauthProfileName:                      "SolaceOauthResourceServer",
					AuthorizationGroupsClaimName:          "",
					EndpointJwks:                          endpointJwks,
					EndpointUserinfo:                      endpointUserInfo,
					Issuer:                                "https://solace.access.token.test.com",
					ResourceServerParseAccessTokenEnabled: helpers.True,
					ResourceServerRequiredAudience:        "",
					ResourceServerRequiredIssuer:          "",
					ResourceServerRequiredScope:           "",
					ResourceServerValidateAudienceEnabled: helpers.False,
					ResourceServerValidateIssuerEnabled:   helpers.False,
					ResourceServerValidateScopeEnabled:    helpers.False,
					ResourceServerValidateTypeEnabled:     helpers.False,
					OauthRole:                             "resource-server",
				},
				testcontext.Messaging().VPN,
				nil,
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().AuthorizationGroupApi.CreateMsgVpnAuthorizationGroup(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpnAuthorizationGroup{
					Enabled:                helpers.True,
					AuthorizationGroupName: "solclient_oauth_auth_group",
				},
				testcontext.Messaging().VPN,
				nil,
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().ClientUsernameApi.CreateMsgVpnClientUsername(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpnClientUsername{
					Enabled:        helpers.True,
					ClientUsername: "solclient_oauth",
				},
				testcontext.Messaging().VPN,
				nil,
			)
			Expect(err).ToNot(HaveOccurred())

			url = fmt.Sprintf("tcps://%s:%d", testcontext.Messaging().Host, testcontext.Messaging().MessagingPorts.SecurePort)
			builder = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyVPNName:                               testcontext.Messaging().VPN,
				config.TransportLayerSecurityPropertyTrustStorePath:         constants.ValidFixturesPath,
				config.TransportLayerSecurityPropertyCertRejectExpired:      true,
				config.TransportLayerSecurityPropertyCertValidateServername: false,
				config.TransportLayerSecurityPropertyCertValidated:          true,
				config.TransportLayerPropertyHost:                           url,
				config.TransportLayerPropertyReconnectionAttempts:           0,
			})
		})

		AfterEach(func() {
			var err error

			_, _, err = testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpn{
					AuthenticationOauthEnabled: helpers.False,
				},
				testcontext.Messaging().VPN,
				nil,
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().AuthenticationOauthProfileApi.DeleteMsgVpnAuthenticationOauthProfile(
				testcontext.SEMP().ConfigCtx(),
				testcontext.Messaging().VPN,
				"SolaceOauthClient",
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().AuthenticationOauthProfileApi.DeleteMsgVpnAuthenticationOauthProfile(
				testcontext.SEMP().ConfigCtx(),
				testcontext.Messaging().VPN,
				"SolaceOauthResourceServer",
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().AuthorizationGroupApi.DeleteMsgVpnAuthorizationGroup(
				testcontext.SEMP().ConfigCtx(),
				testcontext.Messaging().VPN,
				"solclient_oauth_auth_group",
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().ClientUsernameApi.DeleteMsgVpnClientUsername(
				testcontext.SEMP().ConfigCtx(),
				testcontext.Messaging().VPN,
				"solclient_oauth",
			)
			Expect(err).ToNot(HaveOccurred())

			_, _, err = testcontext.SEMP().Config().CertAuthorityApi.DeleteCertAuthority(
				testcontext.SEMP().ConfigCtx(),
				rootAuthorityName,
			)
			Expect(err).ToNot(HaveOccurred())

			if messagingService.IsConnected() {
				helpers.DisconnectMessagingService(messagingService)
			}

		})

		DescribeTable("Messaging Service connects successfully",
			func(access, id, issuer string) {
				var err error
				messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
					access,
					id,
					issuer,
				)).Build()
				Expect(err).ToNot(HaveOccurred())
				Expect(messagingService.Connect()).ToNot(HaveOccurred())
				helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
					Expect(client.ClientUsername).To(Equal("solclient_oauth"))
				})
			},
			Entry("When given id token a, no access token and no issuer identifier", "", tokenA, ""),
			Entry("When given id token b, access token c and no issuer identifier", tokenC, tokenB, ""),
			Entry("When given id token b, access token c and no issuer identifier", tokenD, tokenB, ""),
			Entry("When given access token c, no id token and no issuer identifier", tokenC, "", ""),
			Entry("When given access token d, no id token and an issuer identifier", tokenD, "", issuerIdentifier),
		)

                Describe("When the messaging service tries to connect after multiple token updates", func() {
                        Context("When the multiple updates were applied before the first connection with valid tokens", func() {
                                It("should not fail when trying to connect the messaging service", func() {
                                        var err error
                                        // We first set the tokens and issuer identifier to empty strings to prove that the update
                                        // later on actually worked. If the update doesn't work, the connection attempt will fail
                                        // because the original tokens were invalid. If the connection attempt succeeds, it is only
                                        // because the service token properties were successfully updated.
                                        messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
                                                "invalid access token",
                                                "invalid id token",
                                                "",
                                        )).Build()
                                        Expect(err).ToNot(HaveOccurred())

                                        // First round of updates
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2OIDCIDToken, tokenB)
                                        Expect(err).ToNot(HaveOccurred())
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2AccessToken, tokenC)
                                        Expect(err).ToNot(HaveOccurred())

                                        // Second round of updates
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2OIDCIDToken, tokenB)
                                        Expect(err).ToNot(HaveOccurred())
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2AccessToken, tokenC)
                                        Expect(err).ToNot(HaveOccurred())

                                        helpers.ConnectMessagingService(messagingService)
                                        // Validation of connection state occurs within the ConnectMessagingService
                                        // and DisconnectMessagingService methods
                                        helpers.DisconnectMessagingService(messagingService)
                                })
                        })

                        Context("When the multiple updates were applied after the first connection with valid tokens", func() {
                                It("should not fail when trying to reconnect the messaging service", func() {
                                        var err error

                                        messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
                                                tokenC,
                                                tokenB,
                                                "",
                                        )).WithReconnectionRetryStrategy(config.RetryStrategyForeverRetry()).Build()
                                        Expect(err).ToNot(HaveOccurred())

                                        helpers.ConnectMessagingService(messagingService)

                                        // First round of updates
                                        // Updating the ID token to the same value is not redundant because we are verifying that the update
                                        // of that property can occur at all, regardless of the value being different.
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2OIDCIDToken, tokenB)
                                        Expect(err).ToNot(HaveOccurred())
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2AccessToken, tokenD)
                                        Expect(err).ToNot(HaveOccurred())

                                        // Second round of updates
                                        // Updating the ID token to the same value is not redundant because we are verifying that the update
                                        // of that property can occur at all, regardless of the value being different.
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2OIDCIDToken, tokenB)
                                        Expect(err).ToNot(HaveOccurred())
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2AccessToken, tokenD)
                                        Expect(err).ToNot(HaveOccurred())

                                        reconnectChan := make(chan struct{})
                                        messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
                                                close(reconnectChan)
                                        })

                                        helpers.ForceDisconnectViaSEMPv2(messagingService)
                                        Eventually(reconnectChan).Should(BeClosed())

                                        // Clean up messaging service
                                        helpers.DisconnectMessagingService(messagingService)
                                })
                        })
                })

                Describe("When the service tries to update the token in an invalid way", func() {
                        Context("When the token is updated with an invalid token value after successfully connecting", func() {
                                It("should fail to reconnect", func() {
                                        var err error

                                        messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
                                                tokenC,
                                                tokenB,
                                                "",
                                        )).WithReconnectionRetryStrategy(config.RetryStrategyParameterizedRetry(1, 200*time.Millisecond)).Build()
                                        Expect(err).ToNot(HaveOccurred())

                                        helpers.ConnectMessagingService(messagingService)

                                        // We are passing a non-empty string as the value for a valid token property, so we expect the update
                                        // to not return any errors. Instead the error is expected to be returned later when we try to
                                        // reconnect using the invalid token.
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2OIDCIDToken, "invalid token")
                                        Expect(err).ToNot(HaveOccurred())

                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2AccessToken, "invalid token")
                                        Expect(err).ToNot(HaveOccurred())

                                        reconnectChan := make(chan struct{})
                                        messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
                                                close(reconnectChan)
                                        })

                                        helpers.ForceDisconnectViaSEMPv2(messagingService)
                                        Consistently(reconnectChan).ShouldNot(Receive())
                                        Eventually(messagingService.IsConnected()).Should(BeFalse())

                                        // The service should fail to reconnect above, so if the service is connected at this point,
                                        // then there was an error that was not detected, so we will fail the test here, after
                                        // cleaning up the service.
                                        var messagingServiceIsConnected = messagingService.IsConnected()
                                        helpers.DisconnectMessagingService(messagingService)

                                        if messagingServiceIsConnected {
                                                Fail("Service was expected to be disconnected, but instead was connected.")
                                        }
                                })
                        })

                        Context("When the token is updated with an invalid token type after successfully connecting", func() {
                                It("should fail to reconnect", func() {
                                        var err error

                                        messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
                                                tokenC,
                                                tokenB,
                                                "",
                                        )).WithReconnectionRetryStrategy(config.RetryStrategyParameterizedRetry(1, 200*time.Millisecond)).Build()
                                        Expect(err).ToNot(HaveOccurred())

                                        helpers.ConnectMessagingService(messagingService)

                                        // We are passing a non-empty string as the value for a valid token property, so we expect the update
                                        // to not return any errors. Instead the error is expected to be returned later when we try to
                                        // reconnect using the invalid token.
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2OIDCIDToken, 75)
                                        Expect(err).ToNot(HaveOccurred())

                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2AccessToken, 75)
                                        Expect(err).ToNot(HaveOccurred())

                                        reconnectChan := make(chan struct{})
                                        messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
                                                close(reconnectChan)
                                        })

                                        helpers.ForceDisconnectViaSEMPv2(messagingService)
                                        Consistently(reconnectChan).ShouldNot(Receive())
                                        Eventually(messagingService.IsConnected()).Should(BeFalse())

                                        // The service should fail to reconnect above, so if the service is connected at this point,
                                        // then there was an error that was not detected, so we will fail the test here, after
                                        // cleaning up the service.
                                        var messagingServiceIsConnected = messagingService.IsConnected()
                                        helpers.DisconnectMessagingService(messagingService)

                                        if messagingServiceIsConnected {
                                                Fail("Service was expected to be disconnected, but instead was connected.")
                                        }
                                })
                        })

                        Context("When the token is updated with a nil token value after successfully connecting", func() {
                                It("should fail to reconnect", func() {
                                        var err error

                                        messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
                                                tokenC,
                                                tokenB,
                                                "",
                                        )).WithReconnectionRetryStrategy(config.RetryStrategyParameterizedRetry(1, 200*time.Millisecond)).Build()
                                        Expect(err).ToNot(HaveOccurred())

                                        helpers.ConnectMessagingService(messagingService)

                                        // We are passing a non-empty string as the value for a valid token property, so we expect the update
                                        // to not return any errors. Instead the error is expected to be returned later when we try to
                                        // reconnect using the invalid token.
                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2OIDCIDToken, nil)
                                        Expect(err).ToNot(HaveOccurred())

                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2AccessToken, nil)
                                        Expect(err).ToNot(HaveOccurred())

                                        reconnectChan := make(chan struct{})
                                        messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
                                                close(reconnectChan)
                                        })

                                        helpers.ForceDisconnectViaSEMPv2(messagingService)
                                        Consistently(reconnectChan).ShouldNot(Receive())
                                        Eventually(messagingService.IsConnected()).Should(BeFalse())

                                        // The service should fail to reconnect above, so if the service is connected at this point,
                                        // then there was an error that was not detected, so we will fail the test here, after
                                        // cleaning up the service.
                                        var messagingServiceIsConnected = messagingService.IsConnected()
                                        helpers.DisconnectMessagingService(messagingService)

                                        if messagingServiceIsConnected {
                                                Fail("Service was expected to be disconnected, but instead was connected.")
                                        }
                                })
                        })

                        Context("When the token is updated on a disconnected service", func() {
                                It("should return an IllegalStateError", func() {
                                        var err error

                                        messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
                                                tokenC,
                                                tokenB,
                                                "",
                                        )).Build()
                                        Expect(err).ToNot(HaveOccurred())

                                        // We need to connect and then disconnect the service to get the service into
                                        // the `disconnected` state since it is valid for the application to update the
                                        // tokens on a service that has not yet been connected, but not on one that is
                                        // already disconnected.
                                        helpers.ConnectMessagingService(messagingService)
                                        helpers.DisconnectMessagingService(messagingService)
                                        Expect(messagingService.IsConnected()).To(BeFalse())

                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2OIDCIDToken, tokenB)
                                        Expect(err).To(HaveOccurred())
                                        helpers.ValidateError(err, &solace.IllegalStateError{})

                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeOAuth2AccessToken, tokenC)
                                        Expect(err).To(HaveOccurred())
                                        helpers.ValidateError(err, &solace.IllegalStateError{})
                                })
                        })

                        Context("When an invalid property is used to update the token", func() {
                                It("should return an IllegalArgumentError", func() {
                                        var err error

                                        messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
                                                tokenC,
                                                tokenB,
                                                "",
                                        )).Build()
                                        Expect(err).ToNot(HaveOccurred())

                                        err = messagingService.UpdateProperty(config.AuthenticationPropertySchemeBasicPassword, "arbitrary string")
                                        Expect(err).To(HaveOccurred())
                                        helpers.ValidateError(err, &solace.IllegalArgumentError{})
                                })
                        })
                })

		DescribeTable("Messaging Service fails to connect",
			func(access, id, issuer string) {
				var err error
				messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
					access,
					id,
					issuer,
				)).Build()
				Expect(err).ToNot(HaveOccurred())
				Expect(messagingService.Connect()).To(HaveOccurred())
			},
			Entry("When given an invalid id token, no access token and no issuer identifier", "", "invalid id token", ""),
			Entry("When given no id token, an invalid access token and no issuer identifier", "invalid access token", "", ""),
			Entry("when given access token D but no issuer identifier", tokenD, "", ""),
		)

		Describe("When the usernames of other authentication schemes are set", func() {
			Context("When the username for basic authentication is specified", func() {
				It("should not fail when trying to connect the messaging service", func() {
					var err error
					builder = builder.FromConfigurationProvider(config.ServicePropertyMap{
						config.AuthenticationPropertySchemeBasicUserName: "Basic Username",
					})
					messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
						tokenC,
						tokenB,
						"",
					)).Build()
					Expect(err).ToNot(HaveOccurred())
					Expect(messagingService.Connect()).ToNot(HaveOccurred())
					helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
						Expect(client.ClientUsername).To(Equal("solclient_oauth"))
					})
				})
			})

			Context("When the username for kerberos authentication is specified", func() {
				It("should not fail when trying to connect the messaging service", func() {
					var err error
					builder = builder.FromConfigurationProvider(config.ServicePropertyMap{
						config.AuthenticationPropertySchemeKerberosUserName: "Kerberos Username",
					})
					messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
						tokenC,
						tokenB,
						"",
					)).Build()
					Expect(err).ToNot(HaveOccurred())
					Expect(messagingService.Connect()).ToNot(HaveOccurred())
					helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
						Expect(client.ClientUsername).To(Equal("solclient_oauth"))
					})
				})
			})

			Context("When the username for client certificate authentication is specified", func() {
				It("should not fail when trying to connect the messaging service", func() {
					var err error
					builder = builder.FromConfigurationProvider(config.ServicePropertyMap{
						config.AuthenticationPropertySchemeClientCertUserName: "Client Certificate Username",
					})
					messagingService, err = builder.WithAuthenticationStrategy(config.OAuth2Authentication(
						tokenC,
						tokenB,
						"",
					)).Build()
					Expect(err).ToNot(HaveOccurred())
					Expect(messagingService.Connect()).ToNot(HaveOccurred())
					helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
						Expect(client.ClientUsername).To(Equal("solclient_oauth"))
					})
				})
			})
		})
	})

	Describe("When OAuth Authentication is not allowed on the Message VPN", func() {
		BeforeEach(func() {
			_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpn{
					AuthenticationOauthEnabled: helpers.False,
				},
				testcontext.Messaging().VPN,
				nil,
			)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should produce an error when trying to build a messaging service with OAuth Authentification Strategy", func() {
			var err error
			url = fmt.Sprintf("tcps://%s:%d", testcontext.Messaging().Host, testcontext.Messaging().MessagingPorts.SecurePort)
			messagingService, err = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyVPNName:                               testcontext.Messaging().VPN,
				config.TransportLayerSecurityPropertyTrustStorePath:         constants.ValidFixturesPath,
				config.TransportLayerSecurityPropertyCertRejectExpired:      true,
				config.TransportLayerSecurityPropertyCertValidateServername: false,
				config.TransportLayerSecurityPropertyCertValidated:          true,
				config.TransportLayerPropertyHost:                           url,
				config.TransportLayerPropertyReconnectionAttempts:           0,
			}).WithAuthenticationStrategy(config.OAuth2Authentication(
				tokenC,
				tokenB,
				"",
			)).Build()
			Expect(err).ToNot(HaveOccurred())
			helpers.ValidateNativeError(messagingService.Connect(), subcode.LoginFailure)
		})
	})
})
