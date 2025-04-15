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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/subcode"

	"solace.dev/go/messaging/test/constants"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/sempclient/action"
	sempconfig "solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/sempclient/monitor"
	"solace.dev/go/messaging/test/testcontext"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// MessagingService Lifecycle tests test the various connect and disconnect paths validating that everything works as expected
var _ = Describe("MessagingService Lifecycle", func() {
	// Before each test, setup a builder with basic connection properties over plaintext
	var builder solace.MessagingServiceBuilder
	BeforeEach(func() {
		builder = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration())
	})

	// Spec out tests for all connect and disconnect functions exposed by the MessagingService
	for connectName, connectFunction := range helpers.ConnectFunctions {
		for disconnectName, disconnectFunction := range helpers.DisconnectFunctions {
			var connect = connectFunction
			var disconnect = disconnectFunction
			Context("with "+connectName+" and "+disconnectName, func() {
				// Test success path
				It("can connect and disconnect successfully", func() {
					helpers.TestConnectDisconnectMessagingServiceWithFunctions(builder, connect, disconnect)
				})
				// Test error handling in all connect/disconnect functions
				Context("with authentication set to internal", func() {
					BeforeEach(func() {
						_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(),
							sempconfig.MsgVpn{AuthenticationBasicType: "internal"}, testcontext.Messaging().VPN, nil)
						Expect(err).NotTo(HaveOccurred())
					})
					AfterEach(func() {
						_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(),
							sempconfig.MsgVpn{AuthenticationBasicType: "none"}, testcontext.Messaging().VPN, nil)
						Expect(err).NotTo(HaveOccurred())
					})
					// Validate that we get an error returned from all function variations
					It("should fail to connect with a bad username and password", func() {
						builder.WithAuthenticationStrategy(config.BasicUserNamePasswordAuthentication("notausername", "notapassword"))

						helpers.TestFailedConnectMessagingService(builder, func(err error) {
							Expect(err).To(BeAssignableToTypeOf(&solace.AuthenticationError{}))
						})
					})
				})
			})
		}
	}

	// Basic messaging service tests
	It("can connect idempotently", func() {
		messagingService := helpers.BuildMessagingService(builder)
		c1 := messagingService.ConnectAsync()
		c2 := messagingService.ConnectAsync()
		var e1, e2 error
		// Give lots of time to connect
		Eventually(c1, 10*time.Second).Should(Receive(&e1))
		// c2 should be signaled immediately after
		Eventually(c2, 10*time.Millisecond).Should(Receive(&e2))
		Expect(e1).ToNot(HaveOccurred())
		Expect(e2).ToNot(HaveOccurred())
		Expect(messagingService.IsConnected()).To(BeTrue())
		d1 := messagingService.DisconnectAsync()
		d2 := messagingService.DisconnectAsync()
		Eventually(d1).Should(Receive(&e1))
		Eventually(d2).Should(Receive(&e2))
		Expect(e1).ToNot(HaveOccurred())
		Expect(e2).ToNot(HaveOccurred())
	})
	It("should fail to connect a disconnected service", func() {
		messagingService := helpers.BuildMessagingService(builder)
		helpers.ConnectMessagingService(messagingService)
		helpers.DisconnectMessagingService(messagingService)
		err := messagingService.Connect()
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
	})
	It("should fail to disconnect an unconnected service", func() {
		messagingService := helpers.BuildMessagingService(builder)
		err := messagingService.Disconnect()
		Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
	})
	It("should be able to disconnect an already disconnected service", func() {
		messagingService := helpers.BuildMessagingService(builder)
		helpers.ConnectMessagingService(messagingService)
		helpers.DisconnectMessagingService(messagingService)
		result := messagingService.DisconnectAsync()
		// should disconnect almost immediately, returning nil
		Eventually(result).Should(Receive(BeNil()))
	})
	It("can connect multiple messaging services from the same builder", func() {
		messagingServiceOne := helpers.BuildMessagingService(builder)
		messagingServiceTwo := helpers.BuildMessagingService(builder)
		helpers.ConnectMessagingService(messagingServiceOne)
		helpers.ConnectMessagingService(messagingServiceTwo)
		Expect(messagingServiceOne.IsConnected()).To(BeTrue())
		Expect(messagingServiceTwo.IsConnected()).To(BeTrue())
		Expect(messagingServiceOne.GetApplicationID()).ToNot(Equal(messagingServiceTwo.GetApplicationID()))
		helpers.DisconnectMessagingService(messagingServiceTwo)
		Expect(messagingServiceOne.IsConnected()).To(BeTrue())
		helpers.DisconnectMessagingService(messagingServiceOne)
	})
	It("should be able to jsonify a messaging service config", func() {
		cfgMap := config.ServicePropertyMap{
			config.TransportLayerPropertyHost:              "myHost",
			config.TransportLayerPropertyConnectionRetries: 1,
		}
		expectedJSON := `{"solace":{"messaging":{"transport":{"connection-retries":1,"host":"myHost"}}}}`
		result, err := json.Marshal(cfgMap)
		Expect(err).ToNot(HaveOccurred())
		Expect(string(result)).To(Equal(expectedJSON))
	})
	It("should be able to load a messaging service config from json", func() {
		cfg := `{"solace":{"messaging":{"transport":{"host":"myHost","connection-retries":1}}}}`
		expectedCfgMap := config.ServicePropertyMap{
			config.TransportLayerPropertyHost:              "myHost",
			config.TransportLayerPropertyConnectionRetries: 1,
		}
		actualCfgMap := config.ServicePropertyMap{}
		err := json.Unmarshal([]byte(cfg), &actualCfgMap)
		Expect(err).ToNot(HaveOccurred())
		Expect(len(actualCfgMap)).To(Equal(len(expectedCfgMap)))
		for key, value := range expectedCfgMap {
			actual, ok := actualCfgMap[key]
			Expect(ok).To(BeTrue())
			Expect(actual).To(BeEquivalentTo(value))
		}
	})
	It("can connect and disconnect over websocket plaintext", func() {
		connectionDetails := testcontext.Messaging()
		url := fmt.Sprintf("ws://%s:%d", connectionDetails.Host, connectionDetails.MessagingPorts.WebPort)
		config := config.ServicePropertyMap{
			config.TransportLayerPropertyHost: url,
		}
		builder.FromConfigurationProvider(config)
		helpers.TestConnectDisconnectMessagingService(builder)
	})
	It("should be able to configure the application ID via a build function", func() {
		applicationID := "myApplicationID"
		messagingService, err := builder.BuildWithApplicationID(applicationID)
		Expect(err).To(BeNil())

		Expect(messagingService.GetApplicationID()).To(Equal(applicationID))
		helpers.ConnectMessagingService(messagingService)
		// make sure the application ID doesn't change on a call to connect
		Expect(messagingService.GetApplicationID()).To(Equal(applicationID))
		helpers.DisconnectMessagingService(messagingService)
	})
	It("should be able to configure the application ID via builder properties", func() {
		applicationID := "myApplicationID"
		builder.FromConfigurationProvider(config.ServicePropertyMap{
			config.ClientPropertyName: applicationID,
		})
		messagingService := helpers.BuildMessagingService(builder)

		Expect(messagingService.GetApplicationID()).To(Equal(applicationID))
		helpers.ConnectMessagingService(messagingService)
		// make sure the application ID doesn't change on a call to connect
		Expect(messagingService.GetApplicationID()).To(Equal(applicationID))
		helpers.DisconnectMessagingService(messagingService)
	})
	It("should fail to connect to an unreachable host with bad port", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{
			config.TransportLayerPropertyHost: "tcp://localhost:35432",
		})
		helpers.TestFailedConnectMessagingService(builder, func(err error) {
			helpers.ValidateNativeError(err, subcode.CommunicationError)
		})
	})
	It("should be able to connect with connection retry strategy", func() {
		builder.WithConnectionRetryStrategy(config.RetryStrategyForeverRetry())
		helpers.TestConnectDisconnectMessagingService(builder)
	})
	It("should be able to connect with connection retry strategy from properties", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{config.TransportLayerPropertyConnectionAttemptsTimeout: 1000})
		helpers.TestConnectDisconnectMessagingService(builder)
	})
	It("should be able to connect with connection retry strategy from properties with duration", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{config.TransportLayerPropertyConnectionAttemptsTimeout: 10 * time.Second})
		helpers.TestConnectDisconnectMessagingService(builder)
	})

	It("should be able to connect with reconnection retry strategy", func() {
		builder.WithConnectionRetryStrategy(config.RetryStrategyForeverRetry())
		helpers.TestConnectDisconnectMessagingService(builder)
	})
	It("should be able to connect with reconnection retry strategy from properties", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{config.TransportLayerPropertyReconnectionAttemptsWaitInterval: 1000})
		helpers.TestConnectDisconnectMessagingService(builder)
	})
	It("should be able to connect with reconnection retry strategy from properties with duration", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{config.TransportLayerPropertyReconnectionAttemptsWaitInterval: 10 * time.Second})
		helpers.TestConnectDisconnectMessagingService(builder)
	})

	It("should be able to connect with provision timeout from properties", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{config.ServicePropertyProvisionTimeoutMs: 5000})
		helpers.TestConnectDisconnectMessagingService(builder)
	})
	It("should be able to connect with provision timeout from properties with duration", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{config.ServicePropertyProvisionTimeoutMs: 5 * time.Second})
		helpers.TestConnectDisconnectMessagingService(builder)
	})

	It("should be disconnected when force disconnected by the broker", func() {
		messagingService := helpers.BuildMessagingService(builder.WithReconnectionRetryStrategy(config.RetryStrategyNeverRetry()))
		defer func() {
			if messagingService.IsConnected() {
				messagingService.Disconnect()
			}
		}()
		helpers.ConnectMessagingService(messagingService)
		_, _, err := testcontext.SEMP().Action().ClientApi.
			DoMsgVpnClientDisconnect(
				testcontext.SEMP().ActionCtx(),
				action.MsgVpnClientDisconnect{},
				testcontext.Messaging().VPN,
				url.QueryEscape(messagingService.GetApplicationID()),
			)

		Expect(err).ToNot(HaveOccurred())
		Eventually(messagingService.IsConnected).Should(BeFalse())
	})

	It("should connect with proper version info", func() {
		helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
			// Validate that we added some version info
			Expect(client.SoftwareVersion).To(ContainSubstring("pubsubplus-go-client"))
			Expect(client.SoftwareDate).To(ContainSubstring("pubsubplus-go-client"))
		})
	})

	It("should connect using the proper username", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{
			config.AuthenticationPropertySchemeClientCertUserName: "notthedefaultusername",
		})
		helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
			Expect(client.ClientUsername).To(Equal(testcontext.Messaging().Authentication.BasicUsername))
		})
	})

	Context("when using compression", func() {
		BeforeEach(func() {
			connectionDetails := testcontext.Messaging()
			url := fmt.Sprintf("%s:%d", connectionDetails.Host, connectionDetails.MessagingPorts.CompressedPort)
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.TransportLayerPropertyHost:                 url,
				config.TransportLayerPropertyReconnectionAttempts: 0,
			})
		})

		validCompressionLevels := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		for _, validCompressionLevel := range validCompressionLevels {
			var compressionLevel = validCompressionLevel
			It("should be able to connect using compression level "+fmt.Sprint(compressionLevel), func() {
				builder.WithMessageCompression(compressionLevel)
				helpers.TestConnectDisconnectMessagingService(builder)
			})
		}
		invalidCompressionLevels := []int{-1, 10}
		for _, invalidCompressionLevel := range invalidCompressionLevels {
			var compressionLevel = invalidCompressionLevel
			It("should not able to build using compression level "+fmt.Sprint(compressionLevel), func() {
				builder.WithMessageCompression(compressionLevel)
				_, err := builder.Build()
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
			})
		}

		// When connecting to the compressed port, we must use compression
		It("should not be able to connect using compression level 0", func() {
			builder.WithMessageCompression(0)
			helpers.TestFailedConnectMessagingService(builder, func(err error) {
				helpers.ValidateNativeError(err, subcode.CommunicationError)
			})
		})
	}) // End compression tests

	// Test to validate range of payload compression level is from 0 to 9 (inclusive)
	Context("when using payload compression", func() {
		BeforeEach(func() {
			builder.FromConfigurationProvider(helpers.DefaultConfiguration())
		})

		validPayloadCompressionLevels := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		for _, validPayloadCompressionLevel := range validPayloadCompressionLevels {
			var compressionLevel = validPayloadCompressionLevel
			It("should be able to connect using compression level "+fmt.Sprint(compressionLevel), func() {
				builder.FromConfigurationProvider(config.ServicePropertyMap{
					config.ServicePropertyPayloadCompressionLevel: compressionLevel, // valid payload compression level
				})
				helpers.TestConnectDisconnectMessagingService(builder)
			})
		}
		invalidPayloadCompressionLevels := []int{-1, 10}
		for _, invalidPayloadCompressionLevel := range invalidPayloadCompressionLevels {
			var compressionLevel = invalidPayloadCompressionLevel
			It("should not able to build using payload compression level "+fmt.Sprint(compressionLevel), func() {
				builder.FromConfigurationProvider(config.ServicePropertyMap{
					config.ServicePropertyPayloadCompressionLevel: compressionLevel, // invalid payload compression level
				})
				_, err := builder.Build()
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
			})
		}
	}) // End payload compression tests

	schemeTcps := "tcps"
	schemeWss := "wss"

	secureSchemes := map[string](func(config *testcontext.MessagingConfig) int){
		schemeTcps: func(config *testcontext.MessagingConfig) int { return config.MessagingPorts.SecurePort },
		schemeWss:  func(config *testcontext.MessagingConfig) int { return config.MessagingPorts.SecureWebPort },
	}
	for iterScheme, iterPortFn := range secureSchemes {
		scheme := iterScheme
		portFn := iterPortFn
		// Use TLS connection details
		Context("when using TLS over "+scheme, func() {
			BeforeEach(func() {
				connectionDetails := testcontext.Messaging()
				url := fmt.Sprintf("%s://%s:%d", scheme, connectionDetails.Host, portFn(connectionDetails))
				builder.FromConfigurationProvider(config.ServicePropertyMap{
					config.TransportLayerPropertyHost:                 url,
					config.TransportLayerPropertyReconnectionAttempts: 0,
				})
			})
			It("should fail to connect to secured host without configured trust store", func() {
				_, err := builder.Build()
				helpers.ValidateNativeError(err, subcode.FailedLoadingTruststore)
			})
			It("should be able to configure TLS via the transport security strategies", func() {
				builder.WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().WithCertificateValidation(true, false, constants.ValidFixturesPath, ""))
				helpers.TestConnectDisconnectMessagingService(builder)
			})
			It("should reject untrusted certificate", func() {
				builder.WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().WithCertificateValidation(true, true, constants.InvalidFixturesPath, ""))
				helpers.TestFailedConnectMessagingService(builder, func(err error) {
					helpers.ValidateNativeError(err, subcode.UntrustedCertificate)
				})
			})
			It("should be able to connect with certificate validation off", func() {
				builder.WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().WithoutCertificateValidation())
				helpers.TestConnectDisconnectMessagingService(builder)
			})

			// Skip these tests; they are currently failing in Git actions see SOL-117804
			Context("with a bad server certificate installed on the broker", Label("flaky-tests"), func() {
				var invalidServerCertificate string
				JustBeforeEach(func() {
					Skip("Currently failing in Git actions - SOL-117804")

					certContent, err := ioutil.ReadFile(invalidServerCertificate)
					Expect(err).ToNot(HaveOccurred())
					// Git actions seems to have some trouble with this particular SEMP request and occasionally gets EOF errors
					for i := 0; i < 5; i++ {
						_, _, err = testcontext.SEMP().Config().AllApi.UpdateBroker(testcontext.SEMP().ConfigCtx(), sempconfig.Broker{
							TlsServerCertContent:  string(certContent),
							TlsServerCertPassword: constants.ServerCertificatePassphrase,
						}, nil)
						if err == nil {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					Expect(err).ToNot(HaveOccurred())
					if err != nil {
						// only wait on successful configuration change
						err = testcontext.WaitForSEMPReachable()
						Expect(err).ToNot(HaveOccurred())
					}
				})
				AfterEach(func() {
					Skip("Currently failing in Git actions - SOL-117804")

					certContent, err := ioutil.ReadFile(constants.ValidServerCertificate)
					Expect(err).ToNot(HaveOccurred())
					// Git actions seems to have some trouble with this particular SEMP request and occasionally gets EOF errors
					for i := 0; i < 5; i++ {
						_, _, err = testcontext.SEMP().Config().AllApi.UpdateBroker(testcontext.SEMP().ConfigCtx(), sempconfig.Broker{
							TlsServerCertContent:  string(certContent),
							TlsServerCertPassword: constants.ServerCertificatePassphrase,
						}, nil)
						if err == nil {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					Expect(err).ToNot(HaveOccurred())
					if err != nil {
						// only wait on successful configuration change
						err = testcontext.WaitForSEMPReachable()
						Expect(err).ToNot(HaveOccurred())
					}
				})

				When("using a server certificate with bad SAN", func() {
					BeforeEach(func() {
						invalidServerCertificate = constants.BadServernameServerCertificate
					})
					It("fails to connect when server name validaiton is enabled", func() {
						builder.WithTransportSecurityStrategy(config.
							NewTransportSecurityStrategy().
							WithCertificateValidation(false, true, constants.ValidFixturesPath, ""))
						helpers.TestFailedConnectMessagingService(builder, func(err error) {
							helpers.ValidateNativeError(err, subcode.UntrustedCertificate)
						})
					})
					It("can connect when server name validaiton is disabled", func() {
						builder.WithTransportSecurityStrategy(config.
							NewTransportSecurityStrategy().
							WithCertificateValidation(false, false, constants.ValidFixturesPath, ""))
						helpers.TestConnectDisconnectMessagingService(builder)
					})
				})

				When("using an expired server certificate", func() {
					BeforeEach(func() {
						invalidServerCertificate = constants.ExpiredServerCertificate
					})
					It("fails to connect when expired certificate checking is enabled", func() {
						builder.WithTransportSecurityStrategy(config.
							NewTransportSecurityStrategy().
							WithCertificateValidation(false, true, constants.ValidFixturesPath, ""))
						helpers.TestFailedConnectMessagingService(builder, func(err error) {
							helpers.ValidateNativeError(err, subcode.CertificateDateInvalid)
						})
					})
					It("can connect when expired certificate checking is disabled", func() {
						builder.WithTransportSecurityStrategy(config.
							NewTransportSecurityStrategy().
							WithCertificateValidation(true, true, constants.ValidFixturesPath, ""))
						helpers.TestConnectDisconnectMessagingService(builder)
					})
				})
			})

			// Use valid TLS configuration validating certificate, but NOT validating the server name as
			// we may be running against a remote broker
			Context("when using valid TLS validation configuration", func() {
				BeforeEach(func() {
					builder.FromConfigurationProvider(config.ServicePropertyMap{
						config.TransportLayerSecurityPropertyTrustStorePath:         constants.ValidFixturesPath,
						config.TransportLayerSecurityPropertyCertRejectExpired:      true,
						config.TransportLayerSecurityPropertyCertValidateServername: true,
						config.TransportLayerSecurityPropertyCertValidated:          true,
					})
				})
				It("should be able to connect to the broker over TLS", func() {
					helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
						Expect(client.TlsVersion).ToNot(Equal("N/A"))
					})
				})
				It("should be able to connect to multiple hosts", func() {
					connectionDetails := testcontext.Messaging()
					url := fmt.Sprintf("tcps://%s:%d", connectionDetails.Host, connectionDetails.MessagingPorts.SecurePort)
					builder.FromConfigurationProvider(config.ServicePropertyMap{
						config.TransportLayerPropertyHost: "tc://localhost1:55443,tcps://localhost2:55443," + url,
					})
					helpers.TestConnectDisconnectMessagingService(builder)
				})
				It("should be able to connect with cipher suite", func() {
					builder.WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().
						WithCipherSuites("AES128-SHA"))
					helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
						Expect(client.TlsCipherDescription).To(HavePrefix("AES128-SHA"))
					})
				})
				// Originially this explicitly test tls1.1
				// on systems with new openssl (3.0 or later) tls1.1 is no longer supported from the client
				// As a result this is adapted to explicitly verify tls1.2 in anticipation for tls1.3
				// once openssl 1.1 support is deprecated this maybe
				// We need to explicitly enable TLS1.2 to test a few cases
				Context("when allowing TLS1.2 connections", func() {
					BeforeEach(func() {
						// semp configuration for tls version support
						// revist for enabling support for tls 1.2 in the future
						//testcontext.SEMP().Config().AllApi.UpdateBroker(testcontext.SEMP().ConfigCtx(), sempconfig.Broker{
						//	TlsBlockVersion11Enabled: helpers.False,
						//}, nil)

					})
					AfterEach(func() {
						// semp configuration for tls version support
						// revist for disabling support for tls 1.2 in the future
						//testcontext.SEMP().Config().AllApi.UpdateBroker(testcontext.SEMP().ConfigCtx(), sempconfig.Broker{
						//	TlsBlockVersion11Enabled: helpers.True,
						//}, nil)
					})
					It("should be able to connect with excluded protocols", func() {
						builder.WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().
							WithExcludedProtocols(config.TransportSecurityProtocolSSLv3, config.TransportSecurityProtocolTLSv1, config.TransportSecurityProtocolTLSv1_1))
						helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
							Expect(client.TlsVersion).To(BeEquivalentTo(config.TransportSecurityProtocolTLSv1_2))
						})
					})
				})

				Context("when using client certificate authentication", func() {
					const certificateAuthorityName = "test-ca-1"
					// TODO consider moving some of this configuration to the startup
					BeforeEach(func() {
						_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(),
							sempconfig.MsgVpn{AuthenticationClientCertEnabled: helpers.True}, testcontext.Messaging().VPN, nil)
						Expect(err).ToNot(HaveOccurred())
						certContent, err := ioutil.ReadFile(constants.ValidClientCertificatePEM)
						Expect(err).ToNot(HaveOccurred())
						// sometimes this SEMP command fails on Git Actions. Retry
						for i := 0; i < 5; i++ {
							_, _, err = testcontext.SEMP().Config().ClientCertAuthorityApi.CreateClientCertAuthority(testcontext.SEMP().ConfigCtx(), sempconfig.ClientCertAuthority{
								CertAuthorityName:           certificateAuthorityName,
								CertContent:                 string(certContent),
								CrlDayList:                  "daily",
								CrlTimeList:                 "3:00",
								CrlUrl:                      "",
								OcspNonResponderCertEnabled: helpers.False,
								OcspOverrideUrl:             "",
								OcspTimeout:                 5,
								RevocationCheckEnabled:      helpers.False,
							}, nil)
							// retry until we are successful, or if we run out of retries
							if err == nil {
								break
							}
						}
						Expect(err).ToNot(HaveOccurred())
					})
					AfterEach(func() {
						_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(),
							sempconfig.MsgVpn{AuthenticationClientCertEnabled: helpers.False}, testcontext.Messaging().VPN, nil)
						Expect(err).ToNot(HaveOccurred())
						_, _, err = testcontext.SEMP().Config().ClientCertAuthorityApi.DeleteClientCertAuthority(testcontext.SEMP().ConfigCtx(), certificateAuthorityName)
						Expect(err).ToNot(HaveOccurred())
					})
					It("should be able to connect to the broker configured via properties", func() {
						builder.FromConfigurationProvider(config.ServicePropertyMap{
							config.AuthenticationPropertyScheme:                                 config.AuthenticationSchemeClientCertificate,
							config.AuthenticationPropertySchemeClientCertPrivateKeyFilePassword: constants.ValidCertificateKeyPassword,
							config.AuthenticationPropertySchemeSSLClientPrivateKeyFile:          constants.ValidClientKeyFile,
							config.AuthenticationPropertySchemeSSLClientCertFile:                constants.ValidClientCertificateFile,
						})
						helpers.TestConnectDisconnectMessagingService(builder)
					})
					It("should be able to connect to the broker using client certificate authentication via authentication strategies", func() {
						builder.WithAuthenticationStrategy(config.ClientCertificateAuthentication(
							constants.ValidClientCertificateFile,
							constants.ValidClientKeyFile,
							constants.ValidCertificateKeyPassword,
						))
						helpers.TestConnectDisconnectMessagingService(builder)
					})
					It("should fail to validate server name with invalid client certificate key file", func() {
						builder.WithAuthenticationStrategy(config.ClientCertificateAuthentication(
							constants.ValidClientCertificateFile,
							constants.InvalidClientKeyFile,
							constants.ValidCertificateKeyPassword,
						))
						helpers.TestFailedConnectMessagingService(builder, func(err error) {
							helpers.ValidateNativeError(err, subcode.FailedLoadingCertificateAndKey)
						})
					})
					It("should fail to connect with expired certificate", func() {
						builder.WithAuthenticationStrategy(config.ClientCertificateAuthentication(
							constants.ExpiredClientCertificateFile,
							constants.ValidClientKeyFile,
							constants.ValidCertificateKeyPassword,
						))
						helpers.TestFailedConnectMessagingService(builder, func(err error) {
							helpers.ValidateNativeError(err, subcode.ClientCertificateDateInvalid)
						})
					})
					It("should fail to build with invalid certificate", func() {
						builder.WithAuthenticationStrategy(config.ClientCertificateAuthentication(
							constants.InvalidClientCertificateFile,
							constants.ValidClientKeyFile,
							constants.ValidCertificateKeyPassword,
						))
						helpers.TestFailedConnectMessagingService(builder, func(err error) {
							helpers.ValidateNativeError(err, subcode.FailedLoadingCertificateAndKey)
						})
					})
					It("should fail to connect with wrong key password", func() {
						builder.WithAuthenticationStrategy(config.ClientCertificateAuthentication(
							constants.ValidClientCertificateFile,
							constants.ValidClientKeyFile,
							constants.InvalidCertificateKeyPassword,
						))
						helpers.TestFailedConnectMessagingService(builder, func(err error) {
							helpers.ValidateNativeError(err, subcode.FailedLoadingCertificateAndKey)
						})
					})

					Context("with a different client username", func() {
						const clientUsername = "client-cert-username-test"
						BeforeEach(func() {
							_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(),
								sempconfig.MsgVpn{AuthenticationClientCertAllowApiProvidedUsernameEnabled: helpers.True}, testcontext.Messaging().VPN, nil)
							Expect(err).ToNot(HaveOccurred())
							_, _, err = testcontext.SEMP().Config().ClientUsernameApi.CreateMsgVpnClientUsername(testcontext.SEMP().ConfigCtx(),
								sempconfig.MsgVpnClientUsername{
									ClientProfileName: "default",
									AclProfileName:    "default",
									ClientUsername:    clientUsername,
									Enabled:           helpers.True,
									MsgVpnName:        testcontext.Messaging().VPN,
									Password:          "somerandompassword",
								}, testcontext.Messaging().VPN, nil)
							Expect(err).ToNot(HaveOccurred())
						})

						AfterEach(func() {
							_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(),
								sempconfig.MsgVpn{AuthenticationClientCertAllowApiProvidedUsernameEnabled: helpers.False}, testcontext.Messaging().VPN, nil)
							Expect(err).ToNot(HaveOccurred())
							_, _, err = testcontext.SEMP().Config().ClientUsernameApi.DeleteMsgVpnClientUsername(testcontext.SEMP().ConfigCtx(),
								testcontext.Messaging().VPN, clientUsername)
							Expect(err).ToNot(HaveOccurred())
						})

						It("should be able to connect with overridden username", func() {
							builder.WithAuthenticationStrategy(config.ClientCertificateAuthentication(
								constants.ValidClientCertificateFile,
								constants.ValidClientKeyFile,
								constants.ValidCertificateKeyPassword,
							)).FromConfigurationProvider(config.ServicePropertyMap{
								config.AuthenticationPropertySchemeClientCertUserName: clientUsername,
							})
							helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
								Expect(client.ClientUsername).To(Equal(clientUsername))
							})
						})

						It("should be able to connect with overridden username when basic username is also provided", func() {
							builder.WithAuthenticationStrategy(config.ClientCertificateAuthentication(
								constants.ValidClientCertificateFile,
								constants.ValidClientKeyFile,
								constants.ValidCertificateKeyPassword,
							)).FromConfigurationProvider(config.ServicePropertyMap{
								config.AuthenticationPropertySchemeClientCertUserName: clientUsername,
							}).FromConfigurationProvider(config.ServicePropertyMap{
								config.AuthenticationPropertySchemeBasicUserName: "default",
							})
							helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
								Expect(client.ClientUsername).To(Equal(clientUsername))
							})
						})
					})
				}) // Client Cert Auth end

				// Downgrade to plaintext and compression are only supported over TCPS
				if scheme == schemeTcps {
					// We need to explicitly enable downgrade to plaintext to test downgrade cases
					Context("when using downgrade to plaintext", func() {
						BeforeEach(func() {
							_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(
								testcontext.SEMP().ConfigCtx(),
								sempconfig.MsgVpn{TlsAllowDowngradeToPlainTextEnabled: helpers.True},
								testcontext.Messaging().VPN,
								nil,
							)
							Expect(err).ToNot(HaveOccurred())
						})
						AfterEach(func() {
							_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(
								testcontext.SEMP().ConfigCtx(),
								sempconfig.MsgVpn{TlsAllowDowngradeToPlainTextEnabled: helpers.False},
								testcontext.Messaging().VPN,
								nil,
							)
							Expect(err).ToNot(HaveOccurred())
						})
						It("should be able to connect with downgrade to plaintext on", func() {
							builder.WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().Downgradable())
							helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
								Expect(**client.TlsDowngradedToPlainText).To(BeTrue())
							})
						})
					}) // Downgrade to plaintext end

					// Compression over TLS tests
					Context("when using compression", func() {
						validCompressionLevels := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
						for _, validCompressionLevel := range validCompressionLevels {
							var compressionLevel = validCompressionLevel
							It("should be able to connect using compression level "+fmt.Sprint(compressionLevel), func() {
								builder.WithMessageCompression(compressionLevel)
								helpers.TestConnectDisconnectMessagingService(builder)
							})
						}
						invalidCompressionLevels := []int{-1, 10}
						for _, invalidCompressionLevel := range invalidCompressionLevels {
							var compressionLevel = invalidCompressionLevel
							It("should not able to build using compression level "+fmt.Sprint(compressionLevel), func() {
								builder.WithMessageCompression(compressionLevel)
								_, err := builder.Build()
								Expect(err).To(HaveOccurred())
								Expect(err).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
							})
						}
						// Compression level of 0 is allowed on the TLS port
						It("should be able to connect using compression level 0", func() {
							builder.WithMessageCompression(0)
							helpers.TestConnectDisconnectMessagingService(builder)
						})
					}) // tls compression end

					// This context installs a bad CA to the broker that will reject the valid client certificate we pass to the broker
					Context("when using invalid trusted certificate for client certificate authentication", func() {
						const certificateAuthorityName = "test-bad-ca-1"
						// TODO consider moving some of this configuration to the startup
						BeforeEach(func() {
							_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(),
								sempconfig.MsgVpn{AuthenticationClientCertEnabled: helpers.True}, testcontext.Messaging().VPN, nil)
							Expect(err).ToNot(HaveOccurred())
							certContent, err := ioutil.ReadFile(constants.InvalidClientCertificatePEM)
							Expect(err).ToNot(HaveOccurred())
							_, _, err = testcontext.SEMP().Config().ClientCertAuthorityApi.CreateClientCertAuthority(testcontext.SEMP().ConfigCtx(), sempconfig.ClientCertAuthority{
								CertAuthorityName:           certificateAuthorityName,
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
						})
						AfterEach(func() {
							_, _, err := testcontext.SEMP().Config().MsgVpnApi.UpdateMsgVpn(testcontext.SEMP().ConfigCtx(),
								sempconfig.MsgVpn{AuthenticationClientCertEnabled: helpers.False}, testcontext.Messaging().VPN, nil)
							Expect(err).ToNot(HaveOccurred())
							_, _, err = testcontext.SEMP().Config().ClientCertAuthorityApi.DeleteClientCertAuthority(testcontext.SEMP().ConfigCtx(), certificateAuthorityName)
							Expect(err).ToNot(HaveOccurred())
						})
						It("fails to connect with client certificate authentication", func() {
							builder.WithAuthenticationStrategy(config.ClientCertificateAuthentication(
								constants.ValidClientCertificateFile,
								constants.ValidClientKeyFile,
								constants.ValidCertificateKeyPassword,
							))
							helpers.TestFailedConnectMessagingService(builder, func(err error) {
								Expect(err.Error()).To(ContainSubstring("Untrusted Certificate"))
							})
						})
					}) // Invalid Client Cert end
				} // end tcps only
			}) // Valid TLS end
		}) // TLS end
	} // End tls describe loop

	Describe("MessagingService configured feature tests", func() {
		const topic = "messaging-service-tests"

		It("can set the client name", func() {
			const name = "MyClientName"
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ClientPropertyName: name,
			})
			helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
				Expect(client.ClientName).To(Equal(name))
			})
		})

		It("generates a client name when set to an empty string", func() {
			const name = ""
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ClientPropertyName: name,
			})
			helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
				Expect(len(client.ClientName)).To(BeNumerically(">", 0))
			})
		})

		It("fails to set the client name to a large string", func() {
			nameAsBytes := make([]byte, 161)
			for i := 0; i < len(nameAsBytes); i++ {
				nameAsBytes[i] = 'a'
			}
			name := string(nameAsBytes)
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ClientPropertyName: name,
			})
			service, err := builder.Build()
			helpers.ValidateError(err, &solace.InvalidConfigurationError{})
			Expect(service).To(BeNil())
		})

		It("can set the client description", func() {
			const description = "this is a description"
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ClientPropertyApplicationDescription: description,
			})
			helpers.TestConnectDisconnectMessagingServiceClientValidation(builder, func(client *monitor.MsgVpnClient) {
				Expect(client.Description).To(Equal(description))
			})
		})

		It("can set the vpn name to a nonexistant VPN name and fail to connect", func() {
			const vpnName = "MyMsgVpn"
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyVPNName: vpnName,
			})
			messagingService := helpers.BuildMessagingService(builder)
			err := messagingService.Connect()
			helpers.ValidateNativeError(err, subcode.MsgVpnNotAllowed)
		})

		// sender id

		It("validates that by default generate sender id is false", func() {
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				senderID, present := msg.GetSenderID()
				Expect(present).To(BeFalse())
				Expect(senderID).To(Equal(""))
			})
		})

		It("can set generate sender ID to true", func() {
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyGenerateSenderID: true,
			})
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				senderID, present := msg.GetSenderID()
				Expect(present).To(BeTrue())
				Expect(senderID).ToNot(Equal(""))
			})
		})

		It("can set generate sender ID to false", func() {
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyGenerateSenderID: false,
			})
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				senderID, present := msg.GetSenderID()
				Expect(present).To(BeFalse())
				Expect(senderID).To(Equal(""))
			})
		})

		// sender timestamps

		It("validates that by default generate sender timestamps is false", func() {
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyGenerateSendTimestamps: false,
			})
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				senderTimestamp, present := msg.GetSenderTimestamp()
				Expect(present).To(BeFalse())
				Expect(senderTimestamp.IsZero()).To(BeTrue())
			})
		})

		It("can set generate sender timestamps to true", func() {
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyGenerateSendTimestamps: true,
			})
			startTime := time.Now()
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				endTime := time.Now()
				senderTimestamp, present := msg.GetSenderTimestamp()
				Expect(present).To(BeTrue())
				Expect(senderTimestamp).To(BeTemporally(">", startTime))
				Expect(senderTimestamp).To(BeTemporally("<", endTime))
			})
		})

		It("can set generate sender timestamps to false", func() {
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyGenerateSendTimestamps: false,
			})
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				senderTimestamp, present := msg.GetSenderTimestamp()
				Expect(present).To(BeFalse())
				Expect(senderTimestamp.IsZero()).To(BeTrue())
			})
		})

		// receiver timestamps

		It("validates the default generate receiver timestamp behaviour", func() {
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				receiverTimestamp, present := msg.GetTimeStamp()
				Expect(present).To(BeFalse())
				Expect(receiverTimestamp.IsZero()).To(BeTrue())
			})
		})

		It("can set generate receiver timestamps to true", func() {
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyGenerateReceiveTimestamps: true,
			})
			startTime := time.Now()
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				endTime := time.Now()
				receiverTimestamp, present := msg.GetTimeStamp()
				Expect(present).To(BeTrue())
				Expect(receiverTimestamp).To(BeTemporally(">", startTime))
				Expect(receiverTimestamp).To(BeTemporally("<", endTime))
			})
		})

		It("can set generate receiver timestamps to false", func() {
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ServicePropertyGenerateReceiveTimestamps: false,
			})
			helpers.TestConnectPublishAndReceive(builder, topic, nil, func(msg message.InboundMessage) {
				receiverTimestamp, present := msg.GetTimeStamp()
				Expect(present).To(BeFalse())
				Expect(receiverTimestamp.IsZero()).To(BeTrue(), receiverTimestamp.String()+" was not 0")
			})
		})

		It("can set direct subscription reapply to true", func() {
			builder.WithReconnectionRetryStrategy(config.RetryStrategyForeverRetryWithInterval(10 * time.Millisecond)).
				FromConfigurationProvider(config.ServicePropertyMap{
					config.ServicePropertyReceiverDirectSubscriptionReapply: true,
				})
			messagingService := helpers.BuildMessagingService(builder)
			helpers.ConnectMessagingService(messagingService)
			defer helpers.DisconnectMessagingService(messagingService)
			msgChan := helpers.ReceiveOneMessage(messagingService, topic)
			reconnectChan := make(chan struct{})
			messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
				close(reconnectChan)
			})
			helpers.ForceDisconnectViaSEMPv2(messagingService)
			Eventually(reconnectChan).Should(BeClosed())
			helpers.PublishOneMessage(messagingService, topic)
			Eventually(msgChan).Should(Receive())
		})

		It("can set direct subscription reapply to false", func() {
			builder.WithReconnectionRetryStrategy(config.RetryStrategyForeverRetryWithInterval(10 * time.Millisecond)).
				FromConfigurationProvider(config.ServicePropertyMap{
					config.ServicePropertyReceiverDirectSubscriptionReapply: false,
				})
			messagingService := helpers.BuildMessagingService(builder)
			helpers.ConnectMessagingService(messagingService)
			defer helpers.DisconnectMessagingService(messagingService)
			msgChan := helpers.ReceiveOneMessage(messagingService, topic)
			reconnectChan := make(chan struct{})
			messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
				close(reconnectChan)
			})
			helpers.ForceDisconnectViaSEMPv2(messagingService)
			Eventually(reconnectChan).Should(BeClosed())
			helpers.PublishOneMessage(messagingService, topic)
			Consistently(msgChan).ShouldNot(Receive())
		})
	})

	Describe("MessagingService lifecycle and retry tests", func() {

		var eventChannel chan solace.ServiceEvent
		var secondEventChannel chan solace.ServiceEvent
		var expectedHost interface{}
		// helper functions
		var receiveEventWithOffset = func(offset int, eventChannel chan solace.ServiceEvent, codes ...subcode.Code) {
			var serviceEvent solace.ServiceEvent
			EventuallyWithOffset(offset, eventChannel, 5*time.Second).Should(Receive(&serviceEvent), "Expected to receive service event")
			ExpectWithOffset(offset, time.Since(serviceEvent.GetTimestamp())).To(BeNumerically("<=", 1*time.Second), "Expected timestamp of service event to be recent")
			ExpectWithOffset(offset, serviceEvent.GetBrokerURI()).To(Equal(expectedHost), fmt.Sprintf("Expected broker URI to equal %v", expectedHost))
			if len(codes) > 0 {
				helpers.ValidateNativeError(serviceEvent.GetCause(), codes...)
			} else {
				ExpectWithOffset(offset, serviceEvent.GetCause()).ToNot(HaveOccurred(), "Expected error to not have occurred")
			}
		}
		var receiveEvent = func(eventChannel chan solace.ServiceEvent, codes ...subcode.Code) {
			receiveEventWithOffset(2, eventChannel, codes...)
		}
		var receiveOneEvent = func(eventChannel chan solace.ServiceEvent, codes ...subcode.Code) {
			receiveEventWithOffset(2, eventChannel, codes...)
			ConsistentlyWithOffset(1, eventChannel).ShouldNot(Receive())
		}

		var messagingService solace.MessagingService

		BeforeEach(func() {
			builder.WithReconnectionRetryStrategy(config.RetryStrategyParameterizedRetry(3, 1*time.Second))
			expectedHost = helpers.DefaultConfiguration()[config.TransportLayerPropertyHost]
			eventChannel = make(chan solace.ServiceEvent, 1)
			secondEventChannel = make(chan solace.ServiceEvent, 1)
		})

		AfterEach(func() {
			if messagingService != nil && messagingService.IsConnected() {
				helpers.DisconnectMessagingService(messagingService)
			}
		})

		It("calls the reconnect listener when reconnect occurs", func() {
			messagingService = helpers.BuildMessagingService(builder)
			helpers.ConnectMessagingService(messagingService)

			messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
				eventChannel <- event
			})

			helpers.ForceDisconnectViaSEMPv2(messagingService)
			receiveOneEvent(eventChannel, subcode.CommunicationError)
		})

		It("calls multiple reconnect listener when reconnect occurs", func() {
			messagingService = helpers.BuildMessagingService(builder)
			helpers.ConnectMessagingService(messagingService)

			messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
				eventChannel <- event
			})
			messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
				secondEventChannel <- event
			})

			helpers.ForceDisconnectViaSEMPv2(messagingService)
			receiveOneEvent(eventChannel, subcode.CommunicationError)
			receiveOneEvent(secondEventChannel, subcode.CommunicationError)
		})

		It("does not call the reconnect listener when reconnect occurs after removed", func() {
			messagingService = helpers.BuildMessagingService(builder)
			helpers.ConnectMessagingService(messagingService)

			listenerID := messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
				eventChannel <- event
			})
			messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
				secondEventChannel <- event
			})

			helpers.ForceDisconnectViaSEMPv2(messagingService)
			receiveOneEvent(eventChannel, subcode.CommunicationError)
			receiveOneEvent(secondEventChannel, subcode.CommunicationError)

			messagingService.RemoveReconnectionListener(listenerID)

			helpers.ForceDisconnectViaSEMPv2(messagingService)
			Consistently(eventChannel).ShouldNot(Receive())
			receiveOneEvent(secondEventChannel, subcode.CommunicationError)

		})

		It("calls the service interruption listener when disconnect occurs", func() {
			builder.WithReconnectionRetryStrategy(config.RetryStrategyNeverRetry())
			messagingService = helpers.BuildMessagingService(builder)
			helpers.ConnectMessagingService(messagingService)

			messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
				eventChannel <- event
			})

			helpers.ForceDisconnectViaSEMPv2(messagingService)
			receiveOneEvent(eventChannel, subcode.CommunicationError)
		})

		It("calls multiple service interruption listener when disconnect occurs", func() {
			builder.WithReconnectionRetryStrategy(config.RetryStrategyNeverRetry())
			messagingService = helpers.BuildMessagingService(builder)
			helpers.ConnectMessagingService(messagingService)

			messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
				eventChannel <- event
			})
			messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
				secondEventChannel <- event
			})

			helpers.ForceDisconnectViaSEMPv2(messagingService)
			receiveOneEvent(eventChannel, subcode.CommunicationError)
			receiveOneEvent(secondEventChannel, subcode.CommunicationError)
		})

		It("does not call the service interruption listener when disconnect occurs after removed", func() {
			builder.WithReconnectionRetryStrategy(config.RetryStrategyNeverRetry())
			messagingService = helpers.BuildMessagingService(builder)
			helpers.ConnectMessagingService(messagingService)

			handlerID := messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
				eventChannel <- event
			})
			messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
				secondEventChannel <- event
			})
			messagingService.RemoveServiceInterruptionListener(handlerID)

			helpers.ForceDisconnectViaSEMPv2(messagingService)
			Consistently(eventChannel).ShouldNot(Receive())
			receiveOneEvent(secondEventChannel, subcode.CommunicationError)
		})

		Context("With ToxiProxy", func() {
			BeforeEach(func() {
				helpers.CheckToxiProxy()
				builder = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.ToxicConfiguration())
				expectedHost = helpers.ToxicConfiguration()[config.TransportLayerPropertyHost]
			})

			AfterEach(func() {
				// Just in case
				testcontext.Toxi().SMF().Enable()
			})

			It("should connect over toxi proxy and call the reconnection listener when disconnected", func() {
				messagingService = helpers.BuildMessagingService(builder)
				helpers.ConnectMessagingService(messagingService)

				reconnectAttemptEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					reconnectAttemptEventChannel <- event
				})

				// Disconnect
				testcontext.Toxi().SMF().Disable()
				receiveEvent(reconnectAttemptEventChannel, subcode.CommunicationError)

				// Reconnect
				testcontext.Toxi().SMF().Enable()
				Consistently(reconnectAttemptEventChannel).ShouldNot(Receive())
			})
			It("should connect over toxi proxy and call multiple reconnection listener when disconnected", func() {
				messagingService = helpers.BuildMessagingService(builder)
				helpers.ConnectMessagingService(messagingService)

				messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					eventChannel <- event
				})
				messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					secondEventChannel <- event
				})

				// Disconnect
				testcontext.Toxi().SMF().Disable()
				receiveEvent(eventChannel, subcode.CommunicationError)
				receiveEvent(secondEventChannel, subcode.CommunicationError)

				// Reconnect
				testcontext.Toxi().SMF().Enable()
				Consistently(eventChannel).ShouldNot(Receive())
				Consistently(secondEventChannel).ShouldNot(Receive())
			})

			It("should connect over toxi proxy and not call the reconnection listener once removed", func() {
				messagingService = helpers.BuildMessagingService(builder)
				helpers.ConnectMessagingService(messagingService)

				reconnectAttemptID := messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					eventChannel <- event
				})
				messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					secondEventChannel <- event
				})
				messagingService.RemoveReconnectionAttemptListener(reconnectAttemptID)

				// Disconnect
				testcontext.Toxi().SMF().Disable()
				receiveEvent(secondEventChannel, subcode.CommunicationError)
				// Reconnect
				testcontext.Toxi().SMF().Enable()
				Consistently(eventChannel).ShouldNot(Receive())
			})

			It("should retry forrever when not connected", func() {
				messagingService = helpers.BuildMessagingService(
					builder.WithConnectionRetryStrategy(config.RetryStrategyForeverRetry()),
				)
				testcontext.Toxi().SMF().Disable()

				connection := messagingService.ConnectAsync()
				// 5 seconds is the same as forever right? right...?
				Consistently(connection, 5*time.Second).ShouldNot(Receive())

				testcontext.Toxi().SMF().Enable()
				Eventually(connection, 5*time.Second).Should(Receive(BeNil()))
			})

			It("should retry forrever when not connected with interval", func() {
				messagingService = helpers.BuildMessagingService(builder.WithConnectionRetryStrategy(config.RetryStrategyForeverRetryWithInterval(10 * time.Millisecond)))
				testcontext.Toxi().SMF().Disable()

				connection := messagingService.ConnectAsync()
				Consistently(connection, 2*time.Second).ShouldNot(Receive())

				testcontext.Toxi().SMF().Enable()
				Eventually(connection, 200*time.Millisecond).Should(Receive(BeNil()))
			})

			It("should retry a limited number of times when not connected with parameterized retry", func() {
				messagingService = helpers.BuildMessagingService(builder.WithConnectionRetryStrategy(config.RetryStrategyParameterizedRetry(5, 200*time.Millisecond)))
				testcontext.Toxi().SMF().Disable()
				defer testcontext.Toxi().SMF().Enable()

				connection := messagingService.ConnectAsync()
				Consistently(connection, 500*time.Millisecond).ShouldNot(Receive())

				var err error
				Eventually(connection).Should(Receive(&err))
				helpers.ValidateNativeError(err, subcode.CommunicationError)
			})

			It("should never retry connection when never retry is specified", func() {
				messagingService = helpers.BuildMessagingService(builder.WithConnectionRetryStrategy(config.RetryStrategyNeverRetry()))
				testcontext.Toxi().SMF().Disable()
				defer testcontext.Toxi().SMF().Enable()

				connection := messagingService.ConnectAsync()
				var err error
				Eventually(connection).Should(Receive(&err))
				helpers.ValidateNativeError(err, subcode.CommunicationError)
			})

			It("should retry reconnection forever when disconnected", func() {
				messagingService = helpers.BuildMessagingService(builder.WithReconnectionRetryStrategy(config.RetryStrategyForeverRetry()))
				helpers.ConnectMessagingService(messagingService)

				reconnectAttemptEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					reconnectAttemptEventChannel <- event
				})
				reconnectedEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
					reconnectedEventChannel <- event
				})
				serviceInterruptedChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
					serviceInterruptedChannel <- event
				})

				// Disconnect
				testcontext.Toxi().SMF().Disable()
				receiveEvent(reconnectAttemptEventChannel, subcode.CommunicationError)

				Consistently(serviceInterruptedChannel, 10*time.Second).ShouldNot(Receive())
				Consistently(reconnectedEventChannel).ShouldNot(Receive())

				// Reconnect
				testcontext.Toxi().SMF().Enable()
				Eventually(reconnectedEventChannel, 10*time.Second).Should(Receive())
				Consistently(reconnectAttemptEventChannel).ShouldNot(Receive())
				Consistently(serviceInterruptedChannel).ShouldNot(Receive())
			})

			It("should retry reconnection with interval when disconnected", func() {
				messagingService = helpers.BuildMessagingService(builder.WithReconnectionRetryStrategy(config.RetryStrategyForeverRetryWithInterval(10 * time.Millisecond)))
				helpers.ConnectMessagingService(messagingService)

				reconnectAttemptEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					reconnectAttemptEventChannel <- event
				})
				reconnectedEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
					reconnectedEventChannel <- event
				})
				serviceInterruptedChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
					serviceInterruptedChannel <- event
				})

				// Disconnect
				testcontext.Toxi().SMF().Disable()
				receiveEvent(reconnectAttemptEventChannel, subcode.CommunicationError)

				Consistently(serviceInterruptedChannel, 5*time.Second).ShouldNot(Receive())
				Consistently(reconnectedEventChannel).ShouldNot(Receive())

				// Reconnect, should happen pretty fast
				testcontext.Toxi().SMF().Enable()
				Eventually(reconnectedEventChannel).Should(Receive())
				Consistently(reconnectAttemptEventChannel).ShouldNot(Receive())
				Consistently(serviceInterruptedChannel).ShouldNot(Receive())
			})

			It("should retry reconnection a set number of times", func() {
				messagingService = helpers.BuildMessagingService(builder.WithReconnectionRetryStrategy(config.RetryStrategyParameterizedRetry(5, 100*time.Millisecond)))
				helpers.ConnectMessagingService(messagingService)

				reconnectAttemptEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					reconnectAttemptEventChannel <- event
				})
				reconnectedEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
					reconnectedEventChannel <- event
				})
				serviceInterruptedChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
					serviceInterruptedChannel <- event
				})

				// Disconnect
				testcontext.Toxi().SMF().Disable()
				defer testcontext.Toxi().SMF().Enable()

				receiveEvent(reconnectAttemptEventChannel, subcode.CommunicationError)

				// we expect it to retry for at least 500ms
				Consistently(serviceInterruptedChannel, 500*time.Millisecond).ShouldNot(Receive())
				Consistently(reconnectedEventChannel).ShouldNot(Receive())

				// Reconnect
				Eventually(serviceInterruptedChannel, 1*time.Second).Should(Receive())
				Consistently(reconnectAttemptEventChannel).ShouldNot(Receive())
				Consistently(reconnectedEventChannel).ShouldNot(Receive())
			})

			It("should never retry reconnection", func() {
				messagingService = helpers.BuildMessagingService(builder.WithReconnectionRetryStrategy(config.RetryStrategyNeverRetry()))
				helpers.ConnectMessagingService(messagingService)

				reconnectAttemptEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionAttemptListener(func(event solace.ServiceEvent) {
					reconnectAttemptEventChannel <- event
				})
				reconnectedEventChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddReconnectionListener(func(event solace.ServiceEvent) {
					reconnectedEventChannel <- event
				})
				serviceInterruptedChannel := make(chan solace.ServiceEvent, 1)
				messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
					serviceInterruptedChannel <- event
				})

				// Disconnect
				testcontext.Toxi().SMF().Disable()
				defer testcontext.Toxi().SMF().Enable()
				// Should go straight to disconnected
				Eventually(serviceInterruptedChannel, 100*time.Millisecond).Should(Receive())
				Consistently(reconnectAttemptEventChannel).ShouldNot(Receive())
				Consistently(reconnectedEventChannel).ShouldNot(Receive())
			})
		})
	})
})

type NilMessagingServiceConfigProvider struct{}

func (cp *NilMessagingServiceConfigProvider) GetConfiguration() config.ServicePropertyMap {
	return nil
}

var _ = Describe("MessagingServiceBuilder Validation", func() {
	var builder solace.MessagingServiceBuilder
	var hostAndVPNConfig = config.ServicePropertyMap{
		config.ServicePropertyVPNName:     "default",
		config.TransportLayerPropertyHost: "localhost",
	}
	BeforeEach(func() {
		builder = messaging.NewMessagingServiceBuilder()
	})
	It("should not print secrets", func() {
		username := "hello"
		password := "world"
		authStrategy := config.BasicUserNamePasswordAuthentication(username, password)
		authStr := fmt.Sprint(authStrategy)
		Expect(authStr).ToNot(ContainSubstring(password))
		authMapStr := fmt.Sprint(authStrategy.ToProperties())
		Expect(authMapStr).ToNot(ContainSubstring(password))
		builder.FromConfigurationProvider(helpers.DefaultConfiguration()).
			WithAuthenticationStrategy(authStrategy)
		builderStr := fmt.Sprint(builder)
		Expect(builderStr).ToNot(ContainSubstring(password))
		messagingService := helpers.BuildMessagingService(builder)
		messagingServiceStr := fmt.Sprint(messagingService)
		Expect(messagingServiceStr).ToNot(ContainSubstring(password))
	})
	It("should not panic when passing a nil config provider", func() {
		builder.FromConfigurationProvider(helpers.DefaultConfiguration())
		Expect(func() {
			builder.FromConfigurationProvider(nil)
		}).ToNot(Panic())
		helpers.BuildMessagingService(builder)
	})
	It("should not panic when passing a nil property", func() {
		builder.FromConfigurationProvider(helpers.DefaultConfiguration())
		Expect(func() {
			builder.FromConfigurationProvider(config.ServicePropertyMap{
				config.ClientPropertyName: nil,
			})
		}).ToNot(Panic())
		messagingService := helpers.BuildMessagingService(builder)
		Expect(messagingService.GetApplicationID()).ToNot(BeEmpty())
	})
	It("should not set nil properties", func() {
		builder.FromConfigurationProvider(helpers.DefaultConfiguration())
		builder.FromConfigurationProvider(config.ServicePropertyMap{
			config.ClientPropertyName: nil,
		})
		messagingService := helpers.BuildMessagingService(builder)
		Expect(messagingService.GetApplicationID()).ToNot(BeEmpty())
	})
	It("should handle an invalid authentication scheme", func() {
		builder.FromConfigurationProvider(helpers.DefaultConfiguration())
		builder.FromConfigurationProvider(config.ServicePropertyMap{
			config.AuthenticationPropertyScheme: "some auth scheme",
		})
		messagingService, err := builder.Build()
		Expect(messagingService).To(BeNil())
		helpers.ValidateError(err, &solace.InvalidConfigurationError{})
	})
	It("should not panic when passing a config provider providing a nil map", func() {
		builder.FromConfigurationProvider(helpers.DefaultConfiguration())
		Expect(func() {
			builder.FromConfigurationProvider(&NilMessagingServiceConfigProvider{})
		}).ToNot(Panic())
		helpers.BuildMessagingService(builder)
	})
	It("should fail to connect to an unreachable host list with invalid hosts", func() {
		builder.FromConfigurationProvider(config.ServicePropertyMap{
			config.ServicePropertyVPNName:     "default",
			config.TransportLayerPropertyHost: "tc://localhost1:55443,tcps://localhost2:55443,qcps://localhost:35443",
		}).WithAuthenticationStrategy(config.BasicUserNamePasswordAuthentication("hello", "world"))
		_, err := builder.Build()
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(&solace.ServiceUnreachableError{}))
	})
	It("should fail to build without host", func() {
		_, err := builder.FromConfigurationProvider(config.ServicePropertyMap{
			config.ServicePropertyVPNName: "default",
		}).Build()
		helpers.ValidateError(err, &solace.InvalidConfigurationError{}, string(config.TransportLayerPropertyHost))
	})
	It("should fail to build without vpn", func() {
		_, err := builder.FromConfigurationProvider(config.ServicePropertyMap{
			config.TransportLayerPropertyHost: "localhost",
		}).Build()
		helpers.ValidateError(err, &solace.InvalidConfigurationError{}, string(config.ServicePropertyVPNName))
	})
	It("should fail to build without basic username", func() {
		_, err := builder.FromConfigurationProvider(hostAndVPNConfig).Build()
		helpers.ValidateError(err, &solace.InvalidConfigurationError{}, config.AuthenticationSchemeBasic,
			string(config.AuthenticationPropertySchemeBasicUserName))
	})
	It("should fail to build without basic password", func() {
		_, err := builder.FromConfigurationProvider(hostAndVPNConfig).FromConfigurationProvider(config.ServicePropertyMap{
			config.AuthenticationPropertySchemeBasicUserName: "not nil",
		}).Build()
		helpers.ValidateError(err, &solace.InvalidConfigurationError{}, config.AuthenticationSchemeBasic,
			string(config.AuthenticationPropertySchemeBasicPassword))
	})
	It("should fail to build without client certificate", func() {
		_, err := builder.FromConfigurationProvider(hostAndVPNConfig).FromConfigurationProvider(config.ServicePropertyMap{
			config.AuthenticationPropertyScheme: config.AuthenticationSchemeClientCertificate,
		}).Build()
		helpers.ValidateError(err, &solace.InvalidConfigurationError{}, config.AuthenticationSchemeClientCertificate,
			string(config.AuthenticationPropertySchemeSSLClientCertFile))
	})
	It("should fail to build without client certificate key", func() {
		_, err := builder.FromConfigurationProvider(hostAndVPNConfig).FromConfigurationProvider(config.ServicePropertyMap{
			config.AuthenticationPropertyScheme:                  config.AuthenticationSchemeClientCertificate,
			config.AuthenticationPropertySchemeSSLClientCertFile: "/path/to/cert/file",
		}).Build()
		helpers.ValidateError(err, &solace.InvalidConfigurationError{}, config.AuthenticationSchemeClientCertificate,
			string(config.AuthenticationPropertySchemeSSLClientPrivateKeyFile))
	})
	It("should fail to build without oauth client", func() {
		_, err := builder.FromConfigurationProvider(hostAndVPNConfig).FromConfigurationProvider(config.ServicePropertyMap{
			config.AuthenticationPropertyScheme: config.AuthenticationSchemeOAuth2,
		}).Build()
		helpers.ValidateError(err, &solace.InvalidConfigurationError{}, config.AuthenticationSchemeOAuth2,
			string(config.AuthenticationPropertySchemeOAuth2AccessToken))
	})
	integerProperties := []config.ServiceProperty{
		config.TransportLayerPropertyConnectionAttemptsTimeout,
		config.TransportLayerPropertyConnectionRetries,
		config.TransportLayerPropertyConnectionRetriesPerHost,
		config.TransportLayerPropertyReconnectionAttempts,
		config.TransportLayerPropertyReconnectionAttemptsWaitInterval,
		config.TransportLayerPropertyKeepAliveInterval,
		config.TransportLayerPropertyKeepAliveWithoutResponseLimit,
		config.TransportLayerPropertySocketOutputBufferSize,
		config.TransportLayerPropertySocketInputBufferSize,
		config.TransportLayerPropertyCompressionLevel,
		config.ServicePropertyProvisionTimeoutMs,
	}
	for _, property := range integerProperties {
		It("should fail to build with "+string(property)+" set to invalid int", func() {
			service, err := builder.FromConfigurationProvider(helpers.DefaultConfiguration()).FromConfigurationProvider(config.ServicePropertyMap{
				property: "not_an_int",
			}).Build()
			Expect(service).To(BeNil())
			helpers.ValidateNativeError(err, subcode.ParamOutOfRange)
		})
	}
})

var _ = Describe("API Info", func() {
	var builder solace.MessagingServiceBuilder
	BeforeEach(func() {
		builder = messaging.NewMessagingServiceBuilder().FromConfigurationProvider(helpers.DefaultConfiguration())
	})
	It("can retrieve the version from the API", func() {
		messagingService := helpers.BuildMessagingService(builder)
		client := helpers.ConnectMessagingService(messagingService)
		apiInfo := messagingService.Info()
		Expect(apiInfo).ToNot(BeNil())
		Expect(apiInfo.GetAPIVersion()).ToNot(BeEmpty())
		Expect(client.SoftwareVersion).To(Equal(apiInfo.GetAPIVersion()))
		helpers.DisconnectMessagingService(messagingService)
	})
	It("can retrieve the build date from the API", func() {
		messagingService := helpers.BuildMessagingService(builder)
		client := helpers.ConnectMessagingService(messagingService)
		apiInfo := messagingService.Info()
		Expect(apiInfo).ToNot(BeNil())
		Expect(apiInfo.GetAPIBuildDate()).ToNot(BeEmpty())
		Expect(client.SoftwareDate).To(Equal(apiInfo.GetAPIBuildDate()))
		helpers.DisconnectMessagingService(messagingService)
	})
	It("can retrieve the platform from the API", func() {
		messagingService := helpers.BuildMessagingService(builder)
		client := helpers.ConnectMessagingService(messagingService)
		apiInfo := messagingService.Info()
		Expect(apiInfo).ToNot(BeNil())
		Expect(apiInfo.GetAPIImplementationVendor()).ToNot(BeEmpty())
		Expect(client.Platform).To(Equal(apiInfo.GetAPIImplementationVendor()))
		helpers.DisconnectMessagingService(messagingService)
	})
	It("can retrieve the api name from the API", func() {
		messagingService := helpers.BuildMessagingService(builder)
		client := helpers.ConnectMessagingService(messagingService)
		apiInfo := messagingService.Info()
		Expect(apiInfo).ToNot(BeNil())
		Expect(apiInfo.GetAPIUserID()).ToNot(BeEmpty())
		Expect(client.ClientName).To(Equal(apiInfo.GetAPIUserID()))
		helpers.DisconnectMessagingService(messagingService)
	})
	It("can retrieve the api name from the API when overridden", func() {
		const appID = "myAppId"
		messagingService, err := builder.BuildWithApplicationID(appID)
		Expect(err).ToNot(HaveOccurred())
		client := helpers.ConnectMessagingService(messagingService)
		apiInfo := messagingService.Info()
		Expect(apiInfo).ToNot(BeNil())
		Expect(apiInfo.GetAPIUserID()).To(Equal(appID))
		Expect(client.ClientName).To(Equal(apiInfo.GetAPIUserID()))
		helpers.DisconnectMessagingService(messagingService)
	})
})
