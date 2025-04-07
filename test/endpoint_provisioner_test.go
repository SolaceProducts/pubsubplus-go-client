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
	"math"
	"net/url"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const provisionQueueName = "testProvisionQueue"

var _ = Describe("EndpointProvisioner", func() {
	var messagingService solace.MessagingService
	BeforeEach(func() {
		var err error
		messagingService, err = messaging.NewMessagingServiceBuilder().
			FromConfigurationProvider(helpers.DefaultConfiguration()).
			WithProvisionTimeoutMs(1 * time.Millisecond).
			Build()
		Expect(err).To(BeNil())
	})

	Context("with an unconnected messaging service", func() {

		It("fails to provision a non-durable endpoint", func() {
			outcome := messagingService.EndpointProvisioner().FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable: false,
			}).Provision(provisionQueueName, true)
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			Expect(outcome.GetStatus()).To(BeFalse())
		})

		It("fails to provision when given a max message redelivery range < 0", func() {
			outcome := messagingService.EndpointProvisioner().FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyMaxMessageRedelivery: -1,
			}).Provision(provisionQueueName, true)
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			Expect(outcome.GetStatus()).To(BeFalse())
		})

		It("fails to provision when given a max message redelivery range > 255", func() {
			outcome := messagingService.EndpointProvisioner().FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyMaxMessageRedelivery: 256,
			}).Provision(provisionQueueName, true)
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			Expect(outcome.GetStatus()).To(BeFalse())
		})

		It("fails to provision when given an invalid max message size", func() {
			outcome := messagingService.EndpointProvisioner().FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyMaxMessageSize: -1,
			}).Provision(provisionQueueName, true)
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			Expect(outcome.GetStatus()).To(BeFalse())
		})

		It("fails to provision when given an invalid queue quota size", func() {
			outcome := messagingService.EndpointProvisioner().FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyQuotaMB: -1,
			}).Provision(provisionQueueName, true)
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			Expect(outcome.GetStatus()).To(BeFalse())
		})

		It("fails to provision when given an invalid queue permission", func() {
			outcome := messagingService.EndpointProvisioner().FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyPermission: "not a permission",
			}).Provision(provisionQueueName, true)
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			Expect(outcome.GetStatus()).To(BeFalse())
		})

		It("fails to provision when given valid queue properties", func() {
			outcome := messagingService.EndpointProvisioner().FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:   true,
				config.EndpointPropertyExclusive: true,
			}).Provision(provisionQueueName, true)
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			Expect(outcome.GetStatus()).To(BeFalse())
		})

		It("fails to deprovision", func() {
			err := messagingService.EndpointProvisioner().Deprovision(provisionQueueName, true)
			Expect(err).To(HaveOccurred())
			Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
		})
	})

	Context("with a connected messaging service that will be disconnected", func() {
		var eventChannel chan solace.ServiceEvent
		var provisioner solace.EndpointProvisioner
		var listenerID uint64

		BeforeEach(func() {
			helpers.ConnectMessagingService(messagingService)
			eventChannel = make(chan solace.ServiceEvent, 1)
			listenerID = messagingService.AddServiceInterruptionListener(func(event solace.ServiceEvent) {
				eventChannel <- event
			})

			provisioner = messagingService.EndpointProvisioner()
			Expect(provisioner).ToNot(BeNil())
		})

		AfterEach(func() {
			messagingService.RemoveServiceInterruptionListener(listenerID)
			if messagingService.IsConnected() {
				messagingService.Disconnect()
			}
		})

		forceDisconnectFunctions := map[string]func(messagingService solace.MessagingService){
			"messaging service disconnect": func(messagingService solace.MessagingService) {
				helpers.DisconnectMessagingService(messagingService)
			},
			"SEMPv2 disconnect": func(messagingService solace.MessagingService) {
				helpers.ForceDisconnectViaSEMPv2(messagingService)
				var serviceEvent solace.ServiceEvent
				Eventually(eventChannel, 20*time.Second).Should(Receive(&serviceEvent), "Expected to receive service down event")
				Expect(time.Since(serviceEvent.GetTimestamp())).To(BeNumerically("<=", 1*time.Second), "Expected timestamp of service event to be recent")
				helpers.ValidateNativeError(serviceEvent.GetCause(), subcode.CommunicationError)
				Consistently(eventChannel).ShouldNot(Receive()) // should not have another event since messaging service is down
			},
		}

		for testCase, disconnectFunctionRef := range forceDisconnectFunctions {
			disconnectFunction := disconnectFunctionRef

			It("fails to provision when given valid queue properties with "+testCase, func() {
				disconnectFunction(messagingService)

				outcome := provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
					config.EndpointPropertyDurable:   true,
					config.EndpointPropertyExclusive: true,
				}).Provision(provisionQueueName, true)
				Expect(outcome.GetError()).To(HaveOccurred())
				Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
				Expect(outcome.GetStatus()).To(BeFalse())
			})

			It("fails to deprovision with "+testCase, func() {
				disconnectFunction(messagingService)
				Eventually(messagingService.IsConnected(), 30*time.Second).Should(BeFalse())

				err := provisioner.Deprovision(provisionQueueName, true)
				Expect(err).To(HaveOccurred())
				Expect(err).To(BeAssignableToTypeOf(&solace.IllegalStateError{}))
			})
		}
	})

	Context("with a connected messaging service", func() {
		var provisioner solace.EndpointProvisioner

		BeforeEach(func() {
			err := messagingService.Connect()
			Expect(err).To(BeNil())

			provisioner = messagingService.EndpointProvisioner()
			Expect(provisioner).ToNot(BeNil())
		})

		AfterEach(func() {
			err := messagingService.Disconnect()
			Expect(err).To(BeNil())
		})

		provisionFunctions := map[string](func(solace.EndpointProvisioner) solace.ProvisionOutcome){
			"Provision": func(endpointProvisioner solace.EndpointProvisioner) solace.ProvisionOutcome {
				return endpointProvisioner.Provision(provisionQueueName, false)
			},
			"ProvisionAsync": func(endpointProvisioner solace.EndpointProvisioner) solace.ProvisionOutcome {
				return <-endpointProvisioner.ProvisionAsync(provisionQueueName, false)
			},
			"ProvisionAsyncWithCallback": func(endpointProvisioner solace.EndpointProvisioner) solace.ProvisionOutcome {
				startChan := make(chan solace.ProvisionOutcome)
				endpointProvisioner.ProvisionAsyncWithCallback(provisionQueueName, false, func(outcome solace.ProvisionOutcome) {
					startChan <- outcome
				})
				return <-startChan
			},
		}
		for provisionFunctionName, provisionFunction := range provisionFunctions {
			provision := provisionFunction
			It("should successfully provision a queue using provision function "+provisionFunctionName, func() {
				// remove the queue
				defer func() {
					deprovError := provisioner.Deprovision(provisionQueueName, true)
					Expect(deprovError).To(BeNil())
				}()

				// check that the endpoint does not yet exist on the broker via semp
				clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
				Expect(err).ToNot(BeNil())

				// Now let's provision the endpoint using the API
				// TestPlan TestCase Provision#9 - Tried to provision non durable queue(topic endpoint) (durable =false)
				provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
					config.EndpointPropertyDurable:   true,
					config.EndpointPropertyExclusive: true,
				})
				outcome := provision(provisioner)
				Expect(outcome.GetError()).ToNot(HaveOccurred())
				Expect(outcome.GetError()).To(BeNil())
				Expect(outcome.GetStatus()).To(BeTrue())

				// check that the endpoint was provisioned on the broker via semp
				clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
				Expect(err).To(BeNil())
				Expect(clientResponse.Data).ToNot(BeNil())
				Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
				Expect(clientResponse.Data.Durable).ToNot(BeNil())            // durability should not be nil
				Expect(**(clientResponse.Data.Durable)).To(BeTrue())          // durability should be true
				Expect(clientResponse.Data.AccessType).To(Equal("exclusive")) // should be an exclusive queue
			})

			It("should return error outcome from provision when queue exist using provision function "+provisionFunctionName, func() {
				// remove the queue
				defer func() {
					deprovError := provisioner.Deprovision(provisionQueueName, true)
					Expect(deprovError).To(BeNil())
				}()

				// check that the endpoint does not yet exist on the broker via semp
				clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
				Expect(err).ToNot(BeNil())

				provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
					config.EndpointPropertyDurable:   true,
					config.EndpointPropertyExclusive: false,
				})
				outcome := provision(provisioner) // first call to provisioner should be successful
				Expect(outcome.GetError()).ToNot(HaveOccurred())
				Expect(outcome.GetError()).To(BeNil())
				Expect(outcome.GetStatus()).To(BeTrue())

				// check that the endpoint was provisioned on the broker via semp
				clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
				Expect(err).To(BeNil())
				Expect(clientResponse.Data).ToNot(BeNil())
				Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
				Expect(clientResponse.Data.Durable).ToNot(BeNil())                // durability should not be nil
				Expect(**(clientResponse.Data.Durable)).To(Equal(true))           // durability should be true
				Expect(clientResponse.Data.AccessType).To(Equal("non-exclusive")) // should be an exclusive queue

				outcome = provision(provisioner) // second call to provisioner with same queue properties should Not successful
				Expect(outcome.GetError()).To(HaveOccurred())
				Expect(string(outcome.GetError().Error())).To(Equal("Already Exists"))
				Expect(outcome.GetStatus()).To(BeFalse())

				// check that only one endpoint was provisioned on the broker via semp
				clientResponses, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueues(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, nil)
				Expect(err).To(BeNil())
				Expect(clientResponses.Data).ToNot(BeNil())
				Expect(len(clientResponses.Data)).To(Equal(1)) // should only be one queue
				Expect(clientResponses.Data[0].QueueName).To(Equal(provisionQueueName))
			})
		}

		deprovisionFunctions := map[string](func(solace.EndpointProvisioner) error){
			"Deprovision": func(endpointProvisioner solace.EndpointProvisioner) error {
				return endpointProvisioner.Deprovision(provisionQueueName, false)
			},
			"DeprovisionAsync": func(endpointProvisioner solace.EndpointProvisioner) error {
				return <-endpointProvisioner.DeprovisionAsync(provisionQueueName, false)
			},
			"DeprovisionAsyncWithCallback": func(endpointProvisioner solace.EndpointProvisioner) error {
				errChan := make(chan error)
				endpointProvisioner.DeprovisionAsyncWithCallback(provisionQueueName, false, func(e error) {
					errChan <- e
				})
				return <-errChan
			},
		}

		for deprovisionFunctionName, deprovisionFunction := range deprovisionFunctions {
			deprovision := deprovisionFunction
			It("can successfully deprovision queue using "+deprovisionFunctionName, func() {
				// Let's first provision the endpoint using the API
				provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
					config.EndpointPropertyDurable:   true,
					config.EndpointPropertyExclusive: true,
				})
				outcome := provisioner.Provision(provisionQueueName, true)
				Expect(outcome.GetError()).ToNot(HaveOccurred())
				Expect(outcome.GetStatus()).To(BeTrue())

				// check that the endpoint is on the broker
				clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
				Expect(err).ToNot(HaveOccurred())
				Expect(clientResponse.Data).ToNot(BeNil()) // should have a response
				Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
				Expect(**(clientResponse.Data.Durable)).To(BeTrue()) // should have provisioned a durable queue on the broker

				// Now let's deprovision the endpoint using the API
				err = deprovision(provisioner)
				Expect(err).To(BeNil())

				// check that the endpoint was deprovisioned on the broker via semp
				clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
				Expect(err).ToNot(BeNil())
				Expect(clientResponse.Data).To(BeNil())
			})

			It("should return error from deprovision when queue does not exist using deprovision function "+deprovisionFunctionName, func() {
				// check that the endpoint does not yet exist on the broker via semp
				clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
				Expect(err).ToNot(BeNil())
				Expect(clientResponse.Data).To(BeNil())

				err = deprovision(provisioner) // call to deprovisioner should be unsuccessful
				Expect(err).ToNot(BeNil())
				Expect(string(err.Error())).To(Equal("Unknown Queue"))

				// check that we have no endpoint provisioned on the broker via semp
				clientResponses, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueues(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, nil)
				Expect(err).To(BeNil())
				Expect(clientResponses.Data).ToNot(BeNil())
				Expect(len(clientResponses.Data)).To(Equal(0)) // should have no queues
			})
		}

		// invalid queue names to test
		invalidQueueTestCases := map[string]string{
			"NameLongerThan201CharactersQueueName": "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
			"ReplayLogQueueName":                   "#REPLAY_LOG_defaultLog",
			"ReplayTopicsQueueName":                "#REPLAY_TOPICS_defaultLog",
			"PartitionQueuePrefixQueueName":        "#pq/db583a27835baecd/00011",
			"TopicLevelsQueueName":                 "/test1/test2/",
		}
		for queueLabel, queueName := range invalidQueueTestCases {
			// TestPlan TestCase Provision#1
			It("should not provision queue with invalid queueName - "+queueLabel, func() {
				// remove the provisioned queue
				defer func() {
					provisioner.Deprovision(queueName, true)
				}()

				// check that the endpoint does not yet exist on the broker via semp
				clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(queueName), nil)
				Expect(err).ToNot(BeNil())

				// Now let's provision the endpoint using the API
				provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
					config.EndpointPropertyDurable:   true,
					config.EndpointPropertyExclusive: false,
				})
				outcome := provisioner.Provision(queueName, false)
				Expect(outcome.GetStatus()).To(BeFalse())
				Expect(outcome.GetError()).To(HaveOccurred())
				Expect(string(outcome.GetError().Error())).To(Equal("Invalid Queue Name"))

				// check that the endpoint was provisioned on the broker via semp
				clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(queueName), nil)
				Expect(err).ToNot(BeNil())
				Expect(clientResponse.Data).To(BeNil())
			})
		}

		// valid queue names to test
		validQueueTestCases := map[string]string{
			"ReplicationDataQueueName": "#MSGVPN_REPLICATION_DATA_QUEUE",
			"CfgSyncQueueName":         "#CFGSYNC/OWNER/vmr-135-47/RTR/site/CFG",
			"ZeroQueueName":            "00000000000",
		}

		for queueLabel, queueName := range validQueueTestCases {
			// TestPlan TestCase Provision#1
			It("should provision queue with - "+queueLabel, func() {
				// remove the provisioned queue
				defer func() {
					err := provisioner.Deprovision(queueName, true)
					Expect(err).To(BeNil())
				}()

				// check that the endpoint does not yet exist on the broker via semp
				clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(queueName), nil)
				Expect(err).ToNot(BeNil())

				// Now let's provision the endpoint using the API
				provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
					config.EndpointPropertyDurable:   true,
					config.EndpointPropertyExclusive: false,
				})
				outcome := provisioner.Provision(queueName, false)
				Expect(outcome.GetStatus()).To(BeTrue())
				Expect(outcome.GetError()).ToNot(HaveOccurred())

				// check that the endpoint was provisioned on the broker via semp
				clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
					GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(queueName), nil)
				Expect(err).To(BeNil())
				Expect(clientResponse.Data).ToNot(BeNil())
				Expect(clientResponse.Data.QueueName).To(Equal(queueName))
				Expect(clientResponse.Data.Durable).ToNot(BeNil())                // durability should not be nil
				Expect(**(clientResponse.Data.Durable)).To(BeTrue())              // durability should be true
				Expect(clientResponse.Data.AccessType).To(Equal("non-exclusive")) // should be an non-exclusive queue
			})
		}

		It("fails to provision a non-durable endpoint", func() {
			outcome := messagingService.EndpointProvisioner().FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable: false,
			}).Provision(provisionQueueName, true)
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
			Expect(outcome.GetError().Error()).To(Equal("invalid configuration provided: failed to provision endpoint: Attempt to provision a temporary endpoint in solClient_session_endpointProvision"))
			Expect(outcome.GetStatus()).To(BeFalse())
		})

		// TestPlan TestCase Provision#2
		It("provision durable queue with different queue properties - Queue AccessType", func() {
			// remove the provisioned queue
			defer func() {
				err := provisioner.Deprovision(provisionQueueName, true)
				Expect(err).To(BeNil())
			}()

			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())

			// Queue AccessType - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:   true,
				config.EndpointPropertyExclusive: true,
			})
			outcome := provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())            // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())          // durability should be true
			Expect(clientResponse.Data.AccessType).To(Equal("exclusive")) // should be an exclusive queue

			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// Queue AccessType - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:   true,
				config.EndpointPropertyExclusive: false,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())                // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())              // durability should be true
			Expect(clientResponse.Data.AccessType).To(Equal("non-exclusive")) // should be an non-exclusive queue
		})

		// TestPlan TestCase Provision#2
		It("provision durable queue with different queue properties - Queue DiscardBehaviour", func() {
			// remove the provisioned queue
			defer func() {
				err := provisioner.Deprovision(provisionQueueName, true)
				Expect(err).To(BeNil())
			}()

			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())

			// Queue DiscardBehaviour - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:      true,
				config.EndpointPropertyNotifySender: true,
			})
			outcome := provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())                                             // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())                                           // durability should be true
			Expect(clientResponse.Data.RejectMsgToSenderOnDiscardBehavior).To(Equal("when-queue-enabled")) // should be when-queue-enabled

			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// Queue DiscardBehaviour - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:      true,
				config.EndpointPropertyNotifySender: false,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())                                // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())                              // durability should be true
			Expect(clientResponse.Data.RejectMsgToSenderOnDiscardBehavior).To(Equal("never")) // should be never
		})

		// TestPlan TestCase Provision#2
		It("provision durable queue with different queue properties - Queue MaxRedeliveryCount", func() {
			// remove the provisioned queue
			defer func() {
				provisioner.Deprovision(provisionQueueName, true)
			}()

			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())

			// MaxRedeliveryCount(0) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:              true,
				config.EndpointPropertyMaxMessageRedelivery: 0,
			})
			outcome := provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())   // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue()) // durability should be true
			Expect(clientResponse.Data.MaxRedeliveryCount).To(Equal(int64(0)))
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// MaxRedeliveryCount(255) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:              true,
				config.EndpointPropertyMaxMessageRedelivery: 255,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())   // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue()) // durability should be true
			Expect(clientResponse.Data.MaxRedeliveryCount).To(Equal(int64(255)))
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// MaxRedeliveryCount(-1) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:              true,
				config.EndpointPropertyMaxMessageRedelivery: -1,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeFalse())
			Expect(outcome.GetError()).To(HaveOccurred()) // should have an error
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))

			// MaxRedeliveryCount(256) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:              true,
				config.EndpointPropertyMaxMessageRedelivery: 256,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeFalse())
			Expect(outcome.GetError()).To(HaveOccurred()) // should have an error
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))

			// MaxRedeliveryCount(math.MaxInt64) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:              true,
				config.EndpointPropertyMaxMessageRedelivery: math.MaxInt64, // max integer limit
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeFalse())
			Expect(outcome.GetError()).To(HaveOccurred()) // should have an error
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
		})

		// TestPlan TestCase Provision#2
		It("provision durable queue with different queue properties - Queue MaxMessageSize", func() {
			// remove the provisioned queue
			defer func() {
				provisioner.Deprovision(provisionQueueName, true)
			}()

			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())

			// MaxMessageSize(0) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:        true,
				config.EndpointPropertyMaxMessageSize: 0,
			})
			outcome := provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())   // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue()) // durability should be true
			Expect(clientResponse.Data.MaxMsgSize).To(Equal(int32(0)))
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// MaxMessageSize(10000000) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:        true,
				config.EndpointPropertyMaxMessageSize: 10000000,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())   // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue()) // durability should be true
			Expect(clientResponse.Data.MaxMsgSize).To(Equal(int32(10000000)))
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// MaxMessageSize(-1) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:        true,
				config.EndpointPropertyMaxMessageSize: -1,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeFalse())
			Expect(outcome.GetError()).To(HaveOccurred()) // should have an error
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))

			// MaxMessageSize(math.MaxInt32 + 1) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:        true,
				config.EndpointPropertyMaxMessageSize: (math.MaxInt32 + 1), // max integer limit
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeFalse())
			Expect(outcome.GetError()).To(HaveOccurred()) // should have an error
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
		})

		// TestPlan TestCase Provision#2
		It("provision durable queue with different queue properties - Queue EndpointPermission", func() {
			// remove the provisioned queue
			defer func() {
				provisioner.Deprovision(provisionQueueName, true)
			}()

			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())

			// EndpointPermission(None) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:    true,
				config.EndpointPropertyPermission: config.EndpointPermissionNone,
			})
			outcome := provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())            // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())          // durability should be true
			Expect(clientResponse.Data.Permission).To(Equal("no-access")) // queue permission
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// EndpointPermission(ReadOnly) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:    true,
				config.EndpointPropertyPermission: config.EndpointPermissionReadOnly,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())            // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())          // durability should be true
			Expect(clientResponse.Data.Permission).To(Equal("read-only")) // queue permission
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// EndpointPermission(Consume) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:    true,
				config.EndpointPropertyPermission: config.EndpointPermissionConsume,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())          // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())        // durability should be true
			Expect(clientResponse.Data.Permission).To(Equal("consume")) // queue permission
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// EndpointPermission(ModifyTopic) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:    true,
				config.EndpointPropertyPermission: config.EndpointPermissionModifyTopic,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())               // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())             // durability should be true
			Expect(clientResponse.Data.Permission).To(Equal("modify-topic")) // queue permission
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// EndpointPermission(Delete) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:    true,
				config.EndpointPropertyPermission: config.EndpointPermissionDelete,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())         // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())       // durability should be true
			Expect(clientResponse.Data.Permission).To(Equal("delete")) // queue permission
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// EndpointPermission(bob) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:    true,
				config.EndpointPropertyPermission: "bob",
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeFalse())
			Expect(outcome.GetError()).To(HaveOccurred()) // should have an error
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.IllegalArgumentError{}))
		})

		// TestPlan TestCase Provision#2
		It("provision durable queue with different queue properties - Queue QuotaMB", func() {
			// remove the provisioned queue
			defer func() {
				provisioner.Deprovision(provisionQueueName, true)
			}()

			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())

			// QuotaMB(0) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable: true,
				config.EndpointPropertyQuotaMB: 0,
			})
			outcome := provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())   // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue()) // durability should be true
			Expect(clientResponse.Data.MaxMsgSpoolUsage).To(Equal(int64(0)))
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// QuotaMB(6000000) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable: true,
				config.EndpointPropertyQuotaMB: 6000000,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())   // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue()) // durability should be true
			Expect(clientResponse.Data.MaxMsgSpoolUsage).To(Equal(int64(6000000)))
			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// QuotaMB(-1) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable: true,
				config.EndpointPropertyQuotaMB: -1,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeFalse())
			Expect(outcome.GetError()).To(HaveOccurred()) // should have an error
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))

			// QuotaMB(math.MaxInt64) - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable: true,
				config.EndpointPropertyQuotaMB: math.MaxInt64, // max integer limit
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeFalse())
			Expect(outcome.GetError()).To(HaveOccurred()) // should have an error
			Expect(outcome.GetError()).To(BeAssignableToTypeOf(&solace.InvalidConfigurationError{}))
		})

		// TestPlan TestCase Provision#2
		It("provision durable queue with different queue properties - Queue RespectsTTL", func() {
			// remove the provisioned queue
			defer func() {
				err := provisioner.Deprovision(provisionQueueName, true)
				Expect(err).To(BeNil())
			}()

			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())

			// DiscardBehaviour Queue - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:     true,
				config.EndpointPropertyRespectsTTL: true,
			})
			outcome := provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())             // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())           // durability should be true
			Expect(clientResponse.Data.RespectTtlEnabled).ToNot(BeNil())   // should not be nil
			Expect(**(clientResponse.Data.RespectTtlEnabled)).To(BeTrue()) // should be true

			// remove the queue from the broker
			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil())

			// DiscardBehaviour Queue - Now let's provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:     true,
				config.EndpointPropertyRespectsTTL: false,
			})
			outcome = provisioner.Provision(provisionQueueName, false)
			Expect(outcome.GetStatus()).To(BeTrue())
			Expect(outcome.GetError()).ToNot(HaveOccurred())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())              // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(BeTrue())            // durability should be true
			Expect(clientResponse.Data.RespectTtlEnabled).ToNot(BeNil())    // should not be nil
			Expect(**(clientResponse.Data.RespectTtlEnabled)).To(BeFalse()) // should be false
		})

		// TestPlan TestCase Deprovision#4
		It("deprovision a durable queue which has been deprovisioned before", func() {
			// Let's first provision the endpoint using the API
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:   true,
				config.EndpointPropertyExclusive: true,
			})
			outcome := provisioner.Provision(provisionQueueName, true)
			Expect(outcome.GetError()).ToNot(HaveOccurred())
			Expect(outcome.GetStatus()).To(BeTrue())

			// check that the endpoint is on the broker
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(HaveOccurred())
			Expect(clientResponse.Data).ToNot(BeNil()) // should have a response
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(**(clientResponse.Data.Durable)).To(BeTrue()) // should have provisioned a durable queue on the broker

			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil()) // should be successful

			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil()) // we should ignore queue missing errors

			err = provisioner.Deprovision(provisionQueueName, false) // call to deprovisioner should be unsuccessful
			Expect(err).ToNot(BeNil())                               // we should not ignore queue missing errors
			Expect(string(err.Error())).To(Equal("Unknown Queue"))
		})

		// TestPlan TestCase Deprovision#8
		It("should return error from deprovision when queue does not exist on broker", func() {
			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())
			Expect(clientResponse.Data).To(BeNil())

			err = provisioner.Deprovision(provisionQueueName, true)
			Expect(err).To(BeNil()) // we should ignore queue missing errors

			err = provisioner.Deprovision(provisionQueueName, false) // call to deprovisioner should be unsuccessful
			Expect(err).ToNot(BeNil())                               // we should not ignore queue missing errors
			Expect(string(err.Error())).To(Equal("Unknown Queue"))

			// check that we have no endpoint provisioned on the broker via semp
			clientResponses, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueues(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, nil)
			Expect(err).To(BeNil())
			Expect(clientResponses.Data).ToNot(BeNil())
			Expect(len(clientResponses.Data)).To(Equal(0)) // should have no queues
		})

		It("should return error outcome from provision when queue with different properties exist", func() {
			// remove the queue
			defer func() {
				deprovError := provisioner.Deprovision(provisionQueueName, true)
				Expect(deprovError).To(BeNil())
			}()

			// check that the endpoint does not yet exist on the broker via semp
			clientResponse, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).ToNot(BeNil())

			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:              true,
				config.EndpointPropertyExclusive:            false,
				config.EndpointPropertyMaxMessageRedelivery: 20,
				config.EndpointPropertyPermission:           config.EndpointPermissionNone, // no permission
				config.EndpointPropertyRespectsTTL:          true,
				config.EndpointPropertyQuotaMB:              10,
				config.EndpointPropertyNotifySender:         true,
				config.EndpointPropertyMaxMessageSize:       1024,
			})
			outcome := provisioner.Provision(provisionQueueName, true) // should be successful
			Expect(outcome.GetError()).To(BeNil())
			Expect(outcome.GetStatus()).To(BeTrue())

			// check that the endpoint was provisioned on the broker via semp
			clientResponse, _, err = testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueue(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, url.QueryEscape(provisionQueueName), nil)
			Expect(err).To(BeNil())
			Expect(clientResponse.Data).ToNot(BeNil())
			Expect(clientResponse.Data.QueueName).To(Equal(provisionQueueName))
			Expect(clientResponse.Data.Durable).ToNot(BeNil())                  // durability should not be nil
			Expect(**(clientResponse.Data.Durable)).To(Equal(true))             // durability should be true
			Expect(clientResponse.Data.AccessType).To(Equal("non-exclusive"))   // should be an exclusive queue
			Expect(clientResponse.Data.MaxRedeliveryCount).To(Equal(int64(20))) // should same as what was provisioned
			Expect(**(clientResponse.Data.RespectTtlEnabled)).To(BeTrue())      // should same as what was provisioned

			// attempt to provision the same name with different properties
			provisioner = provisioner.FromConfigurationProvider(config.EndpointPropertyMap{
				config.EndpointPropertyDurable:              true,
				config.EndpointPropertyExclusive:            true, // different for existing queue
				config.EndpointPropertyMaxMessageRedelivery: 50,
				config.EndpointPropertyPermission:           config.EndpointPermissionConsume, // consume permission
				config.EndpointPropertyRespectsTTL:          false,                            // different for existing queue
				config.EndpointPropertyQuotaMB:              10,                               //  same as existing queue
				config.EndpointPropertyNotifySender:         true,                             // same as exisiting queue
				config.EndpointPropertyMaxMessageSize:       4096,                             // different for existing queue
			})

			outcome = provisioner.Provision(provisionQueueName, true) // should be unsuccessful
			Expect(outcome.GetError()).To(HaveOccurred())
			Expect(string(outcome.GetError().Error())).To(Equal("Endpoint Property Mismatch"))
			Expect(outcome.GetStatus()).To(BeFalse())

			// check that only one endpoint was provisioned on the broker via semp
			clientResponses, _, err := testcontext.SEMP().Monitor().MsgVpnApi.
				GetMsgVpnQueues(testcontext.SEMP().MonitorCtx(), testcontext.Messaging().VPN, nil)
			Expect(err).To(BeNil())
			Expect(clientResponses.Data).ToNot(BeNil())
			Expect(len(clientResponses.Data)).To(Equal(1)) // should only be one queue
			Expect(clientResponses.Data[0].QueueName).To(Equal(provisionQueueName))
			Expect(clientResponses.Data[0].AccessType).To(Equal("non-exclusive")) // should be the same as the existing queue
			Expect(**(clientResponse.Data.RespectTtlEnabled)).To(BeTrue())        // should be the same as the existing queue
		})
	})
})
