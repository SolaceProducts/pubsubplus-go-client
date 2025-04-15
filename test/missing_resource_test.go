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
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
	"solace.dev/go/messaging/pkg/solace/subcode"
	"solace.dev/go/messaging/test/helpers"
	"solace.dev/go/messaging/test/testcontext"

	sempconfig "solace.dev/go/messaging/test/sempclient/config"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Missing Resource Creation Strategy", func() {

	const myQueue = "MyQueue"
	var messagingService solace.MessagingService
	var receiver solace.PersistentMessageReceiver

	BeforeEach(func() {
		messagingService = helpers.BuildMessagingService(messaging.NewMessagingServiceBuilder().
			FromConfigurationProvider(helpers.DefaultConfiguration()))
		helpers.ConnectMessagingService(messagingService)
	})

	AfterEach(func() {
		if receiver.IsRunning() {
			receiver.Terminate(10 * time.Second)
		}
		if messagingService.IsConnected() {
			helpers.DisconnectMessagingService(messagingService)
		}
	})

	Describe("with resource creation disallowed", func() {

		BeforeEach(func() {
			testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpnClientProfile{
					AllowGuaranteedEndpointCreateEnabled: helpers.False,
				},
				testcontext.Messaging().VPN,
				testcontext.Messaging().ClientProfile,
				nil,
			)
		})

		AfterEach(func() {
			testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
				testcontext.SEMP().ConfigCtx(),
				sempconfig.MsgVpnClientProfile{
					AllowGuaranteedEndpointCreateEnabled: helpers.True,
				},
				testcontext.Messaging().VPN,
				testcontext.Messaging().ClientProfile,
				nil,
			)
		})

		When("MissingResourcesCreationStrategy is set to CREATE_ON_START", func() {
			DescribeTable("fails to create missing endpoint",
				func(queue *resource.Queue) {
					var err error
					receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
						WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).
						Build(queue)
					Expect(receiver).ToNot(BeNil())
					Expect(err).ToNot(HaveOccurred())
					helpers.ValidateNativeError(receiver.Start(), subcode.PermissionNotAllowed)
				},
				Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
				Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
				Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
				Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
			)
		})

		When("MissingResourcesCreationStrategy is set to DO_NOT_CREATE", func() {
			DescribeTable("fails to create missing endpoint",
				func(queue *resource.Queue) {
					var err error
					receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
						WithMissingResourcesCreationStrategy(config.PersistentReceiverDoNotCreateMissingResources).
						Build(queue)
					Expect(receiver).ToNot(BeNil())
					Expect(err).ToNot(HaveOccurred())
					helpers.ValidateNativeError(receiver.Start(), subcode.PermissionNotAllowed, subcode.UnknownQueueName)
				},
				Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
				Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
				Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
				Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
			)
		})
	})

	Describe("with resource creation allowed", func() {

		AfterEach(func() {
			//This block checks if a durable queue is still present when the receiver terminates
			response, _, err := testcontext.SEMP().Monitor().QueueApi.GetMsgVpnQueue(
				testcontext.SEMP().MonitorCtx(),
				testcontext.Messaging().VPN,
				myQueue,
				nil,
			)
			if err == nil {
				dur := **response.Data.Durable
				if dur {
					receiver.Terminate(10 * time.Second)
					_, _, err2 := testcontext.SEMP().Monitor().QueueApi.GetMsgVpnQueue(
						testcontext.SEMP().MonitorCtx(),
						testcontext.Messaging().VPN,
						myQueue,
						nil,
					)
					Expect(err2).ToNot(HaveOccurred())
				}
			}

			//This block deletes the queue on the broker
			testcontext.SEMP().Config().
				QueueApi.DeleteMsgVpnQueue(testcontext.SEMP().ConfigCtx(), testcontext.Messaging().VPN, myQueue)
		})

		When("with endpoint durability specified to all", func() {
			BeforeEach(func() {
				testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						AllowGuaranteedEndpointCreateEnabled:    helpers.True,
						AllowGuaranteedEndpointCreateDurability: "all",
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
			})

			When("MissingResourcesCreationStrategy is set to CREATE_ON_START", func() {

				DescribeTable("succeeds in creating missing endpoint",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						Expect(receiver.Start()).To(BeNil())
						helpers.CheckResourceInfo(receiver, queue.IsDurable(), queue.GetName())
						helpers.ReceiveOnePersistentMessage(receiver, resource.TopicSubscriptionOf("test/example"), messagingService, "test/example", "testing")
					},
					Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
					Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
					Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
					Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
				)
			})

			When("MissingResourcesCreationStrategy is set to DO_NOT_CREATE", func() {

				DescribeTable("fails to create missing durable endpoints",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverDoNotCreateMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						helpers.ValidateNativeError(receiver.Start(), subcode.UnknownQueueName)
					},
					Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
					Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
				)

				DescribeTable("succeeds in creating non durable endpoints",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverDoNotCreateMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						Expect(receiver.Start()).ToNot(HaveOccurred())
						helpers.CheckResourceInfo(receiver, queue.IsDurable(), queue.GetName())
						helpers.ReceiveOnePersistentMessage(receiver, resource.TopicSubscriptionOf("test/example"), messagingService, "test/example", "testing")
					},
					Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
					Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
				)
			})
		})

		When("with endpoint durability specified to durable", func() {
			BeforeEach(func() {
				testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						AllowGuaranteedEndpointCreateEnabled:    helpers.True,
						AllowGuaranteedEndpointCreateDurability: "durable",
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
			})

			AfterEach(func() {
				testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						AllowGuaranteedEndpointCreateEnabled:    helpers.True,
						AllowGuaranteedEndpointCreateDurability: "all",
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
			})

			When("MissingResourcesCreationStrategy is set to CREATE_ON_START", func() {

				DescribeTable("succeeds in creating durable endpoints only",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						Expect(receiver.Start()).ToNot(HaveOccurred())
						helpers.CheckResourceInfo(receiver, queue.IsDurable(), queue.GetName())
						helpers.ReceiveOnePersistentMessage(receiver, resource.TopicSubscriptionOf("test/example"), messagingService, "test/example", "testing")
					},
					Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
					Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
				)

				DescribeTable("fails to create non durable endpoints",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						helpers.ValidateNativeError(receiver.Start(), subcode.PermissionNotAllowed)
					},
					Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
					Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
				)
			})

			When("MissingResourcesCreationStrategy is set to DO_NOT_CREATE", func() {

				DescribeTable("fails to create missing durable and non durable endpoints ",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverDoNotCreateMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						helpers.ValidateNativeError(receiver.Start(), subcode.PermissionNotAllowed, subcode.UnknownQueueName)
					},
					Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
					Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
					Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
					Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
				)
			})
		})

		When("with endpoint durability specified to non-durable", func() {
			BeforeEach(func() {
				testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						AllowGuaranteedEndpointCreateEnabled:    helpers.True,
						AllowGuaranteedEndpointCreateDurability: "non-durable",
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
			})

			AfterEach(func() {
				testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						AllowGuaranteedEndpointCreateEnabled:    helpers.True,
						AllowGuaranteedEndpointCreateDurability: "all",
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
			})

			When("MissingResourcesCreationStrategy is set to CREATE_ON_START", func() {

				DescribeTable("succeeds in creating non-durable endpoints only",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						Expect(receiver.Start()).ToNot(HaveOccurred())
						helpers.CheckResourceInfo(receiver, queue.IsDurable(), queue.GetName())
						helpers.ReceiveOnePersistentMessage(receiver, resource.TopicSubscriptionOf("test/example"), messagingService, "test/example", "testing")
					},
					Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
					Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
				)

				DescribeTable("fails to create durable endpoints",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						helpers.ValidateNativeError(receiver.Start(), subcode.PermissionNotAllowed)
					},
					Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
					Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
				)
			})

			When("MissingResourcesCreationStrategy is set to DO_NOT_CREATE", func() {

				DescribeTable("fails to create durable endpoints",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverDoNotCreateMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						helpers.ValidateNativeError(receiver.Start(), subcode.UnknownQueueName)
					},
					Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
					Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
				)

				DescribeTable("succeeds in creating non-durable endpoints only",
					func(queue *resource.Queue) {
						var err error
						receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
							WithMissingResourcesCreationStrategy(config.PersistentReceiverDoNotCreateMissingResources).Build(queue)
						Expect(receiver).ToNot(BeNil())
						Expect(err).ToNot(HaveOccurred())
						Expect(receiver.Start()).ToNot(HaveOccurred())
						helpers.CheckResourceInfo(receiver, queue.IsDurable(), queue.GetName())
						helpers.ReceiveOnePersistentMessage(receiver, resource.TopicSubscriptionOf("test/example"), messagingService, "test/example", "testing")
					},
					Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
					Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
				)
			})
		})
	})

	Describe("MissingResourcesCreationStrategy is called despite queue already present on the broker", func() {

		AfterEach(func() {
			testcontext.SEMP().Config().
				QueueApi.DeleteMsgVpnQueue(testcontext.SEMP().ConfigCtx(), testcontext.Messaging().VPN, myQueue)
		})

		When("with endpoint durability set to all and with CREATE_ON_START", func() {
			BeforeEach(func() {
				testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						AllowGuaranteedEndpointCreateEnabled:    helpers.True,
						AllowGuaranteedEndpointCreateDurability: "all",
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
			})

			DescribeTable("should not fail to start the receiver",
				func(queue *resource.Queue) {
					if queue.IsExclusivelyAccessible() {
						helpers.CreateQueue(myQueue)
					} else {
						helpers.CreateNonExclusiveQueue(myQueue)
					}
					var err error
					receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
						WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).Build(queue)
					Expect(receiver).ToNot(BeNil())
					Expect(err).ToNot(HaveOccurred())
					Expect(receiver.Start()).ToNot(HaveOccurred())
					helpers.CheckResourceInfo(receiver, queue.IsDurable(), queue.GetName())
					helpers.ReceiveOnePersistentMessage(receiver, resource.TopicSubscriptionOf("test/example"), messagingService, "test/example", "testing")
				},
				Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
				Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
				Entry("non durable exclusive queue", resource.QueueNonDurableExclusive(myQueue)),
				Entry("non durable exclusive anonymous queue", resource.QueueNonDurableExclusiveAnonymous()),
			)
		})

		When("with endpoint durability set to durable and with CREATE_ON_START", func() {
			BeforeEach(func() {
				testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						AllowGuaranteedEndpointCreateEnabled:    helpers.True,
						AllowGuaranteedEndpointCreateDurability: "durable",
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
			})

			AfterEach(func() {
				testcontext.SEMP().Config().ClientProfileApi.UpdateMsgVpnClientProfile(
					testcontext.SEMP().ConfigCtx(),
					sempconfig.MsgVpnClientProfile{
						AllowGuaranteedEndpointCreateEnabled:    helpers.True,
						AllowGuaranteedEndpointCreateDurability: "all",
					},
					testcontext.Messaging().VPN,
					testcontext.Messaging().ClientProfile,
					nil,
				)
			})

			DescribeTable("should not fail to start the receiver",
				func(queue *resource.Queue) {
					if queue.IsExclusivelyAccessible() {
						helpers.CreateQueue(myQueue)
					} else {
						helpers.CreateNonExclusiveQueue(myQueue)
					}
					var err error
					receiver, err = messagingService.CreatePersistentMessageReceiverBuilder().
						WithMissingResourcesCreationStrategy(config.PersistentReceiverCreateOnStartMissingResources).Build(queue)
					Expect(receiver).ToNot(BeNil())
					Expect(err).ToNot(HaveOccurred())
					Expect(receiver.Start()).ToNot(HaveOccurred())
					helpers.CheckResourceInfo(receiver, queue.IsDurable(), queue.GetName())
					helpers.ReceiveOnePersistentMessage(receiver, resource.TopicSubscriptionOf("test/example"), messagingService, "test/example", "testing")
				},
				Entry("durable exclusive queue", resource.QueueDurableExclusive(myQueue)),
				Entry("durable non exclusive queue", resource.QueueDurableNonExclusive(myQueue)),
			)
		})
	})
})
