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

package helpers

import (
	"solace.dev/go/messaging/test/sempclient/config"
	"solace.dev/go/messaging/test/testcontext"
)

// Our generated SEMPv2 client has **bool as the datatype for booleans to be able to differentiate between "missing" and "false"

// True defined
var True = boolPointer(true)

// False defined
var False = boolPointer(false)

func boolPointer(b bool) **bool {
	bp := &b
	return &bp
}

// AddSubscriptionTopicException function
func AddSubscriptionTopicException(topicString string) {
	testcontext.SEMP().Config().AclProfileApi.CreateMsgVpnAclProfileSubscribeTopicException(
		testcontext.SEMP().ConfigCtx(),
		config.MsgVpnAclProfileSubscribeTopicException{
			MsgVpnName:                    testcontext.Messaging().VPN,
			AclProfileName:                testcontext.Messaging().ACLProfile,
			SubscribeTopicException:       topicString,
			SubscribeTopicExceptionSyntax: "smf",
		},
		testcontext.Messaging().VPN,
		testcontext.Messaging().ACLProfile,
		nil,
	)
}

// RemoveSubscriptionTopicException function
func RemoveSubscriptionTopicException(topicString string) {
	testcontext.SEMP().Config().AclProfileApi.DeleteMsgVpnAclProfileSubscribeTopicException(
		testcontext.SEMP().ConfigCtx(),
		testcontext.Messaging().VPN,
		testcontext.Messaging().ACLProfile,
		"smf",
		topicString,
	)
}

// AddSharedSubscriptionTopicException function
func AddSharedSubscriptionTopicException(topicString string) {
	testcontext.SEMP().Config().AclProfileApi.CreateMsgVpnAclProfileSubscribeShareNameException(
		testcontext.SEMP().ConfigCtx(),
		config.MsgVpnAclProfileSubscribeShareNameException{
			MsgVpnName:                        testcontext.Messaging().VPN,
			AclProfileName:                    testcontext.Messaging().ACLProfile,
			SubscribeShareNameException:       topicString,
			SubscribeShareNameExceptionSyntax: "smf",
		},
		testcontext.Messaging().VPN,
		testcontext.Messaging().ACLProfile,
		nil,
	)
}

// RemoveSharedSubscriptionTopicException function
func RemoveSharedSubscriptionTopicException(topicString string) {
	testcontext.SEMP().Config().AclProfileApi.DeleteMsgVpnAclProfileSubscribeShareNameException(
		testcontext.SEMP().ConfigCtx(),
		testcontext.Messaging().VPN,
		testcontext.Messaging().ACLProfile,
		"smf",
		topicString,
	)
}

// AddPublishTopicException function
func AddPublishTopicException(topicString string) {
	testcontext.SEMP().Config().AclProfileApi.CreateMsgVpnAclProfilePublishTopicException(
		testcontext.SEMP().ConfigCtx(),
		config.MsgVpnAclProfilePublishTopicException{
			MsgVpnName:                  testcontext.Messaging().VPN,
			AclProfileName:              testcontext.Messaging().ACLProfile,
			PublishTopicException:       topicString,
			PublishTopicExceptionSyntax: "smf",
		},
		testcontext.Messaging().VPN,
		testcontext.Messaging().ACLProfile,
		nil,
	)
}

// RemovePublishTopicException function
func RemovePublishTopicException(topicString string) {
	testcontext.SEMP().Config().AclProfileApi.DeleteMsgVpnAclProfilePublishTopicException(
		testcontext.SEMP().ConfigCtx(),
		testcontext.Messaging().VPN,
		testcontext.Messaging().ACLProfile,
		"smf",
		topicString,
	)
}
