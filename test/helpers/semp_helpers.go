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

package helpers

import (
	"time"

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

// HasDomainCertAuthority function
func HasDomainCertAuthority(certAuthName string) (bool, error) {
	var found bool
	var err error
	var certAuthResp config.DomainCertAuthoritiesResponse
	// determine if DomainCertAuhtority exists
	certAuthResp, _, err = testcontext.SEMP().Config().DomainCertAuthorityApi.GetDomainCertAuthorities(
		testcontext.SEMP().ConfigCtx(),
		nil,
	)
	if err != nil {
		return false, err
	}
	if domainCertAuths := certAuthResp.Data; domainCertAuths != nil {
		for _, domainCertAuth := range domainCertAuths {
			if domainCertAuth.CertAuthorityName == certAuthName {
				found = true
				break
			}
		}
		return found, err
	}
	return false, err
}

// EnsureCreateDomainCertAuthority function
func EnsureCreateDomainCertAuthority(certAuthName string, certContent string) error {
	var err error
	var found bool
	found, err = HasDomainCertAuthority(certAuthName)
	if err != nil {
		return err
	}
	// repeat the create http request until the broker has the certificate authority
	for !found {
		_, _, err = testcontext.SEMP().Config().DomainCertAuthorityApi.CreateDomainCertAuthority(
			testcontext.SEMP().ConfigCtx(), config.DomainCertAuthority{
				CertAuthorityName: certAuthName,
				CertContent:       certContent,
			}, nil)
		if err != nil {
			return err
		}
		// wait for base semp service to reload
		err = testcontext.WaitForSEMPReachable()
		if err != nil {
			return err
		}
		// ensure domain certification authority exists on the broker
		// wait up to 30 secs after the service is back is overkill however to have an exit condition to prevent hung tests
		for i := 0; i < 30; i++ {
			found, err = HasDomainCertAuthority(certAuthName)

			if err != nil {
				// the broker can sometimes fail to have semp up after cert auth change
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// EnsureDeleteCertAuthrity function
func EnsureDeleteDomainCertAuthority(certAuthName string) error {
	var found bool
	var err error
	found, err = HasDomainCertAuthority(certAuthName)
	if err != nil {
		return err
	}
	for found {
		_, _, err = testcontext.SEMP().Config().DomainCertAuthorityApi.DeleteDomainCertAuthority(
			testcontext.SEMP().ConfigCtx(),
			certAuthName,
		)
		if err != nil {
			return err
		}
		err = testcontext.WaitForSEMPReachable()
		if err != nil {
			return err
		}
		// ensure domain certification authority does not exist on the broker
		// wait up to 30 secs after the service is back is overkill however to have an exit condition to prevent hung tests
		for i := 0; i < 30; i++ {
			found, err = HasDomainCertAuthority(certAuthName)
			if err != nil {
				// the broker can sometimes fail to have semp up after cert auth change
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}
