// pubsubplus-go-client
//
// Copyright 2024 Solace Corporation. All rights reserved.
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

package receiver

// As the requestReplyMessageReceiverImpl and requestReplyMessageReceiverBuilderImpl wraps there corresponding
// this testing module will focus on the replierImpl struct testing.

import (
	//"fmt"
	"runtime"
	"testing"

	"solace.dev/go/messaging/internal/ccsmp"

	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/message"
)

func TestReplierWithInvalidReplyMessage(t *testing.T) {
	testCorrelationID := "#TEST0"
	testReplyToTopic := "testReplyToDestination"
	replier := &replierImpl{}
	replier.construct(testCorrelationID, testReplyToTopic, &mockReplier{})

	//testReplyMessage := message.NewOutboundMessage()

	err := replier.Reply(nil)
	if err == nil {
		t.Error("Expected error from Reply call with nil message")
	}
}

func TestReplierFailedSendReply(t *testing.T) {
	subCode := 67
	testCorrelationID := "#TEST0"
	testReplyToTopic := "testReplyToDestination"
	replier := &replierImpl{}
	internalReplier := &mockReplier{}

	replyErr := core.ToNativeError(&ccsmp.SolClientErrorInfoWrapper{
		ReturnCode: ccsmp.SolClientReturnCodeFail,
		SubCode:    ccsmp.SolClientSubCode(subCode),
	}, "test native error return:")

	internalReplier.sendReply = func(replyMsg core.ReplyPublishable) error {
		return replyErr
	}
	replier.construct(testCorrelationID, testReplyToTopic, internalReplier)
	testReplyMessage, _ := message.NewOutboundMessage()
	err := replier.Reply(testReplyMessage)
	if err == nil {
		t.Error("Expected error from Reply call with failure to publish")
	}
	if err != replyErr {
		t.Error("Error return was was not the expected error")
	}
}

func TestReplierMessageParametersOnReply(t *testing.T) {
	testCorrelationID := "#TEST0"
	testReplyToTopic := "testReplyToDestination"
	replier := &replierImpl{}
	internalReplier := &mockReplier{}
	var replyMsgDestination string
	var replyMsgCorrelationID string
	var replyMsgCorrelationIDOk bool = false
	// extract fields in the sendReplyFunction
	internalReplier.sendReply = func(replyMsg core.ReplyPublishable) error {
		// grab the prepare reply message and turn into a message that can access fields
		testReplyMsg := message.NewInboundMessage(replyMsg, false)
		replyMsgDestination = testReplyMsg.GetDestinationName()
		replyMsgCorrelationID, replyMsgCorrelationIDOk = testReplyMsg.GetCorrelationID()
		// detach from free as the caller will free the message
		runtime.SetFinalizer(testReplyMsg, nil)
		return nil
	}
	replier.construct(testCorrelationID, testReplyToTopic, internalReplier)
	testReplyMessage, _ := message.NewOutboundMessage()
	err := replier.Reply(testReplyMessage)
	if err != nil {
		t.Errorf("Got error calling replier.Reply: %s", err)
	}

	if replyMsgDestination != testReplyToTopic {
		t.Errorf("Replier.Reply did not set the expected replyTo destination name[%s], instead set destination[%s]", testReplyToTopic, replyMsgDestination)
	}

	if !replyMsgCorrelationIDOk {
		t.Error("Replier.Reply did not set the correlationID on the reply message")
	} else if replyMsgCorrelationID != testCorrelationID {
		t.Errorf("Replier.Reply did not set the expected correlationID[%s], instead set correlationID[%s]", testCorrelationID, replyMsgCorrelationID)
	}
}
