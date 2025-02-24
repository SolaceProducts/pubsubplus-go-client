// pubsubplus-go-client
//
// Copyright 2024-2025 Solace Corporation. All rights reserved.
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
// this testing module will focus on the replierImpl struct testing and builder configuration testing.

import (
	//"fmt"
	"runtime"
	"testing"

	"solace.dev/go/messaging/internal/ccsmp"

	"solace.dev/go/messaging/internal/impl/constants"
	"solace.dev/go/messaging/internal/impl/core"
	"solace.dev/go/messaging/internal/impl/message"

	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/resource"
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

	replyErrInfo := ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCodeFail,
		ccsmp.SolClientSubCode(subCode),
		ccsmp.SolClientResponseCode(0),
		"")

	internalReplier.sendReply = func(replyMsg core.ReplyPublishable) core.ErrorInfo {
		return replyErrInfo
	}
	replier.construct(testCorrelationID, testReplyToTopic, internalReplier)
	testReplyMessage, _ := message.NewOutboundMessage()
	err := replier.Reply(testReplyMessage)
	if err == nil {
		t.Error("Expected error from Reply call with failure to publish")
	}
	if _, ok := err.(*solace.NativeError); !ok {
		t.Error("Error returned was not the expected error type")
	} else if err.Error() != constants.ReplierFailureToPublishReply {
		t.Errorf("Error returned was not the expect error message. Expected: '%s' got: '%s'", constants.ReplierFailureToPublishReply, err.Error())
	}
}

func TestReplierWouldBlockSendReply(t *testing.T) {
	subCode := 0 // ok as last error is not set on solclient would block return
	testCorrelationID := "#TEST0"
	testReplyToTopic := "testReplyToDestination"
	replier := &replierImpl{}
	internalReplier := &mockReplier{}

	replyErrInfo := ccsmp.NewInternalSolClientErrorInfoWrapper(ccsmp.SolClientReturnCodeWouldBlock,
		ccsmp.SolClientSubCode(subCode),
		ccsmp.SolClientResponseCode(0),
		"This is a generated error info.")
	internalReplier.sendReply = func(replyMsg core.ReplyPublishable) core.ErrorInfo {
		return replyErrInfo
	}
	replier.construct(testCorrelationID, testReplyToTopic, internalReplier)
	testReplyMessage, _ := message.NewOutboundMessage()
	err := replier.Reply(testReplyMessage)
	if err == nil {
		t.Error("Expected error from Reply call with failure to publish")
	}

	if _, ok := err.(*solace.PublisherOverflowError); !ok {
		t.Error("Error returned was not the expected error type PublisherOverflowError")
	} else if err.Error() != constants.WouldBlock {
		t.Errorf("Error returned was not the expect error message. Expected: '%s' got: '%s'", constants.WouldBlock, err.Error())
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
	internalReplier.sendReply = func(replyMsg core.ReplyPublishable) core.ErrorInfo {
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

func TestNewReplierWithoutReplyDestination(t *testing.T) {
	builder := message.NewOutboundMessageBuilder()
	outMsg, err := builder.WithCorrelationID("#Test0").BuildWithStringPayload("testpayload", nil)
	if err != nil {
		t.Error("Failed to build message with correlation id")
	}
	outMsgImpl, ok := outMsg.(*message.OutboundMessageImpl)
	if !ok {
		t.Error("Failed to access internal outbound message impl struct")
	}
	msgP, errInfo := ccsmp.SolClientMessageDup(message.GetOutboundMessagePointer(outMsgImpl))
	if errInfo != nil {
		t.Error("Failed to extract duplicate core message from constructed message")
	}
	// create inbound message from constructed outbound message
	internalReceiver := &mockInternalReceiver{}
	receivedMsg := message.NewInboundMessage(msgP, false)
	_, hasReply := NewReplierImpl(receivedMsg, internalReceiver)
	if hasReply {
		t.Error("Created replier for message without replyTo destination")
	}
}

func TestNewReplierWithoutCorrelationID(t *testing.T) {
	builder := message.NewOutboundMessageBuilder()
	outMsg, err := builder.BuildWithStringPayload("testpayload", nil)
	if err != nil {
		t.Error("Failed to build message")
	}
	outMsgImpl, ok := outMsg.(*message.OutboundMessageImpl)
	if !ok {
		t.Error("Failed to access internal outbound message impl struct")
	}
	err = message.SetReplyToDestination(outMsgImpl, "testReplyTopic")
	if err != nil {
		t.Error("Failed to set the replyTo destination")
	}
	msgP, errInfo := ccsmp.SolClientMessageDup(message.GetOutboundMessagePointer(outMsgImpl))
	if errInfo != nil {
		t.Error("Failed to extract duplicate core message from constructed message")
	}
	// create inbound message from constructed outbound message
	internalReceiver := &mockInternalReceiver{}
	receivedMsg := message.NewInboundMessage(msgP, false)
	_, hasReply := NewReplierImpl(receivedMsg, internalReceiver)
	if hasReply {
		t.Error("Created replier for message without correlationID")
	}
}

func TestNewReplierFromRequestMessage(t *testing.T) {
	// a request message has two message fields to allow for the request to be replied to
	// these are:
	// - the ReplyTo Destination, to route the reply back to the originator (publisher making the request)
	// - the correlation, to uniquely identify the request to reply to and correlate the response at the origin
	// NewReplierImpl should only return hasReply == true if and only if both fields are set on a Inbound Message

	// construct inbound message with both fields
	// use outbound message to construct the message
	builder := message.NewOutboundMessageBuilder()
	// set correlationID
	outMsg, err := builder.WithCorrelationID("#Test0").BuildWithStringPayload("testpayload", nil)
	if err != nil {
		t.Error("Failed to build message with correlation id")
	}
	outMsgImpl, ok := outMsg.(*message.OutboundMessageImpl)
	if !ok {
		t.Error("Failed to access internal outbound message impl struct")
	}
	// set reply to destination
	err = message.SetReplyToDestination(outMsgImpl, "testReplyTopic")
	if err != nil {
		t.Error("Failed to set the replyTo destination")
	}
	msgP, errInfo := ccsmp.SolClientMessageDup(message.GetOutboundMessagePointer(outMsgImpl))
	if errInfo != nil {
		t.Error("Failed to extract duplicate core message from constructed message")
	}
	// create inbound message from constructed outbound message
	internalReceiver := &mockInternalReceiver{}
	receivedMsg := message.NewInboundMessage(msgP, false)
	_, hasReply := NewReplierImpl(receivedMsg, internalReceiver)
	if !hasReply {
		t.Error("Failed to create replier from request message with both reply to destination and correlationID")
	}
}

func TestRequestReplyReceiverBuilderWithInvalidBackpressureStrategy(t *testing.T) {
	subscription := resource.TopicSubscriptionOf("testTopic")
	builder := NewRequestReplyMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureStrategy: "notastrategy",
	})
	_, err := builder.Build(subscription)
	if err == nil {
		t.Error("expected to get error when building with invalid backpressure strategy")
	}
}

func TestRequestReplyReceiverBuilderWithInvalidBackpressureBufferSize(t *testing.T) {
	subscription := resource.TopicSubscriptionOf("testTopic")
	builder := NewRequestReplyMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureBufferCapacity: 0,
	})
	_, err := builder.Build(subscription)
	if err == nil {
		t.Error("expected to get error when building with invalid backpressure buffer size")
	}
}

func TestRequestReplyReceiverBuilderWithInvalidBackpressureBufferSizeType(t *testing.T) {
	subscription := resource.TopicSubscriptionOf("testTopic")
	builder := NewRequestReplyMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureBufferCapacity: "huh this isn't an int",
	})
	_, err := builder.Build(subscription)
	if err == nil {
		t.Error("expected to get error when building with invalid backpressure buffer size")
	}
}

func TestRequestReplyReceiverBuilderWithValidConfigurationMapLatestDrop(t *testing.T) {
	subscription := resource.TopicSubscriptionOf("testTopic")
	builder := NewRequestReplyMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureBufferCapacity: 2,
		config.ReceiverPropertyDirectBackPressureStrategy:       config.ReceiverBackPressureStrategyDropLatest,
	})
	receiver, err := builder.Build(subscription)
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*requestReplyMessageReceiverImpl)
	if !ok {
		t.Error("expected to get directMessageReceiverImpl back")
	}
	if receiverImpl.directReceiver.backpressureStrategy != strategyDropLatest {
		t.Error("expected to get backpressure drop oldest")
	}
	if cap(receiverImpl.directReceiver.buffer) != 2 {
		t.Error("expected buffer size of 2")
	}
}

func TestRequestReplyReceiverBuilderWithValidConfigurationMapOldestDrop(t *testing.T) {
	subscription := resource.TopicSubscriptionOf("testTopic")
	builder := NewRequestReplyMessageReceiverBuilderImpl(nil)
	builder.FromConfigurationProvider(config.ReceiverPropertyMap{
		config.ReceiverPropertyDirectBackPressureBufferCapacity: 2,
		config.ReceiverPropertyDirectBackPressureStrategy:       config.ReceiverBackPressureStrategyDropOldest,
	})
	receiver, err := builder.Build(subscription)
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*requestReplyMessageReceiverImpl)
	if !ok {
		t.Error("expected to get directMessageReceiverImpl back")
	}
	if receiverImpl.directReceiver.backpressureStrategy != strategyDropOldest {
		t.Error("expected to get backpressure drop oldest")
	}
	if cap(receiverImpl.directReceiver.buffer) != 2 {
		t.Error("expected buffer size of 2")
	}
}

func TestRequestReplyReceiverBuilderBuildWithConfigurationOldestDrop(t *testing.T) {
	subscription := resource.TopicSubscriptionOf("testTopic")
	builder := NewRequestReplyMessageReceiverBuilderImpl(nil)
	builder.OnBackPressureDropOldest(2)
	receiver, err := builder.Build(subscription)
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*requestReplyMessageReceiverImpl)
	if !ok {
		t.Error("expected to get directMessageReceiverImpl back")
	}
	if receiverImpl.directReceiver.backpressureStrategy != strategyDropOldest {
		t.Error("expected to get backpressure drop oldest")
	}
	if cap(receiverImpl.directReceiver.buffer) != 2 {
		t.Error("expected buffer size of 2")
	}
}

func TestRequestReplyReceiverBuilderBuildWithConfigurationLatestDrop(t *testing.T) {
	subscription := resource.TopicSubscriptionOf("testTopic")
	builder := NewRequestReplyMessageReceiverBuilderImpl(nil)
	builder.OnBackPressureDropLatest(2)
	receiver, err := builder.Build(subscription)
	if err != nil {
		t.Error("did not expect to get an error when building with valid properties")
	}
	receiverImpl, ok := receiver.(*requestReplyMessageReceiverImpl)
	if !ok {
		t.Error("expected to get directMessageReceiverImpl back")
	}
	if receiverImpl.directReceiver.backpressureStrategy != strategyDropLatest {
		t.Error("expected to get backpressure drop oldest")
	}
	if cap(receiverImpl.directReceiver.buffer) != 2 {
		t.Error("expected buffer size of 2")
	}
}

func TestRequestReplyReceiverBuilderBuildWithoutSubscription(t *testing.T) {
	builder := NewRequestReplyMessageReceiverBuilderImpl(nil)
	_, err := builder.Build(nil)
	if err == nil {
		t.Error("Did not get error for missing topic subscription on build")
	}
}
