// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
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

package core

import (
	"testing"
	"time"
)

func TestSolClientPublisherAwaitWritable(t *testing.T) {
	publisher := newCcsmpPublisher(nil, nil, nil)
	c := make(chan error)
	go func() {
		c <- publisher.AwaitWritable(nil)
	}()
	select {
	case <-c:
		t.Error("did not expect to receive anything from the channel until a writable event was emitted")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	publisher.onCanSend()
	select {
	case err := <-c:
		if err != nil {
			t.Errorf("did not expect error from AwaitWritable, got %s", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected to receive result from AwaitWritable, timed out after 100ms")
	}
}

func TestSolClientPublisherAwaitWritableWithChannelInterrupt(t *testing.T) {
	interrupt := make(chan struct{})
	publisher := newCcsmpPublisher(nil, nil, nil)
	c := make(chan error)
	go func() {
		c <- publisher.AwaitWritable(interrupt)
	}()
	select {
	case <-c:
		t.Error("did not expect to receive anything from the channel until a writable event was emitted")
	case <-time.After(100 * time.Millisecond):
		// success
	}
	close(interrupt)
	select {
	case err := <-c:
		if err == nil {
			t.Errorf("expected error from AwaitWritable, got nil")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected to receive result from AwaitWritable, timed out after 100ms")
	}
}

func TestCorrelationTagEncoding(t *testing.T) {
	inputPubID := uint64(9728076316212472000)
	expectedOutputPubID := []byte{0xC0, 0x28, 0xBC, 0x37, 0xDE, 0x11, 0x01, 0x87}
	inputMsgID := uint64(13408755611955770000)
	expectedOutputMsgID := []byte{0x90, 0x32, 0xA6, 0xBA, 0x2F, 0x78, 0x15, 0xBA}
	output := toCorrelationTag(inputPubID, inputMsgID)
	for i, b := range append(expectedOutputPubID, expectedOutputMsgID...) {
		if output[i] != b {
			t.Errorf("Expected byte at index %d to equal %d, was %d", i, b, output[i])
		}
	}
}

func TestCorrelationTagDecoding(t *testing.T) {
	expectedOutputPubID := uint64(9728076316212472000)
	inputPubID := []byte{0xC0, 0x28, 0xBC, 0x37, 0xDE, 0x11, 0x01, 0x87}
	expectedOutputMsgID := uint64(13408755611955770000)
	inputMsgID := []byte{0x90, 0x32, 0xA6, 0xBA, 0x2F, 0x78, 0x15, 0xBA}
	outputPubID, outputMsgID, ok := fromCorrelationTag(append(inputPubID, inputMsgID...))
	if !ok {
		t.Error("expected ok to be true")
	}
	if outputPubID != expectedOutputPubID {
		t.Errorf("expected output publisher id %d to equal %d", outputPubID, expectedOutputPubID)
	}
	if outputMsgID != expectedOutputMsgID {
		t.Errorf("expected output msg id %d to equal %d", outputMsgID, expectedOutputMsgID)
	}
}

func TestCorrelationTagEncodeDecode(t *testing.T) {
	inputPubID := uint64(9728076316212472000)
	inputMsgID := uint64(13408755611955770000)
	correlationTag := toCorrelationTag(inputPubID, inputMsgID)
	outputPubID, outputMsgID, ok := fromCorrelationTag(correlationTag)
	if !ok {
		t.Error("expected ok to be true")
	}
	if outputPubID != inputPubID {
		t.Errorf("expected output publisher id %d to equal %d", outputPubID, inputPubID)
	}
	if outputMsgID != inputMsgID {
		t.Errorf("expected output msg id %d to equal %d", outputMsgID, inputMsgID)
	}
}
